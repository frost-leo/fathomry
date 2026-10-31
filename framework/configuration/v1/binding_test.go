/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package configuration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func fixedSchema[T any](schema Schema[T]) SchemaFactory[T] {
	return func(context.Context, string) (Schema[T], error) { return schema, nil }
}

func startupFiles(t *testing.T, watch bool) string {
	t.Helper()
	directory := t.TempDir()
	flag := "false"
	if watch {
		flag = "true"
	}
	write(t, filepath.Join(directory, ".env"), "FATHOMRY_ENV=development\nFATHOMRY_CONFIG_MODE=local\nFATHOMRY_CONFIG_PROVIDER=viper\nFATHOMRY_CONFIG_WATCH="+flag+"\n")
	if err := os.MkdirAll(filepath.Join(directory, "config/environment"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(directory, "config/base.yaml"), "workers: 1")
	write(t, filepath.Join(directory, "config/environment/development.yaml"), "workers: 2")
	return filepath.Join(directory, ".env")
}
func startupScope(t *testing.T) (*resource.Scope, context.Context) {
	t.Helper()
	scope, err := resource.New(context.Background(), resource.Options{Name: "startup-test", CleanupTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_ = scope.Close(cleanup)
		if !scope.ShutdownComplete() {
			t.Error("scope retained work")
		}
	})
	return scope, ctx
}
func TestStartupResourceLifecycle(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite", true: "watch"}[watch], func(t *testing.T) {
			scope, ctx := startupScope(t)
			var received atomic.Int32
			ref, accepted, err := BindConfiguration(scope, "configuration", fixedSchema(schema()), Policy{Receive: func(_ context.Context, evidence Evidence) error {
				if evidence.Viper == nil || evidence.Nacos != nil || !evidence.Viper.Info().Released {
					t.Error("invalid evidence")
				}
				received.Add(1)
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := accepted.Capture(); !errors.Is(err, settings.ErrUnconfigured) {
				t.Fatal("binding fabricated accepted data", err)
			}
			input, err := PrepareInput(ctx, BootstrapInput{Path: startupFiles(t, watch)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			update, err := scope.Apply(ctx, input.View())
			if err != nil {
				t.Fatal(err)
			}
			if err := update.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			captured, err := accepted.Capture()
			if err != nil {
				t.Fatal("accepted data depends on lease acquisition", err)
			}
			retained, err := settings.As[Loaded](captured)
			if err != nil {
				t.Fatal(err)
			}
			retainedValue, err := retained.ValueCopy()
			if err != nil {
				t.Fatal(err)
			}
			lease, err := ref.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			value, err := lease.Value()
			if err != nil {
				t.Fatal(err)
			}
			view, err := value.InitialView()
			if err != nil {
				t.Fatal(err)
			}
			retainedView, err := retainedValue.InitialView()
			if err != nil || retainedView != view {
				t.Fatal("result custody changed the exact first snapshot", err)
			}
			snapshot, err := settings.As[application](view)
			if err != nil {
				t.Fatal(err)
			}
			data, err := snapshot.ValueCopy()
			if err != nil || data.Workers != 2 {
				t.Fatal("wrong first snapshot", err)
			}
			info, err := value.Info()
			if err != nil || info.Watching != watch || len(info.Documents) != 2 {
				t.Fatal("wrong startup info", err)
			}
			info.Documents[0].Kind = "changed"
			again, _ := value.Info()
			if again.Documents[0].Kind != "base" {
				t.Fatal("info aliases state")
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
			if err := scope.Close(ctx); err != nil {
				t.Fatal("normal Resource cancellation became a source failure", err)
			}
			if !scope.ShutdownComplete() || received.Load() != 2 {
				t.Fatal("source work/evidence not joined")
			}
			if read(t, value.Reader()).Workers != 2 {
				t.Fatal("cleanup invalidated data")
			}
			if after, err := accepted.Capture(); err != nil || after != captured || read(t, retainedValue.Reader()).Workers != 2 {
				t.Fatal("cleanup invalidated accepted-result custody", err)
			}
		})
	}
}

func TestInitialAcceptancePublishesBindingResultBeforeWait(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(fmt.Sprint(watch), func(t *testing.T) {
			dependencies, _ := localDependencies(t, 8)
			path := filepath.Join(t.TempDir(), "application.yaml")
			write(t, path, "workers: 2")
			accepted := settings.NewStore[Loaded]()
			selected := options()
			selected.accepted = &accepted
			session, err := startSession(context.Background(), localPlan(path, watch), schema(), selected, dependencies)
			if err != nil {
				t.Fatal(err)
			}
			defer closeSession(t, session, false)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			select {
			case <-session.state.initialDone:
			case <-ctx.Done():
				t.Fatal("initial publication did not complete")
			}
			// No initial waiter or Resource constructor has resumed to copy the result.
			view, err := accepted.Reader().Capture()
			if err != nil {
				t.Fatal("publication depended on a later result handoff", err)
			}
			snapshot, err := settings.As[Loaded](view)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := snapshot.ValueCopy()
			if err != nil {
				t.Fatal(err)
			}
			initial, err := loaded.InitialView()
			if err != nil {
				t.Fatal(err)
			}
			actual, err := session.InitialView()
			if err != nil || actual != initial {
				t.Fatal("result changed the exact accepted view", err)
			}
			closeSession(t, session, false)
			if after, err := accepted.Reader().Capture(); err != nil || after != view || read(t, loaded.Reader()).Workers != 2 {
				t.Fatal("cleanup discarded publication-time custody", err)
			}
		})
	}
}

func TestStartupFailureUsesExistingResourceCleanup(t *testing.T) {
	scope, ctx := startupScope(t)
	path := startupFiles(t, false)
	write(t, filepath.Join(filepath.Dir(path), "config/environment/development.yaml"), "workers: 0")
	ref, accepted, err := BindConfiguration(scope, "configuration", fixedSchema(schema()), Policy{})
	if err != nil {
		t.Fatal(err)
	}
	input, err := PrepareInput(ctx, BootstrapInput{Path: path}, nil)
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(ctx, input.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(ctx); !errors.Is(err, invalidWorkers) {
		t.Fatal("validator cause lost", err)
	}
	if _, err := ref.Acquire(ctx); err == nil {
		t.Fatal("failed candidate adopted")
	}
	if _, err := accepted.Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("rejected candidate published an accepted result", err)
	}
	_ = scope.Close(ctx)
	if !scope.ShutdownComplete() {
		t.Fatal("failed construction lost cleanup owner")
	}
}
func TestStartupEvidenceFailureCanContinue(t *testing.T) {
	scope, ctx := startupScope(t)
	block := errors.New("required-sink")
	var fail atomic.Bool
	fail.Store(true)
	ref, _, err := BindConfiguration(scope, "configuration", fixedSchema(schema()), Policy{Receive: func(context.Context, Evidence) error {
		if fail.Load() {
			return block
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	input, err := PrepareInput(ctx, BootstrapInput{Path: startupFiles(t, false)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(ctx, input.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(ctx); !errors.Is(err, block) {
		t.Fatal("receiver failure lost", err)
	}
	if _, err := ref.Acquire(ctx); err == nil {
		t.Fatal("failed bootstrap adopted")
	}
	if err := scope.Close(ctx); !errors.Is(err, block) || scope.ShutdownComplete() {
		t.Fatal("pending evidence was discarded", err)
	}
	fail.Store(false)
	_ = scope.Close(ctx)
	if !scope.ShutdownComplete() {
		t.Fatal("existing Resource continuation could not finish")
	}
}
func TestStartupCancellationRetainsBlockedValidator(t *testing.T) {
	scope, ctx := startupScope(t)
	path := startupFiles(t, true)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	write(t, path, string(data)+"FATHOMRY_SHUTDOWN_TIMEOUT=10ms\n")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	declared := schema()
	declared.Validate = func(context.Context, application) error { once.Do(func() { close(entered) }); <-release; return nil }
	ref, accepted, err := BindConfiguration(scope, "configuration", fixedSchema(declared), Policy{})
	if err != nil {
		t.Fatal(err)
	}
	input, err := PrepareInput(ctx, BootstrapInput{Path: path}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scope.Apply(ctx, input.View()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		close(release)
		t.Fatal("validator not entered")
	}
	wait, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = scope.Close(wait)
	stop()
	if err == nil || scope.ShutdownComplete() {
		close(release)
		t.Fatal("blocked work claimed complete", err)
	}
	if _, err := accepted.Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("canceled validator fabricated accepted data", err)
	}
	close(release)
	_ = scope.Close(ctx)
	if !scope.ShutdownComplete() {
		t.Fatal("actual constructor/session did not join")
	}
	if _, err := ref.Acquire(ctx); err == nil {
		t.Fatal("late candidate adopted after close")
	}
	if _, err := accepted.Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("late validator published after cancellation", err)
	}
}
func TestStartupSelectionDetached(t *testing.T) {
	scope, ctx := startupScope(t)
	path := startupFiles(t, false)
	variables := map[string]string{"FATHOMRY_ENV": "development"}
	appVars := []Variable{{Path: "/workers", Value: "3", Present: true, JSON: true}}
	input, err := PrepareInput(ctx, BootstrapInput{Path: path, Variables: variables}, appVars)
	if err != nil {
		t.Fatal(err)
	}
	variables["FATHOMRY_ENV"] = "wrong"
	appVars[0].Value = "99"
	ref, _, err := BindConfiguration(scope, "configuration", fixedSchema(schema()), Policy{})
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(ctx, input.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := ref.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	value, err := lease.Value()
	if err != nil {
		t.Fatal(err)
	}
	if read(t, value.Reader()).Workers != 3 {
		t.Fatal("startup request retained mutable caller input")
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStartupSchemaFactory(t *testing.T) {
	for _, mode := range []string{"selected", "rejected", "timeout", "invalid_bootstrap"} {
		t.Run(mode, func(t *testing.T) {
			scope, ctx := startupScope(t)
			path := startupFiles(t, false)
			write(t, filepath.Join(filepath.Dir(path), "config/environment/production.yaml"), "workers: 3")
			if mode == "timeout" {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				write(t, path, string(data)+"FATHOMRY_STARTUP_TIMEOUT=25ms\n")
			}
			if mode == "invalid_bootstrap" {
				write(t, path, "UNKNOWN=1\n")
			}
			var calls, observations atomic.Int32
			rejected := errors.New("schema-factory-canary")
			factory := func(ctx context.Context, environment string) (Schema[application], error) {
				calls.Add(1)
				if environment != "production" {
					t.Error("factory did not see selected bootstrap")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("factory has no startup budget")
				}
				if mode == "rejected" {
					return Schema[application]{}, rejected
				}
				if mode == "timeout" {
					<-ctx.Done()
					return Schema[application]{}, ctx.Err()
				}
				return schema(), nil
			}
			ref, _, err := BindConfiguration(scope, "configuration", SchemaFactory[application](factory), Policy{
				Receive: func(context.Context, Evidence) error { observations.Add(1); return nil },
			})
			if err != nil || calls.Load() != 0 {
				t.Fatal("binding invoked the factory", err)
			}
			environment := "production"
			input, err := PrepareInput(ctx, BootstrapInput{Path: path, Environment: &environment}, nil)
			if err != nil {
				t.Fatal(err)
			}
			update, err := scope.Apply(ctx, input.View())
			if err != nil {
				t.Fatal(err)
			}
			err = update.Wait(ctx)
			if mode == "selected" {
				if err != nil {
					t.Fatal(err)
				}
				lease, err := ref.Acquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
				value, err := lease.Value()
				if err != nil || read(t, value.Reader()).Workers != 3 {
					t.Fatal("wrong schema/source", err)
				}
				if err := lease.Release(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatal("failed startup admitted")
				}
				if mode == "rejected" && !errors.Is(err, rejected) {
					t.Fatal("factory cause lost", err)
				}
				if mode == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("factory budget lost", err)
				}
			}
			_ = scope.Close(ctx)
			if !scope.ShutdownComplete() {
				t.Fatal("factory failure lost cleanup")
			}
			wantCalls, wantObservations := int32(1), int32(1)
			if mode == "selected" {
				wantObservations = 2
			}
			if mode == "invalid_bootstrap" {
				wantCalls = 0
			}
			if calls.Load() != wantCalls || observations.Load() != wantObservations {
				t.Fatal("unexpected factory/source execution", calls.Load(), observations.Load())
			}
		})
	}
}

func TestBindingSharesInitialDeadlineThroughPreparation(t *testing.T) {
	type configurationValue struct {
		Payload map[string]string `json:"payload"`
	}
	defaults := configurationValue{Payload: make(map[string]string, 8000)}
	for index := range 8000 {
		defaults.Payload[fmt.Sprintf("key-%05d", index)] = strings.Repeat("x", 64)
	}
	path := startupFiles(t, false)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	write(t, path, string(raw)+"FATHOMRY_STARTUP_TIMEOUT=2s\n")
	write(t, filepath.Join(filepath.Dir(path), "config/base.yaml"), "payload: null")
	write(t, filepath.Join(filepath.Dir(path), "config/environment/development.yaml"), "{}")
	scope, ctx := startupScope(t)
	factory := func(initial context.Context, _ string) (Schema[configurationValue], error) {
		deadline, present := initial.Deadline()
		if !present {
			return Schema[configurationValue]{}, errors.New("factory has no initial deadline")
		}
		return Schema[configurationValue]{
			Version: 1, Defaults: defaults,
			Validate: func(candidate context.Context, _ configurationValue) error {
				actual, present := candidate.Deadline()
				if !present || actual.After(deadline) {
					return errors.New("synchronous admission extended the original deadline")
				}
				return candidate.Err()
			},
		}, nil
	}
	ref, _, err := BindConfiguration(scope, "configuration", factory, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	input, err := PrepareInput(ctx, BootstrapInput{Path: path}, nil)
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(ctx, input.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := ref.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestMainConfigurationFeedsSeparateResourceScope(t *testing.T) {
	configurationScope, ctx := startupScope(t)
	ref, _, err := BindConfiguration(configurationScope, "configuration", fixedSchema(schema()), Policy{})
	if err != nil {
		t.Fatal(err)
	}
	input, err := PrepareInput(ctx, BootstrapInput{Path: startupFiles(t, false)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	update, err := configurationScope.Apply(ctx, input.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := ref.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	loaded, err := lease.Value()
	if err != nil {
		t.Fatal(err)
	}
	view, err := loaded.InitialView()
	if err != nil {
		t.Fatal(err)
	}
	var constructed atomic.Int32
	binding := resource.Binding[int, int]{
		Name: "configured-resource", Policy: resource.Fixed,
		Select: func(view settings.View) (int, error) {
			workers, present, err := settings.Read(view, "/workers", func(value int) int { return value })
			if err != nil {
				return 0, err
			}
			if !present {
				return 0, errors.New("main configuration missing")
			}
			return workers, nil
		},
		Clone: func(value int) int { return value },
		Build: func(_ context.Context, workers int) (*resource.Instance[int], error) {
			constructed.Add(1)
			return &resource.Instance[int]{Value: workers}, nil
		},
	}
	if _, err := resource.Bind(configurationScope, binding); !errors.Is(err, resource.ErrSealed) {
		t.Fatal("configuration scope unexpectedly accepts later declarations", err)
	}
	if constructed.Load() != 0 {
		t.Fatal("configuration loading constructed a business resource")
	}
	resourceScope, err := resource.New(context.Background(), resource.Options{Name: "configured-resources"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := resourceScope.Close(cleanup); err != nil {
			t.Error(err)
		}
		if !resourceScope.ShutdownComplete() {
			t.Error("resource scope did not close")
		}
	})
	configured, err := resource.Bind(resourceScope, binding)
	if err != nil {
		t.Fatal(err)
	}
	update, err = resourceScope.Apply(ctx, view)
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	resourceLease, err := configured.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer resourceLease.Release()
	workers, err := resourceLease.Value()
	if err != nil || workers != 2 || constructed.Load() != 1 {
		t.Fatal("accepted main configuration did not feed the resource scope", err)
	}
}
