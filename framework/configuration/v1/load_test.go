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
	"os"
	"path/filepath"
	"testing"
	"time"

	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type application struct {
	Workers  int               `json:"workers"`
	Labels   []string          `json:"labels"`
	Defaults map[string]string `json:"defaults"`
	Optional *int              `json:"optional"`
}

var invalidWorkers = errors.New("private-validator-canary")

func schema() Schema[application] {
	return Schema[application]{Version: 1, Validate: func(ctx context.Context, value application) error {
		if value.Workers < 1 {
			return invalidWorkers
		}
		return ctx.Err()
	}}
}

type testSessionOptions struct {
	InitialTimeout time.Duration
	sessionOptions
}

func options() testSessionOptions {
	return testSessionOptions{InitialTimeout: 3 * time.Second, sessionOptions: sessionOptions{CandidateTimeout: time.Second}}
}

func startSession[T any](lifetime context.Context, plan SourcePlan, schema Schema[T], options testSessionOptions, dependencies Dependencies) (*Session[T], error) {
	if lifetime == nil || options.InitialTimeout <= 0 {
		return nil, at(ErrDeclaration, "start", -1)
	}
	initial, cancel := context.WithTimeout(lifetime, options.InitialTimeout)
	defer cancel()
	return startSessionBefore(lifetime, initial, plan, schema, options.sessionOptions, dependencies)
}
func localDependencies(t *testing.T, capacity int) (Dependencies, *adapters.Inbox[viper.Evidence]) {
	t.Helper()
	runtime, err := adapters.New(context.Background(), adapters.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{Capacity: capacity})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		status, _ := inbox.Inspect()
		for range status.Queued {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
			}
		}
	})
	return Dependencies{Local: viper.Dependencies{Runtime: runtime, Evidence: inbox}}, inbox
}
func localPlan(path string, watch bool) SourcePlan {
	return SourcePlan{Version: 1, Environment: "development", Mode: "local", Provider: "viper", Watch: watch,
		Documents: []Document{{Kind: "base", Encoding: YAML}},
		Local:     &viper.WatchSettings{Paths: []string{path}, Interval: 10 * time.Millisecond}}
}
func write(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path+".next", []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".next", path); err != nil {
		t.Fatal(err)
	}
}
func read(t *testing.T, reader settings.Reader) application {
	t.Helper()
	view, err := reader.Capture()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settings.As[application](view)
	if err != nil {
		t.Fatal(err)
	}
	value, err := snapshot.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func closeSession(t *testing.T, session *Session[application], allowError bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.Close(ctx); err != nil && !allowError {
		t.Error(err)
	}
	if !session.ShutdownComplete() {
		t.Error("cleanup incomplete")
	}
}
func TestBootstrap(t *testing.T) {
	dependencies, inbox := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), ".env")
	write(t, path, "FATHOMRY_ENV=development\nFATHOMRY_CONFIG_MODE=local\nFATHOMRY_CONFIG_PROVIDER=viper\n")
	input := BootstrapInput{Path: path}
	value, err := LoadBootstrap(context.Background(), input, dependencies.Local)
	if err != nil || value.Configuration.Environment != "development" {
		t.Fatal(err)
	}
	production := "production"
	input.Environment = &production
	other, err := LoadBootstrap(context.Background(), input, dependencies.Local)
	if err != nil || other.Configuration.Environment != production {
		t.Fatal("explicit selection failed", err)
	}
	status, _ := inbox.Inspect()
	if status.Queued != 2 {
		t.Fatal("bootstrap evidence was silently acknowledged")
	}
	write(t, path, "FATHOMRY_ENV=../invalid\nFATHOMRY_CONFIG_MODE=local\nFATHOMRY_CONFIG_PROVIDER=viper\n")
	if _, err := LoadBootstrap(context.Background(), input, dependencies.Local); !errors.Is(err, ErrDeclaration) {
		t.Fatal("invalid original environment hidden by an override", err)
	}
	input.Path = filepath.Join(t.TempDir(), "missing")
	if _, err := LoadBootstrap(context.Background(), input, dependencies.Local); !errors.Is(err, ErrMissing) {
		t.Fatal("missing bootstrap was not distinguished", err)
	}
}
func TestAdmissionAndInputIsolation(t *testing.T) {
	dependencies, inbox := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "application.yaml")
	write(t, path, "workers: 2")
	plan := localPlan(path, true)
	defaultValue := 7
	declared := schema()
	declared.Defaults = application{Labels: []string{"original"}, Defaults: map[string]string{"key": "original"}, Optional: &defaultValue}
	selected := options()
	selected.Variables = []Variable{{Path: "/workers", Present: true, JSON: true, Value: "3"}}
	badSchema := declared
	badSchema.Validate = nil
	if value, err := startSession(context.Background(), plan, badSchema, selected, dependencies); value != nil || !errors.Is(err, ErrDeclaration) {
		t.Fatal("nil validator admitted", err)
	}
	state, _ := inbox.Inspect()
	if state.Queued != 0 {
		t.Fatal("rejected declaration created evidence")
	}
	session, err := startSession(context.Background(), plan, declared, selected, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, false)
	plan.Local.Paths[0] = "/not/the/selected/path"
	declared.Defaults.Labels[0] = "mutated"
	declared.Defaults.Defaults["key"] = "mutated"
	defaultValue = 99
	selected.Variables[0].Value = "9"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.waitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	value := read(t, session.Reader())
	if value.Workers != 3 || value.Labels[0] != "original" || value.Defaults["key"] != "original" || *value.Optional != 7 {
		t.Fatal("retained caller aliases")
	}
	write(t, path, "workers: 4")
	update, err := session.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for update.Publication < 2 {
		update, err = session.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	if read(t, session.Reader()).Workers != 3 {
		t.Fatal("captured variable changed")
	}
}
func TestBootstrapEvidencePressure(t *testing.T) {
	dependencies, _ := localDependencies(t, 1)
	path := filepath.Join(t.TempDir(), "application.yaml")
	write(t, path, "workers: 1")
	client, err := viper.New(dependencies.Local)
	if err != nil {
		t.Fatal(err)
	}
	source, err := client.Source(*localPlan(path, false).Local)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Capture(context.Background()); err != nil {
		t.Fatal(err)
	}
	session, err := startSession(context.Background(), localPlan(path, false), schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.waitInitial(ctx); err == nil {
		t.Fatal("pending evidence was discarded")
	}
	if _, err := session.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("failed initial load published")
	}
}
func TestRejectedPlans(t *testing.T) {
	dependencies, _ := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "application.yaml")
	for _, mutate := range []func(*SourcePlan){
		func(value *SourcePlan) { value.Version = 2 },
		func(value *SourcePlan) { value.Environment = "../bad" },
		func(value *SourcePlan) { value.Provider = "nacos" },
		func(value *SourcePlan) { value.Documents = append(value.Documents, value.Documents[0]) },
		func(value *SourcePlan) { value.Local.QueueCapacity = 65 },
		func(value *SourcePlan) { value.Local.Interval = time.Nanosecond },
		func(value *SourcePlan) { value.Local.Paths = []string{"relative"} },
	} {
		plan := localPlan(path, false)
		mutate(&plan)
		if session, err := startSession(context.Background(), plan, schema(), options(), dependencies); err == nil || session != nil {
			t.Fatal("invalid source plan admitted")
		}
	}
}
