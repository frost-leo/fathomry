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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

type finiteSource struct{ raw string }

func (source finiteSource) Capture(context.Context) (configsource.Batch, int, error) {
	batch, err := configsource.NewBatch([]configsource.Raw{{Content: []byte(source.raw)}})
	return batch, -1, err
}
func (finiteSource) Observe(context.Context) (configsource.Observer, error) {
	return nil, errors.New("finite source")
}

func TestScenarioOwnership(t *testing.T) {
	type data struct {
		Value string `json:"value"`
	}
	declaration := Declaration[data]{Schema: Schema[data]{Version: 1}}
	t.Run("provider_is_inert_and_frozen", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "selected.json")
		write(t, path, `{"value":"selected"}`)
		documents := []File{{Path: path, Kind: Base, Encoding: JSON}}
		provider, err := Viper(ViperOptions{Documents: documents})
		if err != nil {
			t.Fatal(err)
		}
		documents[0].Path = filepath.Join(root, "not-selected")
		t.Setenv("FATHOMRY_CONFIG_PROVIDER", "nacos")
		t.Setenv("FATHOMRY_NACOS_PASSWORD", strings.Repeat("x", 1<<20))
		result, err := Load(context.Background(), declaration, Dependencies{Provider: provider})
		if err != nil {
			t.Fatal(err)
		}
		accepted, _ := result.State.Capture()
		value, _ := accepted.ValueCopy()
		if value.Value != "selected" || len(result.Records) != 2 {
			t.Fatal("provider selection or freezing lost")
		}
		status, _ := result.State.Status()
		if !status.Released {
			t.Fatal("finite owners still live")
		}
	})
	t.Run("invalid_declaration_before_source_io", func(t *testing.T) {
		opened := false
		provider := Provider{state: &providerPlan{name: "test", layers: []layer{{Kind: Base, Encoding: JSON}}, open: func(context.Context, *adapters.Runtime) (sourceBinding, error) {
			opened = true
			return sourceBinding{}, nil
		}}}
		invalid := declaration
		invalid.Schema.Version = 0
		if result, err := Load(context.Background(), invalid, Dependencies{Provider: provider}); err == nil || result.State != nil || opened {
			t.Fatal("schema refusal acquired source")
		}
		if _, err := Load(context.Background(), declaration, Dependencies{}); err == nil {
			t.Fatal("zero provider inferred")
		}
	})
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "partial_open", true: "accepted_then_cleanup_error"}[accepted], func(t *testing.T) {
			primary, cleanup := errors.New("open failed"), errors.New("cleanup failed")
			closed := false
			provider := Provider{state: &providerPlan{name: "test", layers: []layer{{Kind: Base, Encoding: JSON}}, open: func(context.Context, *adapters.Runtime) (sourceBinding, error) {
				binding := sourceBinding{source: finiteSource{raw: `{"value":"accepted"}`}, close: func(ctx context.Context) error {
					closed = true
					if ctx.Err() != nil {
						t.Error("canceled cleanup")
					}
					return cleanup
				}}
				if !accepted {
					return binding, primary
				}
				return binding, nil
			}}}
			result, err := Load(context.Background(), declaration, Dependencies{Provider: provider})
			if !closed || !errors.Is(err, ErrCleanup) || !errors.Is(err, cleanup) || (result.State != nil) != accepted || !accepted && !errors.Is(err, primary) || len(result.Records) != 1 {
				t.Fatal("partial ownership/causes lost", err)
			}
			if !result.Records[0].Info().Released {
				t.Fatal("unreleased evidence transferred")
			}
		})
	}
	t.Run("finite_cancellation_joins_cleanup", func(t *testing.T) {
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		var once sync.Once
		defer once.Do(func() { close(release) })
		provider := Provider{state: &providerPlan{name: "test", layers: []layer{{Kind: Base, Encoding: JSON}}, open: func(context.Context, *adapters.Runtime) (sourceBinding, error) {
			return sourceBinding{source: finiteSource{raw: `{"value":"accepted"}`}, close: func(ctx context.Context) error {
				close(entered)
				<-release
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return nil
			}}, nil
		}}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _, err := Load(ctx, declaration, Dependencies{Provider: provider}); done <- err }()
		<-entered
		cancel()
		select {
		case <-done:
			t.Fatal("cleanup abandoned")
		case <-time.After(10 * time.Millisecond):
		}
		once.Do(func() { close(release) })
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("cleanup did not join")
		}
	})
	t.Run("canceled_watch_startup", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "selected.json")
		write(t, path, `{"value":"selected"}`)
		provider, err := Viper(ViperOptions{Documents: []File{{Path: path, Kind: Base, Encoding: JSON}}})
		if err != nil {
			t.Fatal(err)
		}
		cause := errors.New("startup canceled by caller")
		for attempt := range 512 {
			ctx, cancel := context.WithCancelCause(context.Background())
			canceled := make(chan struct{})
			go func() {
				if attempt%4 == 0 {
					runtime.Gosched()
				} else {
					time.Sleep(time.Duration(attempt%4) * time.Microsecond)
				}
				cancel(cause)
				close(canceled)
			}()
			watch, err := Watch(ctx, declaration, Dependencies{Provider: provider}, WatchOptions{})
			<-canceled
			if watch != nil {
				_ = watch.Close(context.Background())
				if _, released := watch.Records(); !released {
					t.Fatal("canceled watch retained unfinished cleanup")
				}
			} else if err == nil {
				t.Fatal("startup returned no owner and no error")
			}
			if err != nil && (errors.Is(err, ErrCleanup) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause)) {
				t.Fatalf("canceled startup lost classification or cause at attempt %d: %v", attempt, err)
			}
		}
	})
	t.Run("failed_watch_setup_cleanup_semantics", func(t *testing.T) {
		for _, test := range []struct {
			name                string
			canceled            bool
			cleanupFails        bool
			cancelDuringCleanup bool
		}{
			{name: "original_error"},
			{name: "canceled_bind", canceled: true},
			{name: "cleanup_error", cleanupFails: true},
			{name: "canceled_and_cleanup_error", canceled: true, cleanupFails: true},
			{name: "later_cancellation", cancelDuringCleanup: true},
		} {
			t.Run(test.name, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				scenario, err := newScenario(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer scenario.runtime.Close(context.Background())
				primary, cleanup, cause := errors.New("admission failure"), errors.New("cleanup failure"), errors.New("caller stop")
				if test.canceled {
					cancel(cause)
					_, primary = scenario.endpoint()
					if !errors.Is(primary, adapters.ErrClosed) {
						t.Fatal("canceled runtime did not refuse Bind", primary)
					}
				}
				closes := 0
				scenario.binding.close = func(cleanupContext context.Context) error {
					closes++
					if cleanupContext.Err() != nil || context.Cause(cleanupContext) != nil {
						t.Error("cleanup inherited caller cancellation")
					}
					if test.cancelDuringCleanup {
						cancel(cause)
					}
					if test.cleanupFails {
						return cleanup
					}
					return nil
				}
				err = scenario.failWatch(ctx, primary)
				if !errors.Is(err, primary) || errors.Is(err, ErrCleanup) != test.cleanupFails || errors.Is(err, cleanup) != test.cleanupFails {
					t.Fatal("startup and cleanup causes were conflated", err)
				}
				if errors.Is(err, ErrClosed) != test.canceled || errors.Is(err, context.Canceled) != test.canceled || errors.Is(err, cause) != test.canceled {
					t.Fatal("startup cancellation identity was lost or fabricated", err)
				}
				if !test.canceled && !test.cleanupFails && err != primary {
					t.Fatal("original admission error replaced")
				}
				status, _ := scenario.runtime.Inspect()
				custody, _ := scenario.evidence.Inspect()
				if closes != 1 || !status.Closed || status.Active != 0 || custody.Outstanding != 0 {
					t.Fatal("failed startup did not join its owners")
				}
			})
		}
	})
}

func TestWatchScenarioFinalization(t *testing.T) {
	observer := newObserver()
	entered, release := make(chan struct{}), make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(release) })
	var closes atomic.Int32
	provider := Provider{state: &providerPlan{name: "test", layers: []layer{{Kind: Base, Encoding: JSON}}, open: func(context.Context, *adapters.Runtime) (sourceBinding, error) {
		return sourceBinding{source: &testSource{observer: observer}, close: func(context.Context) error { closes.Add(1); close(entered); <-release; return nil }}, nil
	}}}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := Watch(parent, Declaration[model]{Schema: modelSchema()}, Dependencies{Provider: provider}, WatchOptions{QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	observer.input <- observation(t, `{"service":{"port":123}}`)
	nextDecision(t, watch, true)
	for range 100 {
		observer.input <- observation(t, `{"service":{"port":123}}`)
		nextDecision(t, watch, true)
	}
	cancel()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not initiate owned cleanup")
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := watch.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close abandoned blocked provider", err)
	}
	if _, ready := watch.Records(); ready {
		t.Fatal("records available before source release")
	}
	status, _ := watch.Status()
	if !status.Closed || status.Released {
		t.Fatal("publication fence confused with release")
	}
	released.Do(func() { close(release) })
	var group sync.WaitGroup
	for range 4 {
		group.Go(func() {
			if err := watch.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if closes.Load() != 1 {
		t.Fatal("source closed repeatedly")
	}
	records, ready := watch.Records()
	if !ready || len(records) != 1 || !records[0].Info().Released {
		t.Fatal("bounded final custody lost")
	}
	records[0] = Record{}
	again, _ := watch.Records()
	if !again[0].Info().Released {
		t.Fatal("caller altered watcher evidence")
	}
	state, _ := watch.Capture()
	value, _ := state.ValueCopy()
	if value.Service.Port != 123 {
		t.Fatal("shutdown erased accepted data")
	}
	stats, _ := watch.state.scenario.runtime.Inspect()
	if !stats.Closed || stats.Active != 0 {
		t.Fatal("scenario runtime remains active")
	}
}
