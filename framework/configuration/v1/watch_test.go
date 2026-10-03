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
	"io"
	"sync"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type testSource struct {
	observer  *testObserver
	openError error
}

func (source *testSource) Capture(context.Context) (configsource.Batch, int, error) {
	return configsource.Batch{}, -1, errors.New("capture not selected")
}
func (source *testSource) Observe(context.Context) (configsource.Observer, error) {
	return source.observer, source.openError
}

type testObserver struct {
	input        chan configsource.Observation
	stopped      chan struct{}
	once         sync.Once
	closeEntered chan struct{}
	closeRelease chan struct{}
	closeError   error
}

func newObserver() *testObserver {
	return &testObserver{input: make(chan configsource.Observation, 16), stopped: make(chan struct{})}
}
func (observer *testObserver) Next(ctx context.Context) (configsource.Observation, error) {
	select {
	case value := <-observer.input:
		return value, nil
	case <-observer.stopped:
		return configsource.Observation{}, io.EOF
	case <-ctx.Done():
		return configsource.Observation{}, ctx.Err()
	}
}
func (observer *testObserver) Close(context.Context) error {
	observer.once.Do(func() {
		close(observer.stopped)
		if observer.closeEntered != nil {
			close(observer.closeEntered)
		}
	})
	if observer.closeRelease != nil {
		<-observer.closeRelease
	}
	return observer.closeError
}
func observation(t *testing.T, raw string) configsource.Observation {
	t.Helper()
	batch, err := configsource.NewBatch([]configsource.Raw{{Content: []byte(raw)}})
	if err != nil {
		t.Fatal(err)
	}
	return configsource.Observation{Batch: batch, FailedIndex: -1}
}
func waitStatus[T any](t *testing.T, watch *Watcher[T], predicate func(Status) bool) Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	timer := time.NewTicker(time.Millisecond)
	defer timer.Stop()
	for {
		status, err := watch.Status()
		if err != nil {
			t.Fatal(err)
		}
		if predicate(status) {
			return status
		}
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatal("configuration status did not reach expected state")
		}
	}
}
func nextDecision[T any](t *testing.T, watch *Watcher[T], accepted bool) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		event, err := watch.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if event.Accepted == accepted && !event.Superseded {
			return event
		}
	}
}
func TestWatchOrdering(t *testing.T) {
	for _, encoding := range []Encoding{JSON, TOML} {
		t.Run(string(encoding), func(t *testing.T) { testWatchOrdering(t, encoding) })
	}
}
func testWatchOrdering(t *testing.T, encoding Encoding) {
	document := func(port int) string {
		if encoding == TOML {
			return fmt.Sprintf("[service]\nport = %d\n", port)
		}
		return fmt.Sprintf(`{"service":{"port":%d}}`, port)
	}
	deps := Dependencies{}
	observer := newObserver()
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	schema := modelSchema()
	validate := schema.Validate
	schema.Validate = func(ctx context.Context, value model) error {
		if value.Service.Port == 2 {
			close(entered)
			<-release
		}
		return validate(ctx, value)
	}
	t.Setenv("FATHOMRY_WATCH_HOST", "must-not-be-read")
	variables := []Variable{{Path: "/service/host", Value: "captured", Present: true}}
	deps.Provider = sourceProvider(&testSource{observer: observer}, encoding)
	watch, err := Watch(context.Background(), Declaration[model]{
		Schema:    schema,
		Variables: variables,
	}, deps, WatchOptions{QueueCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseOnce.Do(func() { close(release) })
		_ = watch.Close(context.Background())
	}()
	if _, err := watch.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("initial zero looked ready", err)
	}
	observer.input <- observation(t, document(1))
	nextDecision(t, watch, true)
	first, _ := watch.Capture()
	observer.input <- observation(t, document(2))
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("slow validator not entered")
	}
	observer.input <- observation(t, document(3))
	waitStatus(t, watch, func(value Status) bool { return value.Observed >= 3 })
	current, _ := watch.Capture()
	value, _ := current.ValueCopy()
	if value.Service.Port != 1 {
		t.Fatal("candidate published before validation")
	}
	releaseOnce.Do(func() { close(release) })
	nextDecision(t, watch, true)
	current, _ = watch.Capture()
	value, _ = current.ValueCopy()
	if value.Service.Port != 3 || current.Sequence() != 3 || current.Description().Revision == first.Description().Revision {
		t.Fatal("obsolete validation published")
	}
	observer.input <- observation(t, document(0))
	if event := nextDecision(t, watch, false); event.Err == nil {
		t.Fatal("invalid update not rejected")
	}
	current, _ = watch.Capture()
	value, _ = current.ValueCopy()
	if value.Service.Port != 3 {
		t.Fatal("invalid update replaced last-good")
	}
	marker := errors.New("source-private-canary")
	observer.input <- configsource.Observation{FailedIndex: 0, Err: marker, Gap: true}
	event := nextDecision(t, watch, false)
	if !errors.Is(event.Err, marker) || event.Status.SourceError == nil {
		t.Fatal("source health failure lost")
	}
	t.Setenv("FATHOMRY_WATCH_HOST", "later")
	variables[0].Value = "later"
	deps.Provider = Provider{}
	observer.input <- observation(t, document(5))
	nextDecision(t, watch, true)
	current, _ = watch.Capture()
	value, _ = current.ValueCopy()
	if value.Service.Host != "captured" || value.Service.Port != 5 {
		t.Fatal("environment was reread or recovery failed")
	}
	if err := watch.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	last, _ := watch.Capture()
	if last.Description().Revision != current.Description().Revision {
		t.Fatal("shutdown rewrote accepted data")
	}
}
func TestWatchCloseRetainsValidation(t *testing.T) {
	deps := Dependencies{}
	observer := newObserver()
	entered := make(chan struct{})
	release := make(chan struct{})
	schema := modelSchema()
	schema.Validate = func(context.Context, model) error { close(entered); <-release; return nil }
	deps.Provider = sourceProvider(&testSource{observer: observer}, JSON)
	watch, err := Watch(context.Background(), Declaration[model]{Schema: schema}, deps, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	observer.input <- observation(t, "{}")
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := watch.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("abandoned validator", err)
	}
	if _, err := watch.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("closed validator published")
	}
	stats, _ := watch.state.scenario.runtime.Inspect()
	if stats.Active != 1 {
		t.Fatal("actual validation work released", stats)
	}
	close(release)
	if err := watch.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("late publication after Close")
	}
}
func TestWatchPartialInitializationAndCleanup(t *testing.T) {
	deps := Dependencies{}
	observer := newObserver()
	observer.closeEntered = make(chan struct{})
	observer.closeRelease = make(chan struct{})
	marker := errors.New("partial acquisition")
	deps.Provider = sourceProvider(&testSource{observer: observer, openError: marker}, JSON)
	watch, err := Watch(context.Background(), Declaration[model]{Schema: modelSchema()}, deps, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-observer.closeEntered
	if _, err := watch.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("failed startup published")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := watch.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("partial owner discarded", err)
	}
	close(observer.closeRelease)
	if err := watch.Close(context.Background()); !errors.Is(err, marker) {
		t.Fatal("initial cause lost", err)
	}
}
func TestWatchMalformedObservationAndOverflow(t *testing.T) {
	deps := Dependencies{}
	observer := newObserver()
	deps.Provider = sourceProvider(&testSource{observer: observer}, JSON)
	watch, err := Watch(context.Background(), Declaration[model]{Schema: modelSchema()}, deps, WatchOptions{QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close(context.Background())
	observer.input <- configsource.Observation{}
	if event := nextDecision(t, watch, false); !errors.Is(event.Err, ErrObservation) {
		t.Fatal(err)
	}
	for index := uint64(2); index <= 5; index++ {
		observer.input <- observation(t, `{"service":{"port":9}}`)
		waitStatus(t, watch, func(value Status) bool { return value.Published == index })
	}
	if event := nextDecision(t, watch, true); !event.Gap {
		t.Fatal("dropped notification did not report gap")
	}
}

func TestWatchConcurrentReadersAndSameValue(t *testing.T) {
	deps := Dependencies{}
	source := newObserver()
	deps.Provider = sourceProvider(&testSource{observer: source}, JSON)
	type pair struct {
		First  int `json:"first"`
		Second int `json:"second"`
	}
	watch, err := Watch(context.Background(), Declaration[pair]{
		Schema: Schema[pair]{Version: 1, Validate: func(_ context.Context, value pair) error {
			if value.First != value.Second {
				return errors.New("mixed pair")
			}
			return nil
		}},
	}, deps, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close(context.Background())
	stop := make(chan struct{})
	failures := make(chan error, 4)
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				view, err := watch.Reader().Capture()
				if errors.Is(err, settings.ErrUnconfigured) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					failures <- err
					return
				}
				snapshot, err := settings.As[pair](view)
				if err != nil {
					failures <- err
					return
				}
				value, err := snapshot.ValueCopy()
				if err != nil {
					failures <- err
					return
				}
				if value.First != value.Second {
					failures <- errors.New("reader observed mixed fields")
					return
				}
			}
		})
	}
	defer func() { close(stop); readers.Wait() }()
	var previous string
	for index := 0; index < 12; index++ {
		raw := fmt.Sprintf("{\"first\":%d,\"second\":%d}", index/2, index/2)
		source.input <- observation(t, raw)
		nextDecision(t, watch, true)
		current, err := watch.Capture()
		if err != nil {
			t.Fatal(err)
		}
		if current.Description().Revision == previous {
			t.Fatal("fresh acceptance reused a revision")
		}
		previous = current.Description().Revision
	}
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
}
