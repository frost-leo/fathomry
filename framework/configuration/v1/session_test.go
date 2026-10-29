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
	"io"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestFiniteAndWatch(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite", true: "watch"}[watch], func(t *testing.T) {
			dependencies, _ := localDependencies(t, 16)
			path := filepath.Join(t.TempDir(), "app.yaml")
			write(t, path, "workers: 2\nlabels: [original]")
			selected := options()
			selected.InitialTimeout = 200 * time.Millisecond
			session, err := Start(context.Background(), localPlan(path, watch), schema(), selected, dependencies)
			if err != nil {
				t.Fatal(err)
			}
			defer closeSession(t, session, false)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := session.WaitInitial(ctx); err != nil {
				t.Fatal(err)
			}
			original, err := session.Reader().Capture()
			if err != nil {
				t.Fatal(err)
			}
			first, err := session.Next(ctx)
			if err != nil || !first.Published {
				t.Fatal("first report", err)
			}
			if watch {
				write(t, path, "workers: 0")
				rejected, err := session.Next(ctx)
				if err != nil || !errors.Is(rejected.Err, invalidWorkers) || rejected.Published {
					t.Fatal("rejection lost", err, rejected.Err)
				}
				if read(t, session.Reader()).Workers != 2 {
					t.Fatal("rejection replaced accepted data")
				}
				// Cross the first-acceptance timeout without cancelling an accepted watch.
				time.Sleep(220 * time.Millisecond)
				write(t, path, "workers: 4")
				accepted, err := session.Next(ctx)
				if err != nil || !accepted.Published || accepted.Publication != 2 {
					t.Fatal("watch failed after initial timeout", err, accepted.Err)
				}
			}
			closeSession(t, session, false)
			if read(t, session.Reader()).Workers < 2 {
				t.Fatal("reader lost accepted data on close")
			}
			old, err := settings.As[application](original)
			if err != nil {
				t.Fatal(err)
			}
			oldValue, err := old.ValueCopy()
			if err != nil || oldValue.Workers != 2 {
				t.Fatal("old view mutated")
			}
			for {
				_, err := session.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := session.WaitInitial(ctx); err != nil {
				t.Fatal("historical first result changed", err)
			}
		})
	}
}
func TestWaitCancellationAndLateValidator(t *testing.T) {
	dependencies, _ := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "app.yaml")
	write(t, path, "workers: 2")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	declared := schema()
	declared.Validate = func(context.Context, application) error { once.Do(func() { close(entered) }); <-release; return nil }
	session, err := Start(context.Background(), localPlan(path, true), declared, options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		closeSession(t, session, true)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("validator not entered")
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if err := session.WaitInitial(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal("wait cancellation lost")
	}
	status, _ := session.Status()
	if status.State != Initializing {
		t.Fatal("wait cancelled the producer")
	}
	if err := session.Close(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal("unjoined close passed")
	}
	if session.ShutdownComplete() {
		t.Fatal("uncooperative callback was supposedly stopped")
	}
	close(release)
	closeSession(t, session, false)
	if _, err := session.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("late callback published", err)
	}
}
func TestInitializationTimeoutRetainsOwnership(t *testing.T) {
	dependencies, _ := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "app.yaml")
	write(t, path, "workers: 2")
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	declared := schema()
	declared.Validate = func(context.Context, application) error { once.Do(func() { close(entered) }); <-release; return nil }
	selected := options()
	selected.InitialTimeout = time.Second
	session, err := Start(context.Background(), localPlan(path, false), declared, selected, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		closeSession(t, session, true)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("not entered")
	}
	if err := session.WaitInitial(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("initial timeout not independent of validation", err)
	}
	if session.ShutdownComplete() {
		t.Fatal("forgot callback")
	}
	status, err := session.Status()
	if err != nil || !errors.Is(status.Latest.Err, context.DeadlineExceeded) {
		t.Fatal("initial failure status was hidden until cleanup", err)
	}
	update, err := session.Next(ctx)
	if err != nil || !errors.Is(update.Err, context.DeadlineExceeded) {
		t.Fatal("initial failure report waited for callback cleanup", err)
	}
	close(release)
	closeSession(t, session, true)
	if _, err := session.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("late publication", err)
	}
}
func TestConcurrentWaitersAndClose(t *testing.T) {
	dependencies, _ := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "app.yaml")
	write(t, path, "workers: 2")
	session, err := Start(context.Background(), localPlan(path, false), schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.WaitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Next(ctx); err != nil {
		t.Fatal(err)
	}
	wait, stop := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := session.Next(wait); result <- err }()
	for {
		session.state.mu.Lock()
		active := session.state.waiter
		session.state.mu.Unlock()
		if active {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("waiter not admitted")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if _, err := session.Next(ctx); !errors.Is(err, ErrLimit) {
		t.Fatal("overlap queued", err)
	}
	stop()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := session.Close(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if !session.ShutdownComplete() {
		t.Fatal("not complete")
	}
}
func TestFirstFailureAndCounters(t *testing.T) {
	dependencies, _ := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "app.yaml")
	write(t, path, "workers: 0")
	session, err := Start(context.Background(), localPlan(path, true), schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.WaitInitial(ctx); !errors.Is(err, invalidWorkers) {
		t.Fatal("validation cause lost", err)
	}
	closeSession(t, session, true)
	write(t, path, "workers: 2")
	session, err = Start(context.Background(), localPlan(path, true), schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, true)
	if err := session.WaitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	session.state.mu.Lock()
	session.state.publication = math.MaxUint64
	session.state.mu.Unlock()
	write(t, path, "workers: 3")
	for {
		value, err := session.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if errors.Is(value.Err, ErrLimit) {
			break
		}
	}
	if read(t, session.Reader()).Workers != 2 {
		t.Fatal("counter exhaustion published")
	}
}
func TestCandidateTimeoutAndReportingCoalescing(t *testing.T) {
	dependencies, _ := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "app.yaml")
	write(t, path, "workers: 1")
	declared := schema()
	var calls atomic.Int32
	declared.Validate = func(ctx context.Context, value application) error {
		calls.Add(1)
		if value.Workers == 2 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	selected := options()
	selected.CandidateTimeout = 20 * time.Millisecond
	session, err := Start(context.Background(), localPlan(path, true), declared, selected, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.WaitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	write(t, path, "workers: 2")
	for {
		value, err := session.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if errors.Is(value.Err, context.DeadlineExceeded) {
			break
		}
	}
	if read(t, session.Reader()).Workers != 1 {
		t.Fatal("timeout published")
	}
	write(t, path, "workers: 3")
	for read(t, session.Reader()).Workers != 3 {
		select {
		case <-ctx.Done():
			t.Fatal("recovery missing")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	// Report coalescing is independent of source acquisition or candidate processing.
	session.state.mu.Lock()
	session.state.reportLocked(Update{Sequence: 99, Stage: Acquisition, Gap: true})
	session.state.reportLocked(Update{Sequence: 100, Stage: Preparation, Err: invalidWorkers})
	session.state.mu.Unlock()
	update, err := session.Next(ctx)
	if err != nil || update.Skipped < 1 || !update.Gap || !errors.Is(update.Err, invalidWorkers) {
		t.Fatal("coalesced report lost facts", err)
	}
}
func TestBatchContract(t *testing.T) {
	batch, err := configsource.NewBatch([]configsource.Raw{{Content: []byte("workers: 2")}})
	if err != nil {
		t.Fatal(err)
	}
	documents := []Document{{Kind: "base", Encoding: configsource.YAML}}
	for _, index := range []int{-2, 0, 1} {
		if _, err := originalLayers(batch, index, nil, documents, "fixture"); !errors.Is(err, ErrObservation) {
			t.Fatal("invalid index accepted", err)
		}
	}
	if _, err := originalLayers(configsource.Batch{}, -1, nil, documents, "fixture"); !errors.Is(err, ErrObservation) {
		t.Fatal("invalid batch")
	}
}
