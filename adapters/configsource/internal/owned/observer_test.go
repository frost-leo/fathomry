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

package owned

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestCloseRetainsResponsibilityAndFencesData(t *testing.T) {
	for _, actualFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful-join", true: "actual-cleanup"}[actualFailure], func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			ready, late := make(chan struct{}), make(chan struct{})
			cleanup := errors.New("actual-cleanup")
			operation := errors.New("operation-canary")
			handle := Observe(context.Background(), func(ctx context.Context, publish func(source.Batch, error)) error {
				batch, _ := NewBatch([]Entry{{Name: "slot", Presence: source.Present, Raw: []byte("old")}})
				publish(batch, nil)
				publish(nil, Fail(source.ErrValue, operation))
				close(ready)
				<-ctx.Done()
				batch, _ = NewBatch([]Entry{{Name: "slot", Presence: source.Present, Raw: []byte("new")}})
				publish(batch, nil)
				close(late)
				<-release
				if actualFailure {
					return cleanup
				}
				return nil
			})
			<-ready
			old, _ := handle.Current()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if err := handle.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("pending close not retained", err)
			}
			<-late
			closing, _ := handle.Current()
			raw, _, _ := closing.Batch.RawCopy("slot")
			if closing.Status != source.Closing || string(raw) != "old" || closing.Generation != old.Generation {
				t.Fatal("data published after Closing")
			}
			once.Do(func() { close(release) })
			joined, cancelJoin := context.WithTimeout(context.Background(), time.Second)
			defer cancelJoin()
			err := handle.Close(joined)
			if actualFailure {
				if !errors.Is(err, cleanup) || errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("cleanup/wait errors mixed", err)
				}
			} else if err != nil {
				t.Fatal("past wait poisoned later Close", err)
			}
			terminal, _ := handle.Current()
			if terminal.Status != source.Closed {
				t.Fatal("close returned before join")
			}
			if !errors.Is(terminal.Failure, operation) || actualFailure && !errors.Is(terminal.Failure, cleanup) {
				t.Fatal("terminal state lost operation or cleanup")
			}
			if _, err := handle.Next(context.Background(), terminal.Cursor); !errors.Is(err, source.ErrClosed) {
				t.Fatal("terminal Next did not terminate", err)
			}
		})
	}
}
func TestSingleWaiterAndStatusRecoveryGeneration(t *testing.T) {
	sends := make(chan func(func(source.Batch, error)), 1)
	handle := Observe(context.Background(), func(ctx context.Context, publish func(source.Batch, error)) error {
		for {
			select {
			case action := <-sends:
				action(publish)
			case <-ctx.Done():
				return nil
			}
		}
	})
	t.Cleanup(func() {
		if err := handle.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	value := handle.(*observer)
	batch, _ := NewBatch([]Entry{{Name: "slot", Presence: source.Present, Raw: []byte{}}})
	sends <- func(publish func(source.Batch, error)) { publish(batch, nil) }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state, _ := handle.Current()
	for state.Status != source.Available {
		var err error
		state, err = handle.Next(ctx, state.Cursor)
		if err != nil {
			t.Fatal(err)
		}
	}
	initial := state.Generation
	failure := errors.New("failure")
	sends <- func(publish func(source.Batch, error)) { publish(nil, failure) }
	state, err := handle.Next(ctx, state.Cursor)
	if err != nil || state.Status != source.Degraded {
		t.Fatal("failure missing", err)
	}
	sends <- func(publish func(source.Batch, error)) { publish(batch, nil) }
	state, err = handle.Next(ctx, state.Cursor)
	if err != nil || state.Status != source.Available || state.Generation != initial {
		t.Fatal("status recovery changed data generation", err)
	}
	wait, stop := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { _, err := handle.Next(wait, state.Cursor); finished <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		value.mu.Lock()
		waiting := value.waiting
		value.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("wait not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := handle.Next(ctx, state.Cursor); !errors.Is(err, source.ErrBusy) {
		t.Fatal("concurrent wait admitted", err)
	}
	stop()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	raw, _, _ := state.Batch.RawCopy("slot")
	if len(raw) != 0 {
		t.Fatal("present empty changed")
	}
}
func FuzzBatchBoundary(f *testing.F) {
	f.Add("slot", []byte("utf8"), uint8(source.Present))
	f.Add("slot", []byte{}, uint8(source.Missing))
	f.Add("bad name", []byte{255}, uint8(0))
	f.Fuzz(func(t *testing.T, name string, raw []byte, presence uint8) {
		if len(raw) > source.MaxDocumentBytes+1 {
			t.Skip()
		}
		batch, err := NewBatch([]Entry{{Name: name, Presence: source.Presence(presence), Raw: raw}})
		if err != nil {
			if batch != nil {
				t.Fatal("failure returned a batch")
			}
			return
		}
		copy, actual, err := batch.RawCopy(name)
		if err != nil || actual != source.Presence(presence) || string(copy) != string(raw) {
			t.Fatal("raw fidelity lost")
		}
		if len(raw) > 0 {
			raw[0] ^= 255
			again, _, _ := batch.RawCopy(name)
			if string(again) != string(copy) {
				t.Fatal("borrowed mutable storage")
			}
		}
	})
}

func TestSlowConsumerCoalescesWithoutRawHistory(t *testing.T) {
	ready := make(chan struct{})
	handle := Observe(context.Background(), func(ctx context.Context, publish func(source.Batch, error)) error {
		for index := range 1000 {
			batch, err := NewBatch([]Entry{{Name: "slot", Presence: source.Present, Raw: []byte(fmt.Sprint(index))}})
			if err != nil {
				return err
			}
			publish(batch, nil)
		}
		close(ready)
		<-ctx.Done()
		return nil
	})
	t.Cleanup(func() {
		if err := handle.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	<-ready
	state, err := handle.Next(context.Background(), nil)
	if err != nil || state.Status != source.Available || state.Generation != 1000 {
		t.Fatal("coalesced state lost latest generation", err)
	}
	raw, _, _ := state.Batch.RawCopy("slot")
	if string(raw) != "999" {
		t.Fatal("slow consumer received stale queued payload")
	}
	wait, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := handle.Next(wait, state.Cursor); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("historical event queue unexpectedly retained", err)
	}
}

func TestLifetimeCancelRetainsOperationAndActualCleanup(t *testing.T) {
	for _, cleanupFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(cleanupFailure), func(t *testing.T) {
			operation, cancellation, cleanup := errors.New("operation"), errors.New("cancellation"), errors.New("cleanup")
			parent, cancel := context.WithCancelCause(context.Background())
			ready := make(chan struct{})
			handle := Observe(parent, func(ctx context.Context, publish func(source.Batch, error)) error {
				publish(nil, Fail(source.ErrValue, operation))
				close(ready)
				<-ctx.Done()
				if cleanupFailure {
					return Fail(source.ErrValue, cleanup)
				}
				return nil
			})
			<-ready
			cancel(cancellation)
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			err := handle.Close(ctx)
			if cleanupFailure {
				if !errors.Is(err, cleanup) || errors.Is(err, operation) {
					t.Fatal("Close mixed operation and cleanup", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			state, _ := handle.Current()
			if !errors.Is(state.Failure, operation) || !errors.Is(state.Failure, cancellation) || !errors.Is(state.Failure, context.Canceled) || cleanupFailure && !errors.Is(state.Failure, cleanup) {
				t.Fatal("terminal evidence was replaced")
			}
		})
	}
}
