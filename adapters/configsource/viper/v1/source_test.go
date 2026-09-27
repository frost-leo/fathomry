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

package viper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/internal/owned"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestObservePacesActualCaptureAttempts(t *testing.T) {
	var attempts atomic.Int32
	var mu sync.Mutex
	var starts []time.Time
	first := make(chan struct{})
	observer := observe(context.Background(), time.Second, func(context.Context) (source.Batch, error) {
		count := attempts.Add(1)
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		if count == 1 {
			close(first)
		}
		return nil, errors.New("persistent-acquisition-refusal")
	})
	t.Cleanup(func() {
		if err := observer.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	<-first
	// Deliberately never consume Next: coalescing cannot hide extra I/O attempts.
	time.Sleep(2300 * time.Millisecond)
	if count := attempts.Load(); count < 2 || count > 3 {
		t.Fatal("unpaced or missing actual attempts", count)
	}
	mu.Lock()
	defer mu.Unlock()
	for index := 1; index < len(starts); index++ {
		if starts[index].Sub(starts[index-1]) < time.Second {
			t.Fatal("attempts exceeded declared interval")
		}
	}
}

func TestShutdownAccountsLateFileCleanupNotBorrowedCause(t *testing.T) {
	for _, actual := range []bool{false, true} {
		t.Run(fmt.Sprint(actual), func(t *testing.T) {
			cleanup := errors.New("late-file-cleanup")
			began := make(chan struct{})
			observer := observe(context.Background(), time.Second, func(ctx context.Context) (source.Batch, error) {
				close(began)
				<-ctx.Done()
				info := source.AcquisitionInfo{Source: "fixture", Document: "slot", Phase: source.ObservePhase}
				if actual {
					return nil, owned.Acquisition(ErrClose, info, cleanup)
				}
				previous := owned.Acquisition(ErrClose, source.AcquisitionInfo{Source: "previous", Phase: source.CapturePhase}, cleanup)
				return nil, owned.Acquisition(ErrRead, info, ctx.Err(), previous)
			})
			<-began
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := observer.Close(ctx)
			state, problem := observer.Current()
			if problem != nil || state.Status != source.Closed {
				t.Fatal("local owner not joined", problem)
			}
			if actual {
				if !errors.Is(err, cleanup) || !errors.Is(state.Failure, cleanup) {
					t.Fatal("late actual cleanup discarded")
				}
			} else if err != nil || errors.Is(state.Failure, cleanup) {
				t.Fatal("borrowed prior cleanup was treated as current cleanup", err)
			}
		})
	}
}
