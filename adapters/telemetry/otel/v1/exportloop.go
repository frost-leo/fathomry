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

package otel

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

// ExportOptions explicitly enables a fixed-period schedule when passed to
// StartExport. Both values are required: Interval is 1ms..24h and Timeout is
// 1ms..1min. Durations use nanoseconds in configuration. Timeout bounds admission
// and waiting; actual native work must still join before another export begins.
type ExportOptions struct {
	Interval time.Duration `json:"interval_ns"`
	Timeout  time.Duration `json:"timeout_ns"`
}

// ExportStatus is a detached observation of one schedule, not delivery or
// durability evidence. Counters saturate. Skipped counts elapsed scheduled ticks
// that were coalesced while an earlier export still owned work. LastError retains
// the most recent failure even after subsequent success.
type ExportStatus struct {
	private
	Attempts  uint64
	Succeeded uint64
	Failures  uint64
	Skipped   uint64
	Running   bool
	Stopped   bool
	LastError error
}

// ExportLoop owns one explicitly started scheduler for one physical source.
// Copies share authority. Stop ends scheduling, not source ownership or evidence
// reception; a timed-out Stop leaves this same handle responsible for joining.
type ExportLoop struct {
	private
	state *exportState
}

type exportState struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	status ExportStatus
}

// StartExport starts one non-overlapping schedule on this owner's direct source.
// It never follows a Ref or acquires a permanent resource lease/root allowance.
// A retired generation keeps its own schedule until its Release/Close begins.
// A second active schedule is refused; a joined schedule can be explicitly
// restarted until source shutdown begins. Receivers remain caller-owned.
func (owner *Owner) StartExport(ctx context.Context, options ExportOptions) (*ExportLoop, error) {
	if ctx == nil || owner == nil || owner.state == nil || owner.client == nil ||
		options.Interval < time.Millisecond || options.Interval > 24*time.Hour ||
		options.Timeout < time.Millisecond || options.Timeout > time.Minute {
		return nil, fail(ErrInput, "export-start")
	}
	if ctx.Err() != nil {
		return nil, fail(ErrState, "export-start", ctx.Err(), context.Cause(ctx))
	}
	owner.loopMu.Lock()
	defer owner.loopMu.Unlock()
	if owner.exportClosing || owner.state.complete.Load() || owner.state.call == nil || owner.state.call.Context().Err() != nil {
		return nil, fail(ErrState, "export-start")
	}
	if owner.loop != nil {
		select {
		case <-owner.loop.state.done:
		default:
			return nil, fail(ErrState, "export-start")
		}
	}
	lifetime, cancel := context.WithCancel(ctx)
	state := &exportState{ctx: lifetime, cancel: cancel, done: make(chan struct{})}
	loop := &ExportLoop{state: state}
	owner.loop = loop
	stopOwner := context.AfterFunc(owner.state.call.Context(), cancel)
	go func() {
		defer stopOwner()
		state.run(options, owner.client.Flush)
	}()
	return loop, nil
}

func (state *exportState) run(options ExportOptions, flush func(context.Context) (*adapters.Receipt[Result], error)) {
	defer close(state.done)
	defer state.cancel()
	defer func() {
		state.mu.Lock()
		state.status.Running = false
		state.status.Stopped = true
		state.mu.Unlock()
	}()
	next := time.Now().Add(options.Interval)
	timer := time.NewTimer(options.Interval)
	defer timer.Stop()
	for {
		select {
		case <-state.ctx.Done():
			return
		case <-timer.C:
		}
		if state.ctx.Err() != nil {
			return
		}
		state.mu.Lock()
		state.status.Attempts = exportCount(state.status.Attempts, 1)
		state.status.Running = true
		state.mu.Unlock()
		work, cancel := context.WithTimeout(state.ctx, options.Timeout)
		receipt, err := flush(work)
		if receipt != nil {
			snapshot, waitErr := receipt.WaitReleased(work)
			if waitErr != nil {
				cancel()
				var joinErr error
				snapshot, joinErr = receipt.WaitReleased(context.Background())
				err = errors.Join(err, waitErr, joinErr)
			}
			err = errors.Join(err, snapshot.Err())
		} else if err == nil {
			err = fail(ErrState, "export-receipt")
		}
		cancel()
		finished := time.Now()
		elapsed := max(time.Duration(0), finished.Sub(next))
		skipped := uint64(elapsed / options.Interval)
		next = finished.Add(options.Interval - elapsed%options.Interval)
		state.mu.Lock()
		state.status.Running = false
		state.status.Skipped = exportCount(state.status.Skipped, skipped)
		if err != nil {
			state.status.Failures = exportCount(state.status.Failures, 1)
			state.status.LastError = translate(err, "export-loop")
		} else {
			state.status.Succeeded = exportCount(state.status.Succeeded, 1)
		}
		state.mu.Unlock()
		if state.ctx.Err() != nil {
			return
		}
		timer.Reset(time.Until(next))
	}
}

func exportCount(value, increment uint64) uint64 {
	if increment > math.MaxUint64-value {
		return math.MaxUint64
	}
	return value + increment
}

// Status returns a detached payload-free snapshot. LastError is safely presented;
// its deliberately inspectable cause graph may contain sensitive native details.
func (loop *ExportLoop) Status() (ExportStatus, error) {
	if loop == nil || loop.state == nil {
		return ExportStatus{}, fail(ErrState, "export-status")
	}
	loop.state.mu.Lock()
	defer loop.state.mu.Unlock()
	return loop.state.status, nil
}

// Stop cancels scheduling and joins the actual Flush stack and accepted receipt.
// It does not perform an additional export, close the source or seal evidence.
// Owner.Close performs final native export after producer work has been joined.
// A historical export failure remains in Status and its independent evidence.
func (loop *ExportLoop) Stop(ctx context.Context) error {
	if loop == nil || loop.state == nil || ctx == nil {
		return fail(ErrInput, "export-stop")
	}
	loop.state.cancel()
	select {
	case <-loop.state.done:
		return nil
	default:
	}
	select {
	case <-loop.state.done:
		return nil
	case <-ctx.Done():
		return fail(adapters.ErrWait, "export-stop", ctx.Err(), context.Cause(ctx))
	}
}

func (owner *Owner) stopExport(ctx context.Context) error {
	owner.loopMu.Lock()
	owner.exportClosing = true
	loop := owner.loop
	owner.loopMu.Unlock()
	if loop == nil {
		return nil
	}
	return loop.Stop(ctx)
}
