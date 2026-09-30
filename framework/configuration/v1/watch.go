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
	"math"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// Event reports one acceptance/rejection or obsolete validation. Notifications
// are bounded; Gap means notification/input history was coalesced. State.Reader
// remains the authoritative accepted data, not this optional event queue.
type Event struct {
	private
	Sequence   uint64
	Accepted   bool
	Superseded bool
	Gap        bool
	Err        error
	Status     Status
}

// Watcher owns source observation, ingress and serial validation. Its Reader is
// initially unconfigured; successful construction is not configuration readiness.
type Watcher[T any] struct {
	private
	state *watchState[T]
	_     typeIdentity[T]
}
type pending struct {
	observation configsource.Observation
	sequence    uint64
}
type watchState[T any] struct {
	data             *State[T]
	plan             plan[T]
	dependencies     Dependencies
	ctx              context.Context
	cancel           context.CancelCauseFunc
	work             context.Context
	receipt          *adapters.Receipt[Evidence]
	capacity         int
	events           []Event
	eventsChanged    chan struct{}
	eventsGap        bool
	input            *pending
	inputChanged     chan struct{}
	ingressStopped   bool
	validationCancel context.CancelFunc
	terminal         error
}

// Watch freezes declarations/environment once, then starts one owned producer.
// It never polls independently of Source.Observe or creates a validator per update.
func Watch[T any](ctx context.Context, declaration Declaration[T], dependencies Dependencies, options WatchOptions) (*Watcher[T], error) {
	if ctx == nil {
		return nil, fail(ErrDeclaration, "watch")
	}
	capacity := options.QueueCapacity
	if capacity == 0 {
		capacity = 16
	}
	if capacity < 1 || capacity > 64 {
		return nil, fail(ErrDeclaration, "watch")
	}
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	selected, err := freeze(ctx, declaration, true)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancelCause(ctx)
	state := &watchState[T]{data: newState[T](), plan: selected, dependencies: dependencies, ctx: lifetime, cancel: cancel, capacity: capacity, eventsChanged: make(chan struct{}), inputChanged: make(chan struct{}, 1)}
	receipt, err := endpoint.Run(lifetime, request("watch"), func(call *adapters.Call[Evidence]) {
		guard, err := call.Hold()
		if err != nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: err})
			return
		}
		state.data.state.mu.Lock()
		state.work = call.Context()
		state.data.state.mu.Unlock()
		go func() {
			defer guard.Release()
			state.run(call)
		}()
	})
	if err != nil {
		cancel(nil)
		return nil, err
	}
	if state.work == nil {
		cancel(nil)
		value, _ := receipt.Snapshot()
		return nil, value.Err()
	}
	state.receipt = receipt
	return &Watcher[T]{state: state}, nil
}
func (state *watchState[T]) run(call *adapters.Call[Evidence]) {
	defer state.cancel(nil)
	data := state.data.state
	observer, openError := state.plan.source.Observe(call.Context())
	if nilInterface(observer) && openError == nil {
		openError = fail(ErrObservation, "observe")
	}
	if openError != nil {
		problem := fail(ErrSource, "observe", openError)
		if call.Context().Err() != nil {
			problem = nil
		}
		data.mu.Lock()
		data.status.SourceError = problem
		data.status.Observed = 1
		data.status.Rejected = 1
		state.eventLocked(Event{Sequence: 1, Err: problem})
		data.mu.Unlock()
		var cleanup error
		if !nilInterface(observer) {
			cleanup = observer.Close(context.Background())
		}
		state.finish(call, problem, cleanup)
		return
	}
	ingressDone := make(chan struct{})
	go func() {
		defer close(ingressDone)
		for call.Context().Err() == nil {
			observation, err := observer.Next(call.Context())
			if err != nil {
				if call.Context().Err() == nil {
					state.ingest(configsource.Observation{FailedIndex: -1, Err: err, Gap: true}, call.Context(), true)
				}
				break
			}
			state.ingest(observation, call.Context(), false)
		}
		data.mu.Lock()
		state.ingressStopped = true
		data.mu.Unlock()
		state.wake()
	}()
	for call.Context().Err() == nil {
		data.mu.Lock()
		input := state.input
		if input == nil {
			stopped := state.ingressStopped
			data.mu.Unlock()
			if stopped {
				break
			}
			select {
			case <-state.inputChanged:
			case <-call.Context().Done():
			}
			continue
		}
		state.input = nil
		validation, cancel := context.WithCancel(call.Context())
		state.validationCancel = cancel
		data.mu.Unlock()

		prepared, err := prepare(validation, state.plan, input.observation)
		var snapshot settings.Snapshot[T]
		if err == nil {
			snapshot, err = prepared.Snapshot()
		}
		cancel()
		data.mu.Lock()
		state.validationCancel = nil
		if data.status.Closed || call.Context().Err() != nil {
			data.mu.Unlock()
			break
		}
		if input.sequence != data.status.Observed {
			data.status.Superseded++
			data.status.Gap = true
			state.eventLocked(Event{Sequence: input.sequence, Superseded: true, Gap: true})
			data.mu.Unlock()
			continue
		}
		if err != nil {
			data.status.Rejected++
			data.status.PreparationError = err
			state.eventLocked(Event{Sequence: input.sequence, Err: err, Gap: input.observation.Gap})
			data.mu.Unlock()
			continue
		}
		err = data.publish(call.Context(), input.sequence, prepared, snapshot)
		if err != nil {
			data.status.Rejected++
			data.status.PreparationError = err
			state.eventLocked(Event{Sequence: input.sequence, Err: err})
			data.mu.Unlock()
			continue
		}
		data.mu.Unlock()
		state.data.adopt(call.Context(), state.dependencies.Resources)
		data.mu.Lock()
		state.eventLocked(Event{Sequence: input.sequence, Accepted: true, Gap: input.observation.Gap})
		data.mu.Unlock()
	}
	cleanup := observer.Close(context.Background())
	<-ingressDone
	data.mu.Lock()
	terminal := state.terminal
	data.mu.Unlock()
	state.finish(call, terminal, cleanup)
}
func (state *watchState[T]) wake() {
	select {
	case state.inputChanged <- struct{}{}:
	default:
	}
}
func (state *watchState[T]) ingest(observation configsource.Observation, ctx context.Context, terminal bool) {
	data := state.data.state
	data.mu.Lock()
	if data.status.Closed || ctx.Err() != nil {
		data.mu.Unlock()
		return
	}
	if data.status.Observed == math.MaxUint64 {
		state.terminal = fail(ErrLimit, "observation_sequence")
		data.mu.Unlock()
		state.cancel(state.terminal)
		return
	}
	data.status.Observed++
	if state.input != nil {
		data.status.Superseded++
		data.status.Gap = true
	}
	data.status.Gap = data.status.Gap || observation.Gap
	data.status.SourceError = nil
	if observation.Err != nil {
		data.status.SourceError = at(ErrSource, "observe", observation.FailedIndex, observation.Err)
	}
	if terminal {
		state.terminal = data.status.SourceError
	}
	state.input = &pending{observation: observation, sequence: data.status.Observed}
	cancel := state.validationCancel
	data.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	state.wake()
}
func (state *watchState[T]) eventLocked(event Event) {
	if len(state.events) == state.capacity {
		copy(state.events, state.events[1:])
		state.events[len(state.events)-1] = Event{}
		state.events = state.events[:len(state.events)-1]
		state.eventsGap = true
		state.data.state.status.Gap = true
	}
	event.Status = state.data.state.status
	state.events = append(state.events, event)
	close(state.eventsChanged)
	state.eventsChanged = make(chan struct{})
}
func (state *watchState[T]) finish(call *adapters.Call[Evidence], primary, cleanup error) {
	data := state.data.state
	data.mu.Lock()
	data.status.Closed = true
	evidence := data.status.Evidence
	state.input = nil
	clear(state.events)
	state.events = nil
	state.plan = plan[T]{}
	state.dependencies = Dependencies{}
	close(state.eventsChanged)
	data.mu.Unlock()
	_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true, Primary: primary, Cleanup: cleanup})
}
func (watch *Watcher[T]) Reader() settings.Reader {
	if watch == nil || watch.state == nil {
		return settings.Reader{}
	}
	return watch.state.data.Reader()
}
func (watch *Watcher[T]) Capture() (Accepted[T], error) {
	if watch == nil || watch.state == nil {
		return Accepted[T]{}, fail(ErrHandle, "capture")
	}
	return watch.state.data.Capture()
}
func (watch *Watcher[T]) Status() (Status, error) {
	if watch == nil || watch.state == nil {
		return Status{}, fail(ErrHandle, "status")
	}
	status, err := watch.state.data.Status()
	value, _ := watch.state.receipt.Snapshot()
	status.Closed = status.Closed || value.Info().Released
	if status.Observed == 0 && value.Info().Released {
		status.SourceError = value.Primary()
	}
	return status, err
}

// Next consumes bounded decision notifications. Cancellation ends only this wait.
// Closed producers have no new deliveries; Capture still exposes last-good data.
func (watch *Watcher[T]) Next(ctx context.Context) (Event, error) {
	if watch == nil || watch.state == nil || ctx == nil {
		return Event{}, fail(ErrHandle, "next")
	}
	state := watch.state
	data := state.data.state
	for {
		if ctx.Err() != nil {
			return Event{}, fail(ErrWait, "next", ctx.Err(), context.Cause(ctx))
		}
		receipt, _ := state.receipt.Snapshot()
		data.mu.Lock()
		if data.status.Closed || state.ctx.Err() != nil || state.work != nil && state.work.Err() != nil || receipt.Info().Released {
			sourceError := data.status.SourceError
			data.mu.Unlock()
			return Event{}, fail(ErrClosed, "next", receipt.Primary(), sourceError, state.work.Err(), context.Cause(state.work))
		}
		if len(state.events) > 0 {
			event := state.events[0]
			copy(state.events, state.events[1:])
			state.events[len(state.events)-1] = Event{}
			state.events = state.events[:len(state.events)-1]
			event.Gap = event.Gap || state.eventsGap
			state.eventsGap = false
			data.mu.Unlock()
			return event, nil
		}
		changed := state.eventsChanged
		data.mu.Unlock()
		select {
		case <-changed:
		case <-state.ctx.Done():
		case <-state.work.Done():
		case <-ctx.Done():
			return Event{}, fail(ErrWait, "next", ctx.Err(), context.Cause(ctx))
		}
	}
}

// Close fences publication before requesting cancellation, then joins the actual
// producer, source owner and validation. Expiration retains the same Watcher.
func (watch *Watcher[T]) Close(ctx context.Context) error {
	if watch == nil || watch.state == nil {
		return fail(ErrHandle, "close")
	}
	if ctx == nil {
		return fail(ErrDeclaration, "close")
	}
	data := watch.state.data.state
	data.mu.Lock()
	data.status.Closed = true
	data.mu.Unlock()
	watch.state.cancel(nil)
	value, err := watch.state.receipt.WaitReleased(ctx)
	if err != nil {
		return fail(ErrWait, "close", err)
	}
	return value.Err()
}
