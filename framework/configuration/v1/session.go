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
	"slices"
	"sync"
	"time"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// State describes producer lifetime, separately from accepted data availability.
type State string

const (
	Initializing State = "initializing"
	Ready        State = "ready"
	Closing      State = "closing"
	Closed       State = "closed"
)

// Stage identifies the bounded phase of one report.
type Stage string

const (
	Acquisition Stage = "acquisition"
	Preparation Stage = "preparation"
	Publication Stage = "publication"
	Cleanup     Stage = "cleanup"
)

// Update owns detached report fields. View is valid only when Published.
// Err retains borrowed immutable cause objects; inspecting them is sensitive.
// Sequence counts consumed source observations, not all reports or native attempts.
type Update struct {
	Sequence    uint64
	Publication uint64
	Stage       Stage
	Published   bool
	View        settings.View
	Gap         bool
	Skipped     uint64
	Err         error
}

// Status is a detached current projection, not a resource-readiness certificate.
type Status struct {
	State    State
	Watching bool
	Accepted bool
	Complete bool
	Latest   Update
}

// Session owns the main-configuration producer returned by LoadConfiguration.
// Even a failed load may return a live Session that must be closed. Do not copy
// or overwrite a live handle. Accepted data may outlive Close; no writable Store
// or application-service lifecycle is exposed.
type Session[T any] struct{ state *sessionState[T] }

type sessionState[T any] struct {
	mu              sync.Mutex
	ctx             context.Context
	cancel          context.CancelCauseFunc
	plan            SourcePlan
	schema          configsource.Schema[T]
	variables       configsource.Layer
	haveVariables   bool
	options         sessionOptions
	dependencies    Dependencies
	store           settings.Store[T]
	phase           State
	stage           Stage
	initialDone     chan struct{}
	initialSet      bool
	initialErr      error
	initialView     settings.View
	workDone        chan struct{} // Acquisition and validation have stopped; cleanup may remain.
	workerDone      chan struct{} // The worker's owned cleanup attempt has returned.
	guardDone       chan struct{}
	initialDeadline time.Time
	cleanupGate     chan struct{}
	changed         chan struct{}
	pending         *Update
	latest          Update
	waiter          bool
	sequence        uint64
	publication     uint64
	terminal        error
	cleanupErr      error
	cleanupLast     error
	observer        configsource.Observer
	observerClosed  bool
	owner           *nacos.Owner
}

func (state *sessionState[T]) guard(timer *time.Timer) {
	defer close(state.guardDone)
	defer timer.Stop()
	select {
	case <-state.ctx.Done():
		state.stop(at(ErrClosed, "lifetime", -1, state.ctx.Err(), context.Cause(state.ctx)))
		return
	case <-timer.C:
		state.mu.Lock()
		if !state.initialSet {
			problem := at(ErrWait, "initialize", -1, context.DeadlineExceeded)
			state.stopLocked(problem)
			state.mu.Unlock()
			state.cancel(problem)
		} else {
			state.mu.Unlock()
		}
	case <-state.initialDone:
	}
	timer.Stop()
	<-state.ctx.Done()
	state.stop(at(ErrClosed, "lifetime", -1, state.ctx.Err(), context.Cause(state.ctx)))
}

func (state *sessionState[T]) stopLocked(problem error) {
	if state.phase == Closing || state.phase == Closed {
		return
	}
	state.phase = Closing
	state.terminal = problem
	if !state.initialSet {
		state.initialSet = true
		state.initialErr = problem
		if state.initialErr == nil {
			state.initialErr = at(ErrClosed, "initialize", -1)
		}
		close(state.initialDone)
	}
	if problem != nil {
		state.reportLocked(Update{Sequence: state.sequence, Publication: state.publication, Stage: state.stage, Err: problem})
	}
	state.signalLocked()
}
func (state *sessionState[T]) stop(problem error) {
	state.mu.Lock()
	state.stopLocked(problem)
	reason := state.terminal
	state.mu.Unlock()
	state.cancel(reason)
}
func (state *sessionState[T]) signalLocked() {
	close(state.changed)
	state.changed = make(chan struct{})
}
func (state *sessionState[T]) active() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.phase != Closing && state.phase != Closed && state.ctx.Err() == nil
}

func (state *sessionState[T]) run() {
	defer close(state.workerDone)
	if state.active() {
		source, owner, err := openSource(state.ctx, state.plan, state.dependencies)
		state.owner = owner
		if err != nil {
			problem := at(ErrSource, "open", -1, err)
			state.stop(problem)
		} else if state.active() {
			state.consume(source)
		}
	}
	if state.ctx.Err() != nil {
		state.stop(at(ErrClosed, "lifetime", -1, state.ctx.Err(), context.Cause(state.ctx)))
	}
	state.stop(nil)
	close(state.workDone)
	_ = state.cleanup(context.Background())
}

func (state *sessionState[T]) consume(source configsource.Source) {
	if !state.plan.Watch {
		batch, index, problem := source.Capture(state.ctx)
		if state.process(configsource.Observation{Batch: batch, FailedIndex: index, Err: problem}) {
			<-state.ctx.Done()
		}
		return
	}
	observer, err := source.Observe(state.ctx)
	if observer != nil {
		state.observer = observer
	}
	if err != nil || observer == nil {
		problem := at(ErrSource, "observe", -1, err)
		state.stop(problem)
		return
	}
	for state.active() {
		state.mu.Lock()
		state.stage = Acquisition
		state.mu.Unlock()
		observation, err := observer.Next(state.ctx)
		if err != nil {
			if state.ctx.Err() == nil {
				problem := at(ErrSource, "observe", -1, err)
				state.stop(problem)
			}
			return
		}
		if !state.process(observation) {
			return
		}
	}
}

func (state *sessionState[T]) process(observation configsource.Observation) bool {
	state.mu.Lock()
	if state.phase == Closing || state.phase == Closed || state.ctx.Err() != nil {
		state.mu.Unlock()
		return false
	}
	if state.sequence == math.MaxUint64 || state.pending != nil && state.pending.Skipped == math.MaxUint64 {
		state.mu.Unlock()
		state.stop(at(ErrLimit, "sequence", -1))
		return false
	}
	state.sequence++
	initial := state.publication == 0
	state.mu.Unlock()
	layers, err := originalLayers(observation.Batch, observation.FailedIndex, observation.Err, state.plan.Documents, "application.acquire")
	if err != nil {
		return state.reject(Acquisition, observation.Gap, err)
	}
	if state.haveVariables {
		layers = append(layers, state.variables)
	}
	state.mu.Lock()
	state.stage = Preparation
	state.mu.Unlock()
	deadline := time.Now().Add(state.options.CandidateTimeout)
	if initial && state.initialDeadline.Before(deadline) {
		deadline = state.initialDeadline
	}
	ctx, cancel := context.WithDeadline(state.ctx, deadline)
	defer cancel()
	prepared, err := configsource.Prepare(ctx, state.schema, layers)
	if err != nil {
		return state.reject(Preparation, observation.Gap, err)
	}
	snapshot, err := prepared.Snapshot()
	if err != nil {
		return state.reject(Preparation, observation.Gap, err)
	}
	state.mu.Lock()
	if state.phase == Closing || state.phase == Closed || state.ctx.Err() != nil {
		state.mu.Unlock()
		return false
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		state.mu.Unlock()
		return state.reject(Preparation, observation.Gap, at(ErrWait, "candidate", -1, context.DeadlineExceeded, ctx.Err(), context.Cause(ctx)))
	}
	if state.publication == math.MaxUint64 {
		state.mu.Unlock()
		state.stop(at(ErrLimit, "publication", -1))
		return false
	}
	state.stage = Publication
	if err := state.store.Publish(snapshot); err != nil {
		state.mu.Unlock()
		return state.reject(Publication, observation.Gap, at(ErrPublish, "publish", -1, err))
	}
	state.publication++
	state.phase = Ready
	if !state.initialSet {
		if state.options.accepted != nil {
			value := Loaded{state: &loaded{view: snapshot.View(), reader: state.store.Reader(), info: Info{
				Environment: state.plan.Environment, Mode: state.plan.Mode, Provider: state.plan.Provider,
				Watching: state.plan.Watch, SchemaVersion: state.schema.Version, Documents: slices.Clone(state.plan.Documents),
			}}}
			accepted, err := settings.New(value, func(value Loaded) Loaded { return value })
			if err == nil {
				err = state.options.accepted.Publish(accepted)
			}
			if err != nil {
				state.mu.Unlock()
				state.stop(at(ErrPublish, "publish_result", -1, err))
				return false
			}
		}
		state.initialSet = true
		state.initialView = snapshot.View()
		close(state.initialDone)
	}
	state.reportLocked(Update{Sequence: state.sequence, Publication: state.publication, Stage: Publication,
		Published: true, View: snapshot.View(), Gap: observation.Gap})
	state.mu.Unlock()
	return true
}

func (state *sessionState[T]) reject(stage Stage, gap bool, problem error) bool {
	state.mu.Lock()
	if state.phase == Closing || state.phase == Closed {
		state.mu.Unlock()
		return false
	}
	state.reportLocked(Update{Sequence: state.sequence, Publication: state.publication, Stage: stage, Gap: gap, Err: problem})
	initial := state.publication == 0
	if initial {
		state.stopLocked(problem)
	}
	state.mu.Unlock()
	if initial {
		state.cancel(problem)
	}
	return !initial
}

func (state *sessionState[T]) reportLocked(update Update) {
	if state.pending != nil {
		update.Gap = update.Gap || state.pending.Gap
		if state.pending.Skipped == math.MaxUint64 {
			update.Skipped = math.MaxUint64
			update.Err = at(ErrLimit, "reports", -1)
			state.stopLocked(update.Err)
		} else {
			update.Skipped = state.pending.Skipped + 1
		}
	}
	state.latest = update
	state.pending = &update
	state.signalLocked()
}

// Reader returns accepted-data access only; a nil session yields a zero Reader.
// The Reader remains usable after producer shutdown if a value was accepted.
func (session *Session[T]) Reader() settings.Reader {
	if session == nil || session.state == nil {
		return settings.Reader{}
	}
	return session.state.store.Reader()
}

func (session *Session[T]) waitInitial(ctx context.Context) error {
	if session == nil || session.state == nil || ctx == nil {
		return at(ErrHandle, "wait_initial", -1)
	}
	select {
	case <-ctx.Done():
		return at(ErrWait, "wait_initial", -1, ctx.Err(), context.Cause(ctx))
	case <-session.state.initialDone:
		session.state.mu.Lock()
		defer session.state.mu.Unlock()
		return session.state.initialErr
	}
}

// InitialView returns the first accepted immutable View without waiting or
// selecting a later Watch value. Successful LoadConfiguration guarantees its
// availability. Failed loads preserve their initial error; a not-yet-settled
// producer returns ErrWait and no usable View. Data remains valid after Close.
func (session *Session[T]) InitialView() (settings.View, error) {
	if session == nil || session.state == nil {
		return settings.View{}, at(ErrHandle, "initial_view", -1)
	}
	session.state.mu.Lock()
	defer session.state.mu.Unlock()
	if !session.state.initialSet {
		return settings.View{}, at(ErrWait, "initial_view", -1)
	}
	if session.state.initialErr != nil {
		return settings.View{}, session.state.initialErr
	}
	return session.state.initialView, nil
}

// Status copies bounded local facts without I/O. Errors and Views retain their
// original ownership contracts; explicit cause/value inspection is sensitive.
func (session *Session[T]) Status() (Status, error) {
	if session == nil || session.state == nil {
		return Status{}, at(ErrHandle, "status", -1)
	}
	state := session.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return Status{State: state.phase, Watching: state.plan.Watch,
		Accepted: state.publication != 0, Complete: state.completeLocked(), Latest: state.latest}, nil
}

// Next consumes one latest report. One waiter is admitted; overlap returns
// ErrLimit. Update.Err describes a candidate/cleanup failure, while the returned
// error describes this wait. After the final report, errors.Is(err, io.EOF) holds.
func (session *Session[T]) Next(ctx context.Context) (Update, error) {
	if session == nil || session.state == nil || ctx == nil {
		return Update{}, at(ErrHandle, "next", -1)
	}
	state := session.state
	state.mu.Lock()
	if state.waiter {
		state.mu.Unlock()
		return Update{}, at(ErrLimit, "next", -1)
	}
	state.waiter = true
	defer func() { state.mu.Lock(); state.waiter = false; state.mu.Unlock() }()
	for {
		if ctx.Err() != nil {
			state.mu.Unlock()
			return Update{}, at(ErrWait, "next", -1, ctx.Err(), context.Cause(ctx))
		}
		if state.pending != nil {
			result := *state.pending
			state.pending = nil
			state.mu.Unlock()
			return result, nil
		}
		if state.phase == Closed {
			state.mu.Unlock()
			return Update{}, at(ErrClosed, "next", -1, io.EOF)
		}
		changed := state.changed
		state.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		state.mu.Lock()
	}
}

// Close fences publication, requests stop and joins owned work. A timeout does
// not discard the owner: call Close again. Bootstrap's shutdown budget caps each
// call in addition to its context. Borrowed Runtime/Inboxes remain untouched.
func (session *Session[T]) Close(ctx context.Context) error {
	if session == nil || session.state == nil || ctx == nil {
		return at(ErrHandle, "close", -1)
	}
	state := session.state
	if state.options.ShutdownTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, state.options.ShutdownTimeout)
		defer cancel()
	}
	state.stop(nil)
	result := state.cleanup(ctx)
	select {
	case <-state.workerDone:
		return result
	case <-ctx.Done():
		return errors.Join(result, at(ErrWait, "close", -1, ctx.Err(), context.Cause(ctx)))
	}
}

// ShutdownComplete confirms owned work/source cleanup, independently of errors.
func (session *Session[T]) ShutdownComplete() bool {
	if session == nil || session.state == nil {
		return false
	}
	session.state.mu.Lock()
	defer session.state.mu.Unlock()
	return session.state.completeLocked()
}

func (state *sessionState[T]) completeLocked() bool {
	if state.phase != Closed {
		return false
	}
	select {
	case <-state.workerDone:
		return true
	default:
		return false
	}
}

func (state *sessionState[T]) cleanup(ctx context.Context) error {
	if ctx.Err() != nil {
		return at(ErrWait, "close", -1, ctx.Err(), context.Cause(ctx))
	}
	select {
	case <-ctx.Done():
		return at(ErrWait, "close", -1, ctx.Err(), context.Cause(ctx))
	case <-state.workDone:
	}
	select {
	case <-ctx.Done():
		return at(ErrWait, "close", -1, ctx.Err(), context.Cause(ctx))
	case <-state.cleanupGate:
	}
	defer func() { state.cleanupGate <- struct{}{} }()
	state.mu.Lock()
	if state.phase == Closed {
		result := errors.Join(state.terminal, state.cleanupErr, state.cleanupLast)
		state.mu.Unlock()
		return result
	}
	state.mu.Unlock()
	complete := true
	var current error
	if state.observer != nil && !state.observerClosed {
		err := state.observer.Close(ctx)
		if err != nil {
			current = at(ErrCleanup, "observer_close", -1, err)
		}
		// Selected provider observers join their receipt before returning a cleanup
		// error on a live wait context. A cancelled wait remains conservatively pending.
		state.observerClosed = ctx.Err() == nil
		complete = state.observerClosed
	}
	if complete && state.owner != nil {
		err := state.owner.Close(ctx)
		complete = state.owner.ShutdownComplete()
		if err != nil {
			current = errors.Join(current, at(ErrCleanup, "source_close", -1, err))
		}
	}
	if complete {
		select {
		case <-state.guardDone:
		case <-ctx.Done():
			complete = false
		}
	}
	if !complete && current == nil {
		current = at(ErrWait, "close", -1, ctx.Err(), context.Cause(ctx))
	}
	state.mu.Lock()
	if current != nil {
		if state.cleanupErr == nil {
			state.cleanupErr = current
		}
		state.cleanupLast = current
	}
	result := errors.Join(state.terminal, state.cleanupErr, state.cleanupLast)
	state.reportLocked(Update{Sequence: state.sequence, Publication: state.publication, Stage: Cleanup, Err: result})
	if complete {
		state.phase = Closed
		state.signalLocked()
	}
	state.mu.Unlock()
	return result
}
