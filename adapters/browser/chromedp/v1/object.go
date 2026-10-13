/*
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

package chromedp

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"github.com/chromedp/cdproto/runtime"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// ObjectGroup owns remote references, including unobserved or canceled results.
// Close uses its existing reservation even when ordinary work is saturated.
type ObjectGroup struct {
	private
	state *objectGroupState
}

type objectGroupState struct {
	native    *native.ObjectGroup
	page      *Page
	call      *adapters.Call[Result]
	guard     adapters.Guard
	gate      chan struct{}
	setupDone chan struct{}
	mu        sync.Mutex
	complete  bool
	closing   bool
	active    int
	idle      chan struct{}
	implicit  bool
	root      *native.Object
	payload   []byte
	primary   error
	final     error
	cleanup   helperCleanupEvidence
}

// Object is a revocable reference, not a native ID. Copies share custody.
// Closing the root of an implicit Evaluate/Poll group also releases descendants.
type Object struct {
	private
	native *native.Object
	group  *ObjectGroup
}

// ObjectObservation separates positive release from historical cleanup failure.
type ObjectObservation struct {
	GroupReleased bool
}

func isObjectResult[T any]() bool {
	return reflect.TypeFor[T]() == reflect.TypeFor[*runtime.RemoteObject]()
}

// NewObjectGroup retains a group before any native object-producing operation.
func (page *Page) NewObjectGroup(ctx context.Context) (*ObjectGroup, *adapters.Receipt[Result], error) {
	return newObjectGroup(ctx, page, "object_group", false, nil)
}

func newObjectGroup(ctx context.Context, page *Page, name string, implicit bool, run func(context.Context, *native.ObjectGroup) (*native.Object, error), metadata ...func() ([]byte, error)) (group *ObjectGroup, receipt *adapters.Receipt[Result], err error) {
	if ctx == nil || page == nil || page.context == nil {
		return nil, nil, fail(ErrInput, "object-group")
	}
	family := page.context
	var primary error
	receipt, err = family.endpoint.Child(ctx, family.call.Scope(), request(name, family.id, 0, family.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := page.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		value, err := page.native.NewObjectGroup(call.Context())
		if value == nil {
			primary = translate(err, "object-group")
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &objectGroupState{native: value, page: page, call: call, guard: guard, gate: make(chan struct{}, 1), setupDone: make(chan struct{}), implicit: implicit}
		defer close(state.setupDone)
		group = &ObjectGroup{state: state}
		family.mu.Lock()
		if family.objectGroups == nil {
			family.objectGroups = make(map[*objectGroupState]struct{})
		}
		family.objectGroups[state] = struct{}{}
		family.mu.Unlock()
		returned := false
		defer func() {
			if !returned {
				cause, _ := recover().(error)
				state.mu.Lock()
				state.primary = fail(ErrOperation, "object-did-not-return", cause)
				state.mu.Unlock()
				value.Cancel()
			}
		}()
		go func() { <-value.Stopping(); _ = group.Close(context.Background()) }()
		if run != nil {
			object, err := run(native.WithHelperCleanupObserver(call.Context(), state.cleanup.observe), value)
			state.cleanup.observe(native.HelperCleanup(err))
			primary = translate(err, name)
			var payload []byte
			if len(metadata) != 0 && err == nil {
				payload, err = metadata[0]()
				primary = errors.Join(primary, translate(err, "value-metadata"))
			}
			state.mu.Lock()
			state.root, state.primary = object, primary
			if len(metadata) != 0 {
				state.payload = payload
			} else if object != nil {
				state.payload = object.DataCopy()
			}
			state.mu.Unlock()
			if object == nil {
				value.Cancel()
			}
		}
		returned = true
	})
	return group, receipt, errors.Join(err, receiptError(receipt, primary))
}

func runRemoteAction[T any](ctx context.Context, page *Page, action Action[T]) (*Object, *adapters.Receipt[Result], error) {
	group, receipt, err := newObjectGroup(ctx, page, action.name, true, func(ctx context.Context, group *native.ObjectGroup) (*native.Object, error) {
		return native.RunObjectAction(ctx, page.native, group, action.native)
	})
	if group == nil {
		return nil, receipt, err
	}
	group.state.mu.Lock()
	root := group.state.root
	group.state.mu.Unlock()
	if root == nil {
		return nil, receipt, err
	}
	return &Object{native: root, group: group}, receipt, err
}

// EvaluateObject returns custody directly, including a non-nil cleanup handle
// when a canceled producer leaves its result unknown.
func EvaluateObject(ctx context.Context, page *Page, expression string, options ...EvaluateOption) (*Object, *adapters.Receipt[Result], error) {
	return runRemoteAction(ctx, page, Evaluate[*runtime.RemoteObject](expression, options...))
}

// Object derives a handle from the exact pointer returned by managed Evaluate
// or Poll. Copying metadata, changing its ObjectID or supplying a foreign page
// cannot construct a new authority.
func (page *Page) Object(value *runtime.RemoteObject) (*Object, error) {
	if page == nil || page.context == nil {
		return nil, fail(ErrInput, "object")
	}
	object, err := page.native.Object(value)
	if err != nil {
		return nil, translate(err, "object")
	}
	page.context.mu.Lock()
	defer page.context.mu.Unlock()
	for state := range page.context.objectGroups {
		if *state.native == *object.Group() {
			return &Object{native: object, group: &ObjectGroup{state: state}}, nil
		}
	}
	return nil, fail(ErrState, "object-custody")
}

func (group *ObjectGroup) Evaluate(ctx context.Context, expression string, options ...EvaluateOption) (*Object, *adapters.Receipt[Result], error) {
	return group.run(ctx, "object_evaluate", func(ctx context.Context) (*native.Object, error) {
		return native.RunObjectAction(ctx, group.state.page.native, group.state.native, native.Evaluate[*runtime.RemoteObject](expression, nativeEval(options)...))
	})
}

func (group *ObjectGroup) Poll(ctx context.Context, expression string, options ...PollOption) (*Object, *adapters.Receipt[Result], error) {
	return group.run(ctx, "object_poll", func(ctx context.Context) (*native.Object, error) {
		return native.RunObjectAction(ctx, group.state.page.native, group.state.native, native.Poll[*runtime.RemoteObject](expression, nativePoll(options)...))
	})
}

func (group *ObjectGroup) PollFunction(ctx context.Context, function string, options ...PollOption) (*Object, *adapters.Receipt[Result], error) {
	return group.run(ctx, "object_poll_function", func(ctx context.Context) (*native.Object, error) {
		return native.RunObjectAction(ctx, group.state.page.native, group.state.native, native.PollFunction[*runtime.RemoteObject](function, nativePoll(options)...))
	})
}

func (group *ObjectGroup) run(ctx context.Context, name string, run func(context.Context) (*native.Object, error)) (object *Object, receipt *adapters.Receipt[Result], err error) {
	return runGroupOperation(ctx, group, name, func(ctx context.Context) (*Object, []byte, error) {
		value, err := run(ctx)
		if value == nil {
			return nil, nil, err
		}
		return &Object{native: value, group: group}, value.DataCopy(), err
	})
}

func runGroupOperation[T any](ctx context.Context, group *ObjectGroup, name string, run func(context.Context) (T, []byte, error)) (value T, receipt *adapters.Receipt[Result], err error) {
	if ctx == nil || group == nil || group.state == nil {
		return value, nil, fail(ErrInput, "object-group")
	}
	state, family := group.state, group.state.page.context
	var primary error
	receipt, err = family.endpoint.Child(ctx, state.call.Scope(), request(name, family.id, 0, family.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := state.page.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		state.mu.Lock()
		if state.closing || state.complete {
			state.mu.Unlock()
			primary = fail(ErrState, "object-group")
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		if state.active == 0 {
			state.idle = make(chan struct{})
		}
		state.active++
		state.mu.Unlock()
		defer func() {
			state.mu.Lock()
			state.active--
			if state.active == 0 {
				close(state.idle)
			}
			state.mu.Unlock()
		}()
		resolved := false
		var cleanup helperCleanupEvidence
		defer func() {
			if !resolved {
				cause, _ := recover().(error)
				primary = fail(ErrOperation, "object-did-not-return", cause)
				_ = call.Resolve(adapters.Outcome[Result]{Primary: primary, Cleanup: cleanup.get()})
			}
		}()
		output, payload, err := run(native.WithHelperCleanupObserver(call.Context(), cleanup.observe))
		cleanup.observe(native.HelperCleanup(err))
		value = output
		primary = translate(err, name)
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: payload, attribution: attribution(info.Info()), observation: browser.Observation{Returned: err == nil, Effect: browser.Unknown}}, Primary: primary, Cleanup: cleanup.get()})
		resolved = true
	})
	return value, receipt, errors.Join(err, receiptError(receipt, primary))
}

func (object *Object) DataCopy() []byte {
	if object == nil || object.native == nil {
		return nil
	}
	return object.native.DataCopy()
}

// Call invokes the native function once, with JSON arguments and this object's
// immutable receiver identity. The result remains in the same owned group.
func (object *Object) Call(ctx context.Context, function string, arguments ...any) (*Object, *adapters.Receipt[Result], error) {
	if object == nil || object.native == nil || object.group == nil {
		return nil, nil, fail(ErrInput, "object")
	}
	return object.group.run(ctx, "object_call", func(ctx context.Context) (*native.Object, error) {
		return object.native.Call(ctx, function, arguments...)
	})
}

func (object *Object) Closed() bool {
	return object != nil && object.native != nil && object.native.Closed()
}

func (object *Object) Close(ctx context.Context) error {
	if ctx == nil || object == nil || object.native == nil || object.group == nil {
		return fail(ErrInput, "object-close")
	}
	state := object.group.state
	state.mu.Lock()
	root := state.implicit && state.root != nil && *state.root == *object.native
	state.mu.Unlock()
	if root {
		return object.group.Close(ctx)
	}
	return translate(object.native.Close(ctx), "object-close")
}

func (group *ObjectGroup) Closed() bool {
	if group == nil || group.state == nil {
		return false
	}
	group.state.mu.Lock()
	defer group.state.mu.Unlock()
	return group.state.complete
}

func (group *ObjectGroup) Close(ctx context.Context) error {
	if ctx == nil || group == nil || group.state == nil {
		return fail(ErrInput, "object-group-close")
	}
	state := group.state
	state.mu.Lock()
	state.closing = true
	active, idle := state.active, state.idle
	state.mu.Unlock()
	_ = state.call.Cancel(nil)
	state.native.Cancel()
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "object-group-close")
	}
	select {
	case <-state.setupDone:
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "object-setup")
	}
	state.mu.Lock()
	if state.complete {
		err := state.final
		state.mu.Unlock()
		return err
	}
	state.mu.Unlock()
	cleanup := translate(state.native.Close(ctx), "object-group-close")
	if !state.native.Closed() {
		return cleanup
	}
	if active != 0 {
		select {
		case <-idle:
		case <-ctx.Done():
			return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "object-work")
		}
	}
	state.mu.Lock()
	primary, payload := state.primary, state.payload
	state.mu.Unlock()
	primary = errors.Join(primary, translate(state.native.Err(), "object-group"))
	cleanup = errors.Join(state.cleanup.get(), cleanup)
	info, _ := state.call.Receipt().Snapshot()
	value := Result{payload: payload, attribution: attribution(info.Info()), observation: browser.Observation{Returned: primary == nil, Effect: browser.Unknown}, object: ObjectObservation{GroupReleased: true}}
	if err := state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: value, Primary: primary, Cleanup: cleanup}); err != nil {
		return err
	}
	state.mu.Lock()
	state.complete, state.final = true, errors.Join(primary, cleanup)
	state.mu.Unlock()
	state.page.context.mu.Lock()
	delete(state.page.context.objectGroups, state)
	state.page.context.mu.Unlock()
	_ = state.guard.Release()
	return state.final
}
