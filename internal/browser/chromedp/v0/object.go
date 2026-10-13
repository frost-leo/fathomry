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
	"crypto/rand"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// ObjectGroup owns remote references on one exact target session. A canceled
// producer with no terminal reply retains uncertain custody until that session
// is confirmed detached or its owned page/context/process is destroyed.
type ObjectGroup struct {
	private
	state *objectGroupState
}

type objectGroupState struct {
	session          *Session
	name             string
	gate             chan struct{}
	mu               sync.Mutex
	objects          map[*objectState]struct{}
	life             context.Context
	cancel           context.CancelCauseFunc
	stopCaller       func() bool
	sealed, closed   bool
	unknown, native  bool
	bytes            int64
	primary, cleanup error
}

// Object is an opaque source/page-bound reference. Copies share release and
// cannot change native identity by modifying the returned metadata.
type Object struct {
	private
	state *objectState
}

type objectState struct {
	group     *objectGroupState
	value     *runtime.RemoteObject
	data      []byte
	id        runtime.RemoteObjectID
	epoch     uint64
	grouped   bool
	closed    bool
	execution *ExecutionContext
	fence     *executionFence
}

type objectOrigin struct {
	epoch        atomic.Uint64
	retirement   atomic.Uint64
	done         chan struct{}
	nodesMu      sync.Mutex
	nodeRevision uint64
	nodeFrames   map[cdp.FrameID]*nodeFrame
}

type executionFence struct {
	origin   *objectOrigin
	revision uint64
}

func (fence *executionFence) valid() error {
	if fence != nil && fence.origin.retirement.Load() != fence.revision {
		return failure(ErrState, "object-execution-retired")
	}
	return nil
}

func remoteObjectType[T any]() bool {
	return reflect.TypeFor[T]() == reflect.TypeFor[*runtime.RemoteObject]()
}

func (session *Session) startObjectOrigin() error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.objectOrigin != nil {
		return nil
	}
	native := sdk.FromContext(session.native)
	if native == nil || native.Target == nil {
		return failure(ErrState, "object-target")
	}
	origin := &objectOrigin{done: make(chan struct{})}
	raw, unsubscribe := native.Target.SubscribeBounded(sdk.EventBuffer{
		MaxEvents: session.owner.settings.MaxEvents, MaxBytes: session.owner.settings.MaxEventBytes,
		OnOverflow: func(err error) { session.cancel(failure(ErrLimit, "object-origin", err)) },
		OnError:    func(err error) { session.cancel(failure(ErrNative, "object-origin", err)) },
		Observe: func(method string, data jsonv2.Value, _ uint64) error {
			if method == runtime.ExecutionContextsCleared.Method {
				origin.epoch.Add(1)
				origin.retirement.Add(1)
				origin.invalidateNodeFrame("", true)
				return nil
			}
			if method == runtime.ExecutionContextDestroyed.Method {
				var event runtime.EventExecutionContextDestroyed
				if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
					return err
				}
				if event.ExecutionContextUniqueID == "" {
					return failure(ErrNative, "object-execution-identity")
				}
				origin.retirement.Add(1)
				return nil
			}
			if method == page.FrameDetached.Method {
				var event page.EventFrameDetached
				if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
					return err
				}
				if event.FrameID == "" {
					return failure(ErrNative, "node-frame-identity")
				}
				origin.invalidateNodeFrame(event.FrameID, false)
				return nil
			}
			var event page.EventFrameNavigated
			if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
				return err
			}
			if event.Frame == nil || event.Frame.ID == "" {
				return failure(ErrNative, "object-frame")
			}
			if event.Frame.ParentID == "" {
				origin.epoch.Add(1)
			}
			origin.invalidateNodeFrame(event.Frame.ID, event.Frame.ParentID == "")
			return nil
		},
	}, page.FrameNavigated.Method, page.FrameDetached.Method, runtime.ExecutionContextsCleared.Method, runtime.ExecutionContextDestroyed.Method)
	session.objectOrigin = origin
	stop := context.AfterFunc(session.work, unsubscribe)
	go func() {
		defer close(origin.done)
		defer stop()
		defer unsubscribe()
		for {
			select {
			case <-session.work.Done():
				return
			case _, ok := <-raw:
				if !ok {
					return
				}
			}
		}
	}()
	return nil
}

// NewObjectGroup reserves custody before subscribing or issuing native work.
func (session *Session) NewObjectGroup(ctx context.Context) (*ObjectGroup, error) {
	_, leave, err := session.enter(ctx, false)
	if err != nil {
		return nil, err
	}
	defer leave()
	return session.newObjectGroup(ctx)
}

func (session *Session) newObjectGroup(ctx context.Context) (*ObjectGroup, error) {
	own, record := session.owner, session.browserContext
	own.mu.Lock()
	if len(record.objectGroups) >= own.settings.MaxObjectGroups {
		own.mu.Unlock()
		return nil, failure(ErrLimit, "object-groups")
	}
	life, cancel := context.WithCancelCause(session.work)
	state := &objectGroupState{session: session, name: "fathomry-" + rand.Text(), gate: make(chan struct{}, 1), objects: make(map[*objectState]struct{}), life: life, cancel: cancel}
	if record.objectGroups == nil {
		record.objectGroups = make(map[*objectGroupState]struct{})
	}
	record.objectGroups[state] = struct{}{}
	own.mu.Unlock()
	state.stopCaller = context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	if err := session.startObjectOrigin(); err != nil {
		state.stopCaller()
		cancel(err)
		own.mu.Lock()
		delete(record.objectGroups, state)
		own.mu.Unlock()
		return nil, err
	}
	return &ObjectGroup{state: state}, nil
}

// RunObjectAction keeps even an uncertain failed producer reachable through its
// group. The action must be an Evaluate/Poll remote-object action from this package.
func RunObjectAction[T any](ctx context.Context, session *Session, group *ObjectGroup, action Action[T]) (object *Object, err error) {
	defer func() { session.remember(err) }()
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return nil, err
	}
	defer leave()
	return runObjectAction(work, session, group, action)
}

func runObjectAction[T any](work context.Context, session *Session, group *ObjectGroup, action Action[T]) (*Object, error) {
	if action.object == nil {
		return nil, failure(ErrInput, "object-action")
	}
	if action.bytes > session.owner.settings.MaxCommandBytes {
		return nil, failure(ErrLimit, "object-input")
	}
	if group == nil {
		var err error
		group, err = session.newObjectGroup(session.work)
		if err != nil {
			return nil, err
		}
	}
	if group.state == nil || group.state.session.sessionState != session.sessionState {
		return nil, failure(ErrInput, "object-origin")
	}
	return group.executeQuery(work, action.query, action.object, action.objectGrouped)
}

func (group *ObjectGroup) executeQuery(ctx context.Context, options []QueryOption, build func(string, []sdk.QueryOption) sdk.Action[*runtime.RemoteObject], grouped bool) (*Object, error) {
	var check func() error
	return group.executeChecked(ctx, func(name string) sdk.Action[*runtime.RemoteObject] {
		var values []sdk.QueryOption
		if options != nil {
			var err error
			values, check, err = group.state.session.prepareQuery(options)
			if err != nil {
				return func(context.Context, *sdk.Target) (*runtime.RemoteObject, error) { return nil, err }
			}
		}
		return build(name, values)
	}, grouped, nil, func() error {
		if check != nil {
			return check()
		}
		return nil
	})
}

func (group *ObjectGroup) execute(ctx context.Context, build func(string) sdk.Action[*runtime.RemoteObject], grouped bool, receivers ...*objectState) (object *Object, err error) {
	return group.executeIn(ctx, build, grouped, nil, receivers...)
}

func (group *ObjectGroup) executeIn(ctx context.Context, build func(string) sdk.Action[*runtime.RemoteObject], grouped bool, execution *ExecutionContext, receivers ...*objectState) (object *Object, err error) {
	return group.executeChecked(ctx, build, grouped, execution, nil, receivers...)
}

func (group *ObjectGroup) executeChecked(ctx context.Context, build func(string) sdk.Action[*runtime.RemoteObject], grouped bool, execution *ExecutionContext, check func() error, receivers ...*objectState) (object *Object, err error) {
	return group.executeOrigin(ctx, build, grouped, execution, nil, false, check, receivers...)
}

func (group *ObjectGroup) executeOrigin(ctx context.Context, build func(string) sdk.Action[*runtime.RemoteObject], grouped bool, execution *ExecutionContext, fence *executionFence, noWait bool, check func() error, receivers ...*objectState) (object *Object, err error) {
	state := group.state
	if noWait {
		select {
		case state.gate <- struct{}{}:
		default:
			return nil, failure(ErrState, "object-group-busy")
		}
	} else {
		select {
		case state.gate <- struct{}{}:
		case <-ctx.Done():
			return nil, failure(ErrState, "object-wait", ctx.Err(), context.Cause(ctx))
		}
	}
	defer func() { <-state.gate }()
	state.mu.Lock()
	stopped := state.sealed || state.closed || state.life.Err() != nil
	if len(receivers) != 0 {
		receiver := receivers[0]
		stopped = stopped || receiver.group != state || receiver.closed || receiver.epoch != state.session.objectOrigin.epoch.Load()
	}
	state.mu.Unlock()
	if stopped {
		return nil, failure(ErrState, "object-group")
	}
	if len(receivers) != 0 {
		execution = receivers[0].execution
		fence = receivers[0].fence
	}
	if err := fence.valid(); err != nil {
		return nil, err
	}
	if execution != nil {
		if err := execution.valid(state.session); err != nil {
			return nil, err
		}
	}
	session, own := state.session, state.session.owner
	own.mu.Lock()
	if session.browserContext.objectCount >= own.settings.MaxObjects {
		own.mu.Unlock()
		return nil, failure(ErrLimit, "objects")
	}
	session.browserContext.objectCount++
	own.mu.Unlock()
	entry := &objectState{group: state, epoch: session.objectOrigin.epoch.Load(), grouped: grouped, execution: execution, fence: fence}
	state.mu.Lock()
	state.objects[entry] = struct{}{}
	state.mu.Unlock()
	object = &Object{state: entry}
	returned := false
	var prepaid atomic.Bool
	prepaid.Store(true)
	var queued, replies atomic.Int64
	defer func() {
		if !returned {
			err = failure(ErrPanic, "object-did-not-return")
		}
		state.mu.Lock()
		if queued.Load() != replies.Load() {
			state.unknown = true
		}
		if err != nil && state.primary == nil {
			state.primary = err
		}
		state.mu.Unlock()
		session.remember(err)
	}()
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(state.life, func() { cancel(context.Cause(state.life)) })
	defer stop()
	defer cancel(nil)
	work = sdk.WithCallLimits(nativeContext{Context: work, native: session.native}, sdk.CallLimits{
		MaxInputBytes: own.settings.MaxCommandBytes,
		Cleanup:       session.cleanupObserver(ctx),
		Before: func(string) error {
			if err := fence.valid(); err != nil {
				return err
			}
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			if execution != nil {
				if err := execution.valid(session); err != nil {
					return err
				}
			}
			if !prepaid.Swap(false) {
				if err := session.reserveCommand(); err != nil {
					return err
				}
			}
			state.mu.Lock()
			state.native = state.native || grouped
			state.mu.Unlock()
			return nil
		},
		Queued: func(string) { queued.Add(1) },
		Result: func(_ string, size int64) error {
			replies.Add(1)
			record := session.accounting()
			record.mu.Lock()
			defer record.mu.Unlock()
			if size > own.settings.MaxResultBytes-record.delivered {
				return failure(ErrLimit, "object-output")
			}
			record.delivered += size
			return nil
		},
	})
	var value *runtime.RemoteObject
	err = invoke(func() error {
		if err := session.reserveCommand(); err != nil {
			return err
		}
		var callErr error
		value, callErr = sdk.Run(work, build(state.name))
		return callErr
	})
	if !grouped && replies.Load() != 0 && err != nil && !errors.Is(err, sdk.ErrPollingTimeout) {
		state.mu.Lock()
		state.unknown = true
		state.mu.Unlock()
	}
	if value != nil {
		buffer := boundedBuffer{limit: own.settings.MaxResultBytes}
		if copyErr := WriteNativeResult(&buffer, value); copyErr != nil {
			err = errors.Join(err, copyErr)
		} else {
			state.mu.Lock()
			if int64(len(buffer.data)) > own.settings.MaxResultBytes-state.bytes {
				err = errors.Join(err, failure(ErrLimit, "object-metadata"))
			} else {
				state.bytes += int64(len(buffer.data))
				entry.data = buffer.data
			}
			state.mu.Unlock()
		}
		entry.value, entry.id = value, value.ObjectID
		own.mu.Lock()
		if session.browserContext.objectRefs == nil {
			session.browserContext.objectRefs = make(map[*runtime.RemoteObject]*objectState)
		}
		session.browserContext.objectRefs[value] = entry
		own.mu.Unlock()
	}
	err = errors.Join(err, fence.valid(), ctx.Err(), context.Cause(ctx), state.life.Err(), context.Cause(state.life))
	if errors.Is(err, sdk.ErrCommandLimit) {
		err = failure(ErrLimit, "object-input", err)
	}
	if err != nil {
		err = failure(ErrNative, "object", err)
	}
	returned = true
	return object, err
}

// Object derives a capability only from the exact returned native pointer. A
// copied struct, fabricated ID, foreign page or changed ID is not a grant.
func (session *Session) Object(value *runtime.RemoteObject) (*Object, error) {
	if session == nil || session.sessionState == nil || session.owner == nil || value == nil {
		return nil, failure(ErrInput, "object")
	}
	session.owner.mu.Lock()
	entry := session.browserContext.objectRefs[value]
	session.owner.mu.Unlock()
	if entry == nil || entry.group.session.sessionState != session.sessionState || value.ObjectID != entry.id {
		return nil, failure(ErrInput, "object-origin")
	}
	object := &Object{state: entry}
	if err := object.valid(); err != nil {
		return nil, err
	}
	return object, nil
}

func (object *Object) valid() error {
	if object == nil || object.state == nil {
		return failure(ErrInput, "object")
	}
	entry, group := object.state, object.state.group
	group.mu.Lock()
	defer group.mu.Unlock()
	if entry.closed || group.closed || group.sealed || group.life.Err() != nil || entry.epoch != group.session.objectOrigin.epoch.Load() {
		return failure(ErrState, "object-stale")
	}
	if err := entry.fence.valid(); err != nil {
		return err
	}
	if entry.execution != nil {
		return entry.execution.valid(group.session)
	}
	return nil
}

// NativeValue returns the original result identity. Its fields are caller data,
// never the identity used by cleanup or an operation on this Object.
func (object *Object) NativeValue() *runtime.RemoteObject {
	if object == nil || object.state == nil {
		return nil
	}
	return object.state.value
}

// DataCopy returns immutable native metadata, including null/undefined type.
func (object *Object) DataCopy() []byte {
	if object == nil || object.state == nil {
		return nil
	}
	return append([]byte(nil), object.state.data...)
}

func (object *Object) Group() *ObjectGroup {
	if object == nil || object.state == nil {
		return nil
	}
	return &ObjectGroup{state: object.state.group}
}

// Call invokes a native function on this object. Arguments are JSON values, not
// raw remote-object IDs. The returned object belongs to this same group.
func (object *Object) Call(ctx context.Context, function string, arguments ...any) (result *Object, err error) {
	return CallObjectArguments(ctx, object, function, len(arguments), func(index int) Argument {
		return ValueArgument(arguments[index])
	})
}

func (object *Object) Closed() bool {
	if object == nil || object.state == nil {
		return false
	}
	object.state.group.mu.Lock()
	defer object.state.group.mu.Unlock()
	return object.state.closed || object.state.group.closed
}

func (object *Object) Close(ctx context.Context) error {
	if object == nil || object.state == nil || ctx == nil {
		return failure(ErrInput, "object-close")
	}
	entry, group := object.state, object.state.group
	work, done, err := (invocation.Budget{Limit: group.session.owner.settings.CleanupTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return err
	}
	defer done()
	select {
	case group.gate <- struct{}{}:
		defer func() { <-group.gate }()
	case <-work.Done():
		return failure(ErrCleanup, "object-join", work.Err())
	}
	group.mu.Lock()
	closed, uncertain := entry.closed || group.closed, group.unknown
	group.mu.Unlock()
	if closed {
		return nil
	}
	if uncertain && !group.resourcesGone() {
		return failure(ErrCleanup, "object-producer-unknown")
	}
	if entry.id != "" && !group.resourcesGone() {
		if err := entry.fence.valid(); err != nil {
			return failure(ErrCleanup, "object-group-release-required")
		}
		_, err = cdp.Call(sdk.WithCallLimits(work, sdk.CallLimits{MaxInputBytes: 4096, MaxOutputBytes: 4096}), sdk.FromContext(group.session.native).Target, runtime.ReleaseObject, runtime.ReleaseObjectParams{ObjectID: entry.id})
		if err != nil {
			return group.rememberCleanup(err)
		}
	}
	group.forget(entry)
	return nil
}

func (state *objectGroupState) resourcesGone() bool {
	if state.session.nativeResourcesGone() {
		return true
	}
	state.session.mu.Lock()
	defer state.session.mu.Unlock()
	return state.session.objectSessionDetached
}

// Only joined local work on an owned native resource can transfer to the
// existing source cleanup record. A borrowed target still needs confirmed detach.
func (state *objectGroupState) transferToSource() bool {
	if state.session.borrowedTarget {
		return false
	}
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	default:
		return false
	}
	state.mu.Lock()
	stopped := state.sealed && state.life.Err() != nil
	state.mu.Unlock()
	if !stopped {
		return false
	}
	own := state.session.owner
	own.mu.Lock()
	if _, pending := own.sessions[state.session.browserContext]; !pending {
		own.mu.Unlock()
		return false
	}
	if own.pendingObjects == nil {
		own.pendingObjects = make(map[*objectGroupState]struct{})
	}
	own.pendingObjects[state] = struct{}{}
	own.mu.Unlock()
	return true
}

func (state *objectGroupState) forget(entry *objectState) {
	state.mu.Lock()
	if entry.closed {
		state.mu.Unlock()
		return
	}
	entry.closed = true
	delete(state.objects, entry)
	state.bytes -= int64(len(entry.data))
	state.mu.Unlock()
	own := state.session.owner
	own.mu.Lock()
	delete(state.session.browserContext.objectRefs, entry.value)
	state.session.browserContext.objectCount--
	own.mu.Unlock()
}

func (state *objectGroupState) rememberCleanup(err error) error {
	err = failure(ErrCleanup, "object-release", err)
	state.mu.Lock()
	if state.cleanup == nil {
		state.cleanup = err
	}
	history := state.cleanup
	state.mu.Unlock()
	state.session.rememberCleanup(err)
	return errors.Join(history, err)
}

func (group *ObjectGroup) Cancel() {
	if group != nil && group.state != nil {
		group.state.mu.Lock()
		group.state.sealed = true
		group.state.mu.Unlock()
		group.state.cancel(context.Canceled)
	}
}

func (group *ObjectGroup) Stopping() <-chan struct{} {
	if group == nil || group.state == nil {
		return nil
	}
	return group.state.life.Done()
}

func (group *ObjectGroup) Closed() bool {
	if group == nil || group.state == nil {
		return false
	}
	group.state.mu.Lock()
	defer group.state.mu.Unlock()
	return group.state.closed
}

func (group *ObjectGroup) Err() error {
	if group == nil || group.state == nil {
		return failure(ErrInput, "object-group")
	}
	group.state.mu.Lock()
	defer group.state.mu.Unlock()
	return group.state.primary
}

func (group *ObjectGroup) Close(ctx context.Context) error {
	if group == nil || group.state == nil || ctx == nil {
		return failure(ErrInput, "object-group-close")
	}
	group.Cancel()
	state := group.state
	work, done, err := (invocation.Budget{Limit: state.session.owner.settings.CleanupTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return err
	}
	defer done()
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-work.Done():
		return failure(ErrCleanup, "object-group-join", work.Err())
	}
	state.mu.Lock()
	closed, history, unknown, native := state.closed, state.cleanup, state.unknown, state.native
	objects := make([]*objectState, 0, len(state.objects))
	for entry := range state.objects {
		objects = append(objects, entry)
	}
	state.mu.Unlock()
	if closed {
		return history
	}
	gone := state.resourcesGone()
	if unknown && !gone {
		return state.rememberCleanup(failure(ErrCleanup, "object-producer-unknown"))
	}
	if !gone {
		work = sdk.WithCallLimits(work, sdk.CallLimits{MaxInputBytes: 4096, MaxOutputBytes: 4096})
		target := sdk.FromContext(state.session.native).Target
		if native {
			if _, err := cdp.Call(work, target, runtime.ReleaseObjectGroup, runtime.ReleaseObjectGroupParams{ObjectGroup: state.name}); err != nil {
				return state.rememberCleanup(err)
			}
		}
		for _, entry := range objects {
			if !entry.grouped && entry.id != "" {
				if _, err := cdp.Call(work, target, runtime.ReleaseObject, runtime.ReleaseObjectParams{ObjectID: entry.id}); err != nil {
					return state.rememberCleanup(err)
				}
			}
		}
	}
	for _, entry := range objects {
		state.forget(entry)
	}
	state.stopCaller()
	state.mu.Lock()
	state.closed = true
	state.mu.Unlock()
	state.session.owner.mu.Lock()
	delete(state.session.browserContext.objectGroups, state)
	state.session.owner.mu.Unlock()
	return history
}
