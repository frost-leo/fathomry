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

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// Action is a closed native delegation. Its zero value is invalid.
type Action[T any] struct {
	private
	native        sdk.Action[T]
	local         func(context.Context, *Session) (T, error)
	bytes         int64
	navigation    bool
	object        func(string, []sdk.QueryOption) sdk.Action[*runtime.RemoteObject]
	objectGrouped bool
	managed       func(string, []sdk.QueryOption) sdk.Action[T]
	query         []QueryOption
}

// NeedsCustody reports whether a value-returning native operation can still
// retain remote exception objects or produce an unknown result after cancellation.
func (action Action[T]) NeedsCustody() bool { return action.managed != nil }

// RunAction admits one native high-level operation without exposing its Target.
func RunAction[T any](ctx context.Context, session *Session, action Action[T]) (value T, err error) {
	if action.managed != nil {
		return runManagedValue(ctx, session, action)
	}
	if action.object != nil {
		object, err := RunObjectAction(ctx, session, nil, action)
		if object != nil && object.NativeValue() != nil {
			value = any(object.NativeValue()).(T)
		}
		return value, err
	}
	work, leave, err := session.enter(ctx, action.navigation)
	if err != nil {
		session.remember(err)
		return value, err
	}
	defer leave()
	returned := false
	defer func() {
		if !returned {
			session.remember(failure(ErrPanic, "action-did-not-return"))
		}
	}()
	if action.native == nil && action.local == nil {
		returned = true
		err = failure(ErrInput, "action")
		session.remember(err)
		return value, err
	}
	if action.bytes > session.owner.settings.MaxCommandBytes {
		returned = true
		err = failure(ErrLimit, "action-input")
		session.remember(err)
		return value, err
	}
	err = invoke(func() error {
		var callErr error
		value, callErr = action.run(work, session)
		if callErr == nil && action.navigation {
			record := session.accounting()
			record.mu.Lock()
			record.data.navigations++
			record.mu.Unlock()
		}
		return callErr
	})
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), work.Err(), context.Cause(work))
	if err != nil {
		if errors.Is(err, sdk.ErrCommandLimit) {
			err = failure(ErrLimit, "action-bytes", err)
		}
		err = failure(ErrNative, "action", err)
	}
	session.remember(err)
	returned = true
	return value, err
}

// Actions runs closed void actions in order. No raw native Target enters a callback.
func (session *Session) Actions(ctx context.Context, actions ...Operation) error {
	if len(actions) == 0 {
		err := failure(ErrInput, "actions")
		session.remember(err)
		return err
	}
	if session != nil && session.sessionState != nil && session.owner != nil && len(actions) > session.owner.settings.MaxCommands {
		err := failure(ErrInput, "action-count")
		session.remember(err)
		return err
	}
	for _, action := range actions {
		if action == nil || nilLike(action) {
			err := failure(ErrInput, "nil-action")
			session.remember(err)
			return err
		}
		if err := action.apply(ctx, session); err != nil {
			return err
		}
	}
	return nil
}

// Operation is implemented only by closed actions in this package.
type Operation interface {
	apply(context.Context, *Session) error
}

func (action Action[T]) apply(ctx context.Context, session *Session) error {
	_, err := RunAction(ctx, session, action)
	return err
}

// Into stores the returned value during admitted execution.
// The output pointer must not be used concurrently.
func Into[T any](action Action[T], output *T) Action[sdk.Void] {
	if output == nil || action.native == nil && action.local == nil && action.object == nil && action.managed == nil {
		return Action[sdk.Void]{}
	}
	return Action[sdk.Void]{bytes: action.bytes, navigation: action.navigation, local: func(ctx context.Context, session *Session) (sdk.Void, error) {
		value, err := action.run(ctx, session)
		if err == nil {
			*output = value
		}
		return sdk.Void{}, err
	}}
}

// Step runs cooperative caller code with cancellation only, never a native context.
func Step(run func(context.Context) error) Action[sdk.Void] {
	if run == nil {
		return Action[sdk.Void]{}
	}
	return Action[sdk.Void]{local: func(ctx context.Context, _ *Session) (sdk.Void, error) { return sdk.Void{}, run(bareContext{ctx}) }}
}

func (action Action[T]) run(ctx context.Context, session *Session) (T, error) {
	if action.managed != nil {
		return runManagedValueAdmitted(ctx, session, action)
	}
	if action.object != nil {
		var value T
		object, err := runObjectAction(ctx, session, nil, action)
		if object != nil && object.NativeValue() != nil {
			value = any(object.NativeValue()).(T)
		}
		return value, err
	}
	if action.local != nil {
		return action.local(ctx, session)
	}
	return sdk.Run(session.nativeWork(ctx), action.native)
}

// Command delegates a finite page-local CDP command through a revocable session.
// Parameters remain borrowed until execution finishes.
// Identity-bearing commands and resource producers need their scoped APIs.
func Command[P, R any](command cdp.Command[P, R], params P) Action[R] {
	return Action[R]{local: func(ctx context.Context, session *Session) (R, error) {
		grant := &executorGrant{session: session, work: ctx}
		defer grant.closeAndJoin()
		return cdp.Call(bareContext{ctx}, grant, command, params)
	}}
}

// Protocol grants synchronous access to finite page-local CDP commands.
// It does not grant authority from native IDs or the wider protocol catalogue.
func (session *Session) Protocol(ctx context.Context, run func(context.Context, cdp.Session) error) error {
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		session.remember(err)
		return err
	}
	defer leave()
	if run == nil {
		err := failure(ErrInput, "protocol")
		session.remember(err)
		return err
	}
	grant := &executorGrant{session: session, work: work}
	defer grant.closeAndJoin()
	err = invoke(func() error { return run(bareContext{work}, grant) })
	grant.closeAndJoin()
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), work.Err(), context.Cause(work))
	if err != nil {
		err = failure(ErrNative, "protocol", err)
	}
	session.remember(err)
	return err
}

func nilLike(value any) bool {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan:
		return reflected.IsNil()
	}
	return false
}

type executorGrant struct {
	private
	session        *Session
	work           context.Context
	mu             sync.Mutex
	closed, active bool
	idle           chan struct{}
}

func (grant *executorGrant) closeAndJoin() {
	grant.mu.Lock()
	grant.closed = true
	active, idle := grant.active, grant.idle
	grant.mu.Unlock()
	if active {
		<-idle
	}
}
func (grant *executorGrant) Call(ctx context.Context, method string, params, result any) (err error) {
	grant.mu.Lock()
	if grant.closed || grant.active || ctx == nil {
		grant.mu.Unlock()
		return failure(ErrState, "protocol")
	}
	grant.active = true
	grant.idle = make(chan struct{})
	grant.mu.Unlock()
	defer func() {
		err = errors.Join(err, ctx.Err(), context.Cause(ctx), grant.work.Err(), context.Cause(grant.work))
		grant.session.remember(err)
		grant.mu.Lock()
		grant.active = false
		close(grant.idle)
		grant.mu.Unlock()
	}()
	if err := grant.work.Err(); err != nil {
		return failure(ErrState, "protocol", err, context.Cause(grant.work))
	}
	if err := ctx.Err(); err != nil {
		return failure(ErrState, "protocol", err, context.Cause(ctx))
	}
	if !permitted(method) {
		return failure(ErrUnsupported, "command")
	}
	if params == nil && protocolCommands[method].params != "cdp.Empty" {
		return failure(ErrInput, "missing-params")
	}
	if params != nil {
		typ := reflect.TypeOf(params)
		if typ.Kind() == reflect.Pointer {
			if nilLike(params) {
				return failure(ErrInput, "nil-params")
			}
			typ = typ.Elem()
		}
		want := protocolCommands[method].params
		if typ.PkgPath()+"."+typ.Name() != "github.com/chromedp/cdproto/"+want {
			return failure(ErrInput, "command-params")
		}
	}
	if method == "Page.printToPDF" {
		var mode page.PrintToPDFTransferMode
		switch value := params.(type) {
		case page.PrintToPDFParams:
			mode = value.TransferMode
		case *page.PrintToPDFParams:
			copied := *value
			params = copied
			mode = copied.TransferMode
		}
		if mode != "" && mode != page.PrintToPDFTransferModeReturnAsBase64 {
			return failure(ErrUnsupported, "native-stream")
		}
	}
	if err := grant.session.reserveCommand(); err != nil {
		return err
	}
	work, cancel := context.WithCancelCause(grant.work)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	defer stop()
	defer cancel(context.Canceled)
	var raw jsonv2.Value
	err = sdk.FromContext(grant.session.native).Target.Call(grant.session.commandWork(work, false), method, params, &raw)
	if err != nil {
		if errors.Is(err, sdk.ErrCommandLimit) {
			err = failure(ErrLimit, "command-bytes", err)
		}
		return failure(ErrNative, "command", err)
	}
	if result != nil {
		return jsonv2.Unmarshal(raw, result, sdk.DefaultUnmarshalOptions)
	}
	return nil
}
func (grant *executorGrant) Subscribe(string) (<-chan jsonv2.Value, func()) {
	events := make(chan jsonv2.Value)
	close(events)
	grant.session.remember(failure(ErrUnsupported, "protocol-subscription"))
	return events, func() {}
}

func (session *Session) nativeWork(ctx context.Context) context.Context {
	return session.commandWork(ctx, true)
}

func (session *Session) reserveCommand() error {
	record := session.accounting()
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.contextClosed {
		return failure(ErrState, "command")
	}
	if record.data.commands >= uint64(session.owner.settings.MaxCommands) {
		return failure(ErrLimit, "commands")
	}
	record.data.commands++
	return nil
}

func (session *Session) commandWork(ctx context.Context, admit bool, checks ...func() error) context.Context {
	value := session.owner.settings
	record := session.accounting()
	return sdk.WithCallLimits(nativeContext{Context: ctx, native: session.native}, sdk.CallLimits{
		MaxInputBytes: value.MaxCommandBytes, MaxOutputBytes: value.MaxResultBytes,
		Cleanup: session.cleanupObserver(ctx),
		Before: func(string) error {
			for _, check := range checks {
				if err := check(); err != nil {
					return err
				}
			}
			if admit {
				return session.reserveCommand()
			}
			return nil
		},
		Result: func(_ string, size int64) error {
			record.mu.Lock()
			defer record.mu.Unlock()
			if size > value.MaxResultBytes-record.delivered {
				return failure(ErrLimit, "command-output")
			}
			record.delivered += size
			return nil
		}})
}
func marshalBounded(buffer *boundedBuffer, value any) error {
	var encoder jsonv2.Encoder
	encoder.Reset(buffer, sdk.DefaultMarshalOptions)
	return jsonv2.MarshalEncode(&encoder, value, sdk.DefaultMarshalOptions)
}

func permitted(method string) bool {
	return finitePageCommands[method] || method == page.PrintToPDF.Method
}
