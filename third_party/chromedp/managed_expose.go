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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
)

// BindingOutcome describes one admitted handler, encoding and reply.
type BindingOutcome struct {
	Value             jsonv2.Value
	Undefined         bool
	Returned          bool
	ReplyAcknowledged bool
	Primary           error
	Reply             error
}

// BindingOptions supplies producer admission and per-binding limits.
// Admit runs before payload decoding and handler creation. It must return
// promptly, without browser calls. Admit and Complete are trusted owner hooks.
// They must not panic, call Goexit, or retain work on a rejected admission.
// Complete receives already encoded output and must return promptly.
type BindingOptions struct {
	Limits         BindingLimits
	MaxEvents      int
	MaxEventBytes  int64
	CleanupTimeout time.Duration
	Admit          func(context.Context, int64) (context.Context, func(BindingOutcome), error)
}

// Binding owns one registration, its event producer and actual handler calls.
// Cancel requests shutdown. Close removes the registration and joins the work.
type Binding struct {
	target                  *Target
	name, nativeName        string
	options                 BindingOptions
	calls                   bindingState
	life                    context.Context
	cancel                  context.CancelCauseFunc
	setupDone, listenerDone chan struct{}
	gate                    chan struct{}
	mu                      sync.Mutex
	pending                 map[uint64]*bindingAdmission
	captures                map[string]chan struct{}
	cleaned                 map[string]bool
	script                  page.ScriptIdentifier
	scriptAttempted         bool
	nativeAttempted         bool
	closed                  bool
	setup, primary, cleanup error
}

type bindingAdmission struct {
	origin   string
	install  bool
	ctx      context.Context
	complete func(BindingOutcome)
	release  func()
}

// BindFunc uses the same page wrapper and argument conversion as ExposeFunc.
// A non-nil result needs Close, including after a partial installation failure.
func BindFunc[A, R any](name string, fn func(context.Context, A) (R, error), options BindingOptions) Action[*Binding] {
	return func(ctx context.Context, target *Target) (*Binding, error) {
		options := options
		if options.Limits.MaxActive < 0 || options.Limits.MaxInputBytes < 0 || options.Limits.MaxOutputBytes < 0 || options.MaxEvents < 0 || options.MaxEventBytes < 0 || options.CleanupTimeout < 0 {
			return nil, errors.New("invalid binding limits")
		}
		global := target.bindings.limits
		options.Limits.MaxActive = int(bindingLimit(int64(options.Limits.MaxActive), int64(global.MaxActive)))
		options.Limits.MaxInputBytes = bindingLimit(options.Limits.MaxInputBytes, global.MaxInputBytes)
		options.Limits.MaxOutputBytes = bindingLimit(options.Limits.MaxOutputBytes, global.MaxOutputBytes)
		options.Limits.PrivateErrors = options.Limits.PrivateErrors || global.PrivateErrors
		if name == "" || strings.HasPrefix(name, exposePrefix) || fn == nil {
			return nil, errors.New("invalid binding name or handler")
		}
		if options.Limits.MaxInputBytes > 0 && int64(len(name)) > options.Limits.MaxInputBytes {
			return nil, ErrBindingLimit
		}
		if err := target.executionOriginError(); err != nil {
			return nil, err
		}
		jsName, err := jsonv2.Marshal(name)
		if err != nil {
			return nil, fmt.Errorf("encoding binding name: %w", err)
		}
		if options.CleanupTimeout <= 0 {
			options.CleanupTimeout = 5 * time.Second
		}
		life, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
		b := &Binding{target: target, name: name, nativeName: exposePrefix + rand.Text(), options: options, life: life, cancel: cancel,
			setupDone: make(chan struct{}), listenerDone: make(chan struct{}), gate: make(chan struct{}, 1), pending: make(map[uint64]*bindingAdmission)}
		nativeReady := make(chan struct{})
		b.calls.limits = options.Limits
		target.exposedMu.Lock()
		if _, exists := target.exposed[name]; exists {
			target.exposedMu.Unlock()
			cancel(nil)
			return nil, errors.New("binding name is in use already")
		}
		if target.exposed == nil {
			target.exposed = make(map[string]*Binding)
		}
		target.exposed[name] = b
		target.exposedMu.Unlock()
		defer close(b.setupDone)
		events := EventBuffer{MaxEvents: options.MaxEvents, MaxBytes: options.MaxEventBytes}
		if events.MaxEvents == 0 {
			events.MaxEvents = target.events.limits.MaxEvents
		}
		if events.MaxBytes == 0 {
			events.MaxBytes = target.events.limits.MaxBytes
		}
		events.filter = func(method string, raw jsonv2.Value) bool {
			if method == runtime.ExecutionContextCreated.Method {
				return true
			}
			routing := bindingState{limits: BindingLimits{MaxInputBytes: target.events.limits.MaxBytes}}
			matched, err := routing.matches(raw, b.nativeName)
			if err != nil {
				b.report(err)
				b.Cancel()
			}
			return matched && err == nil
		}
		events.Observe = func(method string, raw jsonv2.Value, sequence uint64) error {
			if method == runtime.ExecutionContextCreated.Method {
				var event runtime.EventExecutionContextCreated
				if err := jsonv2.Unmarshal(raw, &event, DefaultUnmarshalOptions); err != nil {
					return err
				}
				if event.Context == nil {
					return errors.New("binding execution context is missing")
				}
				b.mu.Lock()
				b.pending[sequence] = &bindingAdmission{origin: event.Context.UniqueID, install: true}
				b.mu.Unlock()
				return nil
			}
			return b.admit(raw, sequence)
		}
		events.OnOverflow = func(err error) { b.report(err); b.Cancel() }
		events.OnError = func(err error) { b.report(err); b.Cancel() }
		raw, unsubscribe := target.SubscribeBounded(events, runtime.BindingCalled.Method, runtime.ExecutionContextCreated.Method)
		go func() {
			defer close(b.listenerDone)
			defer func() { b.Cancel(); unsubscribe(); b.discardPending() }()
			for {
				select {
				case <-b.life.Done():
					return
				case raw, ok := <-raw:
					if !ok {
						return
					}
					var message struct {
						Params   jsonv2.Value `json:"params"`
						Sequence uint64       `json:"sequence"`
					}
					if err := jsonv2.Unmarshal(raw, &message, DefaultUnmarshalOptions); err != nil {
						b.report(err)
						return
					}
					b.mu.Lock()
					admission := b.pending[message.Sequence]
					delete(b.pending, message.Sequence)
					b.mu.Unlock()
					if admission == nil {
						b.report(errors.New("binding admission is missing"))
						return
					}
					if admission.install {
						select {
						case <-nativeReady:
						case <-b.life.Done():
							return
						}
						work, cancel := context.WithTimeout(context.WithoutCancel(b.life), b.options.CleanupTimeout)
						work = WithCallLimits(work, CallLimits{MaxInputBytes: 65536, MaxOutputBytes: 65536})
						err := b.captureNativeFunction(work, admission.origin)
						cancel()
						if err != nil {
							b.report(err)
							return
						}
						continue
					}
					go runBindingCall(b, admission, message.Params, fn)
				}
			}
		}()
		jsBinding, _ := jsonv2.Marshal(b.nativeName)
		script := "(" + exposeFuncJS + ")(" + string(jsName) + "," + string(jsBinding) + ")"
		setup := withQueued(ctx, func(method string) {
			switch method {
			case runtime.AddBinding.Method:
				b.nativeAttempted = true
			case page.AddScriptToEvaluateOnNewDocument.Method:
				b.scriptAttempted = true
			}
		})
		_, err = cdp.Call(setup, target, runtime.AddBinding, runtime.AddBindingParams{Name: b.nativeName})
		close(nativeReady)
		if err == nil {
			for _, origin := range target.executionOriginCopy() {
				if err = b.captureNativeFunction(setup, origin); err != nil {
					break
				}
			}
		}
		if err == nil {
			result, evalErr := cdp.Call(setup, target, runtime.Evaluate, runtime.EvaluateParams{Expression: script})
			err = evalErr
			if err == nil && result.ExceptionDetails != nil {
				err = &ExceptionError{result.ExceptionDetails}
			}
		}
		if err == nil && !target.isWorker {
			added, addErr := cdp.Call(setup, target, page.AddScriptToEvaluateOnNewDocument, page.AddScriptToEvaluateOnNewDocumentParams{Source: script, RunImmediately: ptr(true)})
			b.script, err = added.Identifier, addErr
			if err == nil && b.script == "" {
				err = errors.New("binding script has no identity")
			}
		}
		if err == nil {
			err = target.executionOriginError()
		}
		if err != nil {
			b.mu.Lock()
			b.setup = err
			b.mu.Unlock()
			b.report(err)
			b.Cancel()
		}
		return b, err
	}
}

func bindingLimit(selected, global int64) int64 {
	if selected <= 0 {
		return global
	}
	if global <= 0 {
		return selected
	}
	return min(selected, global)
}

func (b *Binding) report(err error) {
	if err == nil {
		return
	}
	b.mu.Lock()
	if b.primary == nil {
		b.primary = err
	}
	b.mu.Unlock()
	b.calls.report(err)
	b.target.bindings.report(err)
}

func (b *Binding) admit(raw jsonv2.Value, sequence uint64) error {
	if !b.target.bindings.acquire(len(raw)) {
		return ErrBindingLimit
	}
	if !b.calls.acquire(len(raw)) {
		b.target.bindings.release()
		return ErrBindingLimit
	}
	release := sync.OnceFunc(func() { b.calls.release(); b.target.bindings.release() })
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	var route struct {
		ExecutionContextID runtime.ExecutionContextID `json:"executionContextId"`
	}
	if err := jsonv2.Unmarshal(raw, &route, DefaultUnmarshalOptions); err != nil {
		release()
		return err
	}
	origin := b.target.executionOrigin(route.ExecutionContextID)
	if origin == "" {
		release()
		return errors.New("binding execution origin is missing")
	}
	ctx, complete := b.life, func(BindingOutcome) {}
	if b.options.Admit != nil {
		var err error
		ctx, complete, err = b.options.Admit(b.life, int64(len(raw)))
		if err != nil {
			release()
			return err
		}
		if ctx == nil || complete == nil {
			release()
			return errors.New("binding admission is invalid")
		}
	}
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(b.life, func() { cancel(context.Cause(b.life)) })
	originalRelease := release
	release = sync.OnceFunc(func() { stop(); cancel(nil); originalRelease() })
	b.mu.Lock()
	b.pending[sequence] = &bindingAdmission{origin: origin, ctx: work, complete: complete, release: release}
	transferred = true
	b.mu.Unlock()
	return nil
}
func (b *Binding) discardPending() {
	b.mu.Lock()
	pending := b.pending
	b.pending = make(map[uint64]*bindingAdmission)
	b.mu.Unlock()
	for _, admission := range pending {
		if admission.install {
			continue
		}
		func() {
			defer admission.release()
			admission.complete(BindingOutcome{Primary: context.Canceled})
		}()
	}
}

func runBindingCall[A, R any](b *Binding, admission *bindingAdmission, raw jsonv2.Value, fn func(context.Context, A) (R, error)) {
	defer admission.release()
	outcome := BindingOutcome{Undefined: reflect.TypeFor[R]() == reflect.TypeFor[Void]()}
	callID := int64(0)
	finished := false
	defer func() {
		defer func() {
			b.report(errors.Join(outcome.Primary, outcome.Reply))
			admission.complete(outcome)
		}()
		if value := recover(); value != nil {
			if b.calls.limits.PrivateErrors {
				cause, _ := value.(error)
				outcome.Primary = errors.Join(errors.New("binding panic"), cause)
			} else {
				outcome.Primary = fmt.Errorf("panic: %v", value)
			}
		} else if !finished {
			outcome.Primary = errors.New("binding call did not return")
		}
		outcome.Primary = errors.Join(outcome.Primary, admission.ctx.Err(), context.Cause(admission.ctx), b.life.Err(), context.Cause(b.life))
		if callID != 0 {
			outcome.Reply = errors.New("binding reply did not return")
			outcome.ReplyAcknowledged, outcome.Reply = b.reply(admission.origin, callID, outcome)
		}
	}()
	if admission.ctx.Err() != nil || b.life.Err() != nil {
		outcome.Primary = errors.Join(context.Cause(admission.ctx), context.Cause(b.life))
		finished = true
		return
	}
	var event runtime.EventBindingCalled
	if err := jsonv2.Unmarshal(raw, &event, DefaultUnmarshalOptions); err != nil {
		outcome.Primary = err
		finished = true
		return
	}
	var call struct {
		ID   int64          `json:"id"`
		Args []jsonv2.Value `json:"args"`
	}
	if err := jsonv2.Unmarshal([]byte(event.Payload), &call, DefaultUnmarshalOptions); err != nil {
		outcome.Primary = err
		finished = true
		return
	}
	callID = call.ID
	if callID <= 0 {
		outcome.Primary = errors.New("binding call has no identity")
		finished = true
		return
	}
	arg, err := exposedArg[A](call.Args)
	if err != nil {
		outcome.Primary = err
		finished = true
		return
	}
	result, err := fn(admission.ctx, arg)
	outcome.Returned, outcome.Primary = true, err
	if err == nil && !outcome.Undefined {
		if b.calls.limits.MaxOutputBytes > 0 {
			buffer := bindingBuffer{limit: b.calls.limits.MaxOutputBytes}
			outcome.Primary = marshalBinding(&buffer, result)
			outcome.Value = buffer.data
		} else {
			outcome.Value, outcome.Primary = jsonv2.Marshal(result, DefaultMarshalOptions)
		}
	}
	finished = true
}

func (b *Binding) reply(origin string, id int64, outcome BindingOutcome) (bool, error) {
	if !b.target.hasExecutionOrigin(origin) {
		return false, errors.New("binding execution origin ended")
	}
	ok, value := "true", "undefined"
	if outcome.Primary != nil {
		ok = "false"
		message := "binding failed"
		if !b.calls.limits.PrivateErrors {
			message = strings.ToValidUTF8(outcome.Primary.Error(), "\uFFFD")
		}
		data, err := jsonv2.Marshal(message)
		if err != nil {
			return false, err
		}
		value = string(data)
	} else if !outcome.Undefined {
		value = string(outcome.Value)
	}
	name, _ := jsonv2.Marshal(b.nativeName)
	expression := "(" + exposeReplyJS + ")(" + string(name) + "," + strconv.FormatInt(id, 10) + "," + ok + "," + value + ")"
	ctx, cancel := context.WithTimeout(context.WithoutCancel(b.life), b.options.CleanupTimeout)
	defer cancel()
	ctx = WithCallLimits(ctx, CallLimits{MaxInputBytes: 6*int64(len(expression)) + 4096, MaxOutputBytes: 65536})
	result, err := cdp.Call(ctx, b.target, runtime.Evaluate, runtime.EvaluateParams{Expression: expression, UniqueContextID: origin, ReturnByValue: ptr(true), Silent: ptr(true)})
	if err != nil {
		return false, err
	}
	if result.ExceptionDetails != nil {
		return false, &ExceptionError{result.ExceptionDetails}
	}
	if result.Result == nil || string(result.Result.Value) != "true" {
		return false, errors.New("binding reply did not settle a promise")
	}
	return true, nil
}

// Cancel does not wait for cooperative handlers or codecs.
func (b *Binding) Cancel() {
	if b == nil {
		return
	}
	b.calls.mu.Lock()
	b.calls.sealed = true
	b.calls.mu.Unlock()
	b.cancel(context.Canceled)
}

// Stopping closes when shutdown is requested, before actual handler completion.
func (b *Binding) Stopping() <-chan struct{} {
	if b == nil {
		return nil
	}
	return b.life.Done()
}

// WaitStopped joins the event listener and the actual handler, codec and reply work.
func (b *Binding) WaitStopped(ctx context.Context) error {
	if b == nil || ctx == nil {
		return ErrInvalidContext
	}
	select {
	case <-b.listenerDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.calls.mu.Lock()
	active, idle := b.calls.active, b.calls.idle
	b.calls.mu.Unlock()
	if active != 0 {
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Closed reports confirmed removal and actual work completion.
func (b *Binding) Closed() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// Err preserves the first handler or producer failure for explicit inspection.
func (b *Binding) Err() error {
	if b == nil {
		return ErrInvalidContext
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return errors.Join(b.setup, b.primary)
}

// Close first revokes the page wrapper, then joins handlers and replies.
// Cleanup history remains available after a successful retry.
func (b *Binding) Close(ctx context.Context) error {
	if b == nil || ctx == nil {
		return ErrInvalidContext
	}
	b.Cancel()
	work, cancel := context.WithTimeout(ctx, b.options.CleanupTimeout)
	defer cancel()
	select {
	case b.gate <- struct{}{}:
		defer func() { <-b.gate }()
	case <-work.Done():
		return work.Err()
	}
	select {
	case <-b.setupDone:
	case <-work.Done():
		return work.Err()
	}
	select {
	case <-b.listenerDone:
	case <-work.Done():
		return work.Err()
	}
	b.mu.Lock()
	closed, history := b.closed, b.cleanup
	b.mu.Unlock()
	if closed {
		return history
	}
	for origin := range b.cleaned {
		if !b.target.hasExecutionOrigin(origin) {
			delete(b.cleaned, origin)
		}
	}
	work = WithCallLimits(work, CallLimits{MaxInputBytes: 65536, MaxOutputBytes: 65536})
	var cleanup error
	nativeActive := b.nativeAttempted
	if nativeActive {
		for _, origin := range b.target.executionOriginCopy() {
			if b.cleaned[origin] {
				continue
			}
			cleanup = errors.Join(cleanup, b.captureNativeFunction(work, origin))
		}
	}
	if b.scriptAttempted {
		if b.script == "" {
			cleanup = errors.Join(cleanup, errors.New("binding script identity is unknown"))
		} else {
			_, err := cdp.Call(work, b.target, page.RemoveScriptToEvaluateOnNewDocument, page.RemoveScriptToEvaluateOnNewDocumentParams{Identifier: b.script})
			if err == nil {
				b.scriptAttempted = false
			} else {
				cleanup = errors.Join(cleanup, err)
			}
		}
	}
	if b.nativeAttempted {
		_, err := cdp.Call(work, b.target, runtime.RemoveBinding, runtime.RemoveBindingParams{Name: b.nativeName})
		if err == nil {
			b.nativeAttempted = false
		} else {
			cleanup = errors.Join(cleanup, err)
		}
	}
	name, _ := jsonv2.Marshal(b.nativeName)
	expression := "(" + exposeRevokeJS + ")(" + string(name) + ")"
	for _, origin := range b.target.executionOriginCopy() {
		if b.cleaned[origin] {
			continue
		}
		if nativeActive {
			cleanup = errors.Join(cleanup, b.captureNativeFunction(work, origin))
		} else {
			b.mu.Lock()
			captured := b.captures[origin] != nil
			b.mu.Unlock()
			if !captured {
				continue
			}
		}
		result, err := cdp.Call(work, b.target, runtime.Evaluate, runtime.EvaluateParams{Expression: expression, UniqueContextID: origin, ReturnByValue: ptr(true), Silent: ptr(true)})
		if err != nil && !b.target.hasExecutionOrigin(origin) {
			continue
		}
		if err == nil && result.ExceptionDetails != nil {
			err = &ExceptionError{result.ExceptionDetails}
		}
		if err == nil && (result.Result == nil || string(result.Result.Value) != "true") {
			err = errors.New("binding wrapper cleanup was not confirmed")
		}
		if err == nil {
			if b.cleaned == nil {
				b.cleaned = make(map[string]bool)
			}
			b.cleaned[origin] = true
		}
		cleanup = errors.Join(cleanup, err)
	}
	cleanup = errors.Join(cleanup, b.WaitStopped(work))
	cleanup = errors.Join(cleanup, b.target.executionOriginError())
	if cleanup != nil {
		b.mu.Lock()
		if b.cleanup == nil {
			b.cleanup = cleanup
		}
		history = b.cleanup
		b.mu.Unlock()
		return errors.Join(history, cleanup)
	}
	b.target.exposedMu.Lock()
	if b.target.exposed[b.name] == b {
		delete(b.target.exposed, b.name)
	}
	b.target.exposedMu.Unlock()
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	return history
}

func (b *Binding) captureNativeFunction(ctx context.Context, origin string) error {
	b.mu.Lock()
	for previous, done := range b.captures {
		select {
		case <-done:
			if !b.target.hasExecutionOrigin(previous) {
				delete(b.captures, previous)
			}
		default:
		}
	}
	if done := b.captures[origin]; done != nil {
		b.mu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if b.captures == nil {
		b.captures = make(map[string]chan struct{})
	}
	done := make(chan struct{})
	b.captures[origin] = done
	b.mu.Unlock()
	queued := false
	defer func() {
		if !queued {
			b.mu.Lock()
			delete(b.captures, origin)
			b.mu.Unlock()
		}
		close(done)
	}()
	// A lost reply cannot authorize capturing a later replacement by name.
	name, _ := jsonv2.Marshal(b.nativeName)
	expression := "(" + exposeCleanupJS + ")(" + string(name) + ")"
	limits, _ := ctx.Value(callLimitsKey{}).(CallLimits)
	limits.MaxInputBytes = bindingLimit(65536, limits.MaxInputBytes)
	limits.MaxOutputBytes = bindingLimit(65536, limits.MaxOutputBytes)
	ctx = withQueued(WithCallLimits(ctx, limits), func(string) { queued = true })
	result, err := cdp.Call(ctx, b.target, runtime.Evaluate, runtime.EvaluateParams{Expression: expression, UniqueContextID: origin, ReturnByValue: ptr(true), Silent: ptr(true)})
	if err != nil && !b.target.hasExecutionOrigin(origin) {
		return nil
	}
	if err == nil && result.ExceptionDetails != nil {
		err = &ExceptionError{result.ExceptionDetails}
	}
	if err == nil && (result.Result == nil || string(result.Result.Value) != "true") {
		err = errors.New("binding cleanup capture was not confirmed")
	}
	return err
}
