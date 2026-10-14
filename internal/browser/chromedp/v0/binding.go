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
	"sync"

	sdk "github.com/chromedp/chromedp"
)

// Binding owns one managed SDK registration and its actual callback execution.
type Binding struct {
	private
	state *managedBinding
}

type managedBinding struct {
	session                 *Session
	native                  *sdk.Binding
	life                    context.Context
	cancel                  context.CancelCauseFunc
	stopCaller              func() bool
	stopNative              func() bool
	mu                      sync.Mutex
	closed                  bool
	gate                    chan struct{}
	setup, primary, cleanup error
}

// BindingAdmission runs inside producer admission, never inside a user handler.
// Its completion owns one encoded SDK outcome; it must return without panic/Goexit.
type BindingAdmission func(context.Context, int64) (context.Context, func(sdk.BindingOutcome) error, error)

// BindFunc reserves registration and shared callback capacity before native setup.
// User handlers receive cancellation only, without native or caller context values.
func BindFunc[A, R any](ctx context.Context, session *Session, name string, fn func(context.Context, A) (R, error), admit BindingAdmission) (*Binding, error) {
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return nil, err
	}
	defer leave()
	if fn == nil || name == "" || int64(len(name)) > session.owner.settings.MaxCommandBytes {
		return nil, failure(ErrInput, "binding")
	}
	own := session.owner
	own.mu.Lock()
	if len(session.browserContext.bindings) >= own.settings.MaxBindings {
		own.mu.Unlock()
		return nil, failure(ErrLimit, "bindings")
	}
	life, cancel := context.WithCancelCause(session.work)
	state := &managedBinding{session: session, life: life, cancel: cancel, gate: make(chan struct{}, 1)}
	if session.browserContext.bindings == nil {
		session.browserContext.bindings = make(map[*managedBinding]struct{})
	}
	session.browserContext.bindings[state] = struct{}{}
	own.mu.Unlock()
	state.stopCaller = context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	value, err := sdk.Run(session.nativeWork(work), sdk.BindFunc(name, func(ctx context.Context, input A) (R, error) { return fn(bareContext{ctx}, input) }, sdk.BindingOptions{
		Limits:    sdk.BindingLimits{MaxActive: own.settings.MaxBindingCalls, MaxInputBytes: own.settings.MaxCommandBytes, MaxOutputBytes: own.settings.MaxResultBytes, PrivateErrors: true},
		MaxEvents: own.settings.MaxEvents, MaxEventBytes: own.settings.MaxEventBytes, CleanupTimeout: own.settings.CleanupTimeout,
		Admit: func(ctx context.Context, size int64) (context.Context, func(sdk.BindingOutcome), error) {
			own.mu.Lock()
			if state.life.Err() != nil || session.browserContext.bindingCalls >= own.settings.MaxBindingCalls {
				own.mu.Unlock()
				return nil, nil, failure(ErrLimit, "binding-calls")
			}
			session.browserContext.bindingCalls++
			own.mu.Unlock()
			release := sync.OnceFunc(func() { own.mu.Lock(); session.browserContext.bindingCalls--; own.mu.Unlock() })
			callback, complete := ctx, func(outcome sdk.BindingOutcome) error {
				cause := errors.Join(outcome.Primary, outcome.Reply)
				if cause != nil {
					return failure(ErrNative, "binding-call", cause)
				}
				return nil
			}
			if admit != nil {
				var err error
				callback, complete, err = admit(bareContext{ctx}, size)
				if err != nil {
					release()
					return nil, nil, err
				}
				if callback == nil || complete == nil {
					release()
					return nil, nil, failure(ErrInput, "binding-admission")
				}
			}
			return callback, func(outcome sdk.BindingOutcome) {
				defer release()
				delivered := false
				defer func() {
					if !delivered {
						session.remember(failure(ErrPanic, "binding-evidence-did-not-return"))
					}
				}()
				cause := complete(outcome)
				delivered = true
				if cause != nil {
					cause = failure(ErrNative, "binding-call", cause)
					state.mu.Lock()
					if state.primary == nil {
						state.primary = cause
					}
					state.mu.Unlock()
					session.remember(cause)
				}
			}, nil
		},
	}))
	if err != nil {
		err = failure(ErrNative, "binding", err)
		state.mu.Lock()
		state.setup = err
		state.mu.Unlock()
		session.remember(err)
	}
	if value == nil {
		state.stopCaller()
		cancel(nil)
		own.mu.Lock()
		delete(session.browserContext.bindings, state)
		own.mu.Unlock()
		return nil, err
	}
	state.native = value
	state.stopNative = context.AfterFunc(life, value.Cancel)
	return &Binding{state: state}, err
}

func (binding *Binding) Cancel() {
	if binding == nil || binding.state == nil {
		return
	}
	binding.state.cancel(context.Canceled)
	binding.state.native.Cancel()
}
func (binding *Binding) Stopping() <-chan struct{} {
	if binding == nil || binding.state == nil {
		return nil
	}
	return binding.state.native.Stopping()
}
func (binding *Binding) Closed() bool {
	if binding == nil || binding.state == nil {
		return false
	}
	binding.state.mu.Lock()
	defer binding.state.mu.Unlock()
	return binding.state.closed
}
func (binding *Binding) Err() error {
	if binding == nil || binding.state == nil {
		return failure(ErrInput, "binding")
	}
	state := binding.state
	state.mu.Lock()
	primary := errors.Join(state.setup, state.primary)
	state.mu.Unlock()
	if state.native != nil {
		if native := state.native.Err(); native != nil {
			return errors.Join(primary, failure(ErrNative, "binding", native))
		}
	}
	return primary
}
func (binding *Binding) Close(ctx context.Context) error {
	if ctx == nil || binding == nil || binding.state == nil {
		return failure(ErrInput, "binding-close")
	}
	state := binding.state
	binding.Cancel()
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return failure(ErrCleanup, "binding-join", ctx.Err(), context.Cause(ctx))
	}
	state.mu.Lock()
	closed, history := state.closed, state.cleanup
	state.mu.Unlock()
	if closed {
		return history
	}
	var err error
	gone := state.session.nativeResourcesGone()
	if gone {
		err = state.native.WaitStopped(ctx)
	} else {
		err = state.native.Close(ctx)
	}
	if err != nil {
		err = failure(ErrCleanup, "binding-close", err)
		state.mu.Lock()
		if state.cleanup == nil {
			state.cleanup = err
		}
		history = state.cleanup
		state.mu.Unlock()
		state.session.rememberCleanup(err)
	}
	if gone && err == nil || state.native.Closed() {
		state.stopCaller()
		state.stopNative()
		state.mu.Lock()
		state.closed = true
		state.mu.Unlock()
		state.session.owner.mu.Lock()
		delete(state.session.browserContext.bindings, state)
		state.session.owner.mu.Unlock()
	}
	state.session.remember(binding.Err())
	return errors.Join(history, err)
}
