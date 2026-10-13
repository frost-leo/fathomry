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
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Binding owns one page-to-Go registration. Aliases share its source, limits and
// cleanup. Cancel is safe inside a handler; Close must not wait for that handler
// from within its own stack.
type Binding struct {
	private
	state *bindingState
}
type bindingState struct {
	native   *native.Binding
	context  *Context
	page     *Page
	call     *adapters.Call[Result]
	guard    adapters.Guard
	gate     chan struct{}
	mu       sync.Mutex
	complete bool
	final    error
}

// BindFunc uses native argument and promise semantics, without passing a Target
// or a recoverable native context to fn. Each callback reserves independent
// evidence before A decoding, goroutine creation and R encoding. Page-visible
// errors are fixed text; native causes remain deliberately inspectable locally.
func BindFunc[A, R any](ctx context.Context, page *Page, name string, fn func(context.Context, A) (R, error)) (*Binding, *adapters.Receipt[Result], error) {
	if ctx == nil || page == nil || page.context == nil || fn == nil {
		return nil, nil, fail(ErrInput, "binding")
	}
	c := page.context
	var binding *Binding
	var primary error
	receipt, err := c.endpoint.Child(ctx, c.call.Scope(), request("binding", c.id, 0, c.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := page.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		defer finish()
		guard, err := call.Hold()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		state := &bindingState{context: c, page: page, call: call, guard: guard, gate: make(chan struct{}, 1)}
		admit := func(ctx context.Context, _ int64) (context.Context, func(sdk.BindingOutcome) error, error) {
			var callback context.Context
			var complete func(sdk.BindingOutcome) error
			var primary error
			receipt, err := c.endpoint.Child(ctx, call.Scope(), request("binding_call", c.id, 0, c.budget.EvidenceBytes), func(child *adapters.Call[Result]) {
				held, err := child.Hold()
				if err != nil {
					primary = err
					_ = child.Resolve(adapters.Outcome[Result]{Primary: primary})
					return
				}
				callback = child.Context()
				complete = func(outcome sdk.BindingOutcome) (primary error) {
					resolved := false
					defer func() {
						if !resolved {
							_ = child.Resolve(adapters.Outcome[Result]{Primary: fail(ErrOperation, "binding-evidence-did-not-return")})
						}
						_ = held.Release()
					}()
					if cause := errors.Join(outcome.Primary, outcome.Reply); cause != nil {
						primary = fail(ErrOperation, "binding-call", cause)
					}
					effect := browser.Unknown
					if outcome.ReplyAcknowledged {
						effect = browser.Acknowledged
					}
					info, _ := child.Receipt().Snapshot()
					value := Result{payload: append([]byte(nil), outcome.Value...), attribution: attribution(info.Info()), observation: browser.Observation{Returned: outcome.Returned, Effect: effect},
						binding: BindingObservation{Returned: outcome.Returned, Undefined: outcome.Undefined, ReplyAcknowledged: outcome.ReplyAcknowledged}}
					err := child.Resolve(adapters.Outcome[Result]{Present: true, Value: value, Primary: primary})
					resolved = err == nil
					return errors.Join(primary, err)
				}
			})
			return callback, complete, errors.Join(err, receiptError(receipt, primary))
		}
		value, err := native.BindFunc(call.Context(), page.native, name, fn, admit)
		primary = translate(err, "binding")
		if value == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state.native = value
		binding = &Binding{state: state}
		c.mu.Lock()
		if c.bindings == nil {
			c.bindings = make(map[*bindingState]struct{})
		}
		c.bindings[state] = struct{}{}
		c.mu.Unlock()
		go func() { <-value.Stopping(); _ = binding.Close(context.Background()) }()
	})
	return binding, receipt, errors.Join(err, receiptError(receipt, primary))
}
func (binding *Binding) Cancel() {
	if binding == nil || binding.state == nil {
		return
	}
	_ = binding.state.call.Cancel(nil)
	binding.state.native.Cancel()
}
func (binding *Binding) Closed() bool {
	if binding == nil || binding.state == nil {
		return false
	}
	binding.state.mu.Lock()
	defer binding.state.mu.Unlock()
	return binding.state.complete
}
func (binding *Binding) Close(ctx context.Context) error {
	if ctx == nil || binding == nil || binding.state == nil {
		return fail(ErrInput, "binding-close")
	}
	state := binding.state
	binding.Cancel()
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "binding-join")
	}
	state.mu.Lock()
	if state.complete {
		err := state.final
		state.mu.Unlock()
		return err
	}
	state.mu.Unlock()
	cleanup := translate(state.native.Close(ctx), "binding-close")
	if !state.native.Closed() {
		return cleanup
	}
	var primary error
	if err := state.native.Err(); err != nil {
		primary = fail(ErrOperation, "binding", err)
	}
	info, _ := state.call.Receipt().Snapshot()
	if err := state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{attribution: attribution(info.Info()), observation: browser.Observation{Returned: primary == nil, Effect: browser.Unknown}}, Primary: primary, Cleanup: cleanup}); err != nil {
		return err
	}
	state.mu.Lock()
	state.complete = true
	state.final = errors.Join(primary, cleanup)
	state.mu.Unlock()
	state.context.mu.Lock()
	delete(state.context.bindings, state)
	state.context.mu.Unlock()
	_ = state.guard.Release()
	return state.final
}
