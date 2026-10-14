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
	"iter"
	"sync"

	"github.com/chromedp/cdproto/cdp"
	sdk "github.com/chromedp/chromedp"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Subscription owns one typed native event subscription, independent of its
// iterator. Close or cancel its construction context even when never reading.
// Overflow seals this subscription, not the page or unrelated subscriptions.
type Subscription[E any] struct {
	private
	state *subscriptionState
}

type subscriptionState struct {
	native            *native.Subscription
	context           *Context
	page              *Page
	call              *adapters.Call[Result]
	guard             adapters.Guard
	gate              chan struct{}
	mu                sync.Mutex
	closing, complete bool
	active            bool
	idle              chan struct{}
	iterator          bool
	iteratorDone      chan struct{}
	primary, final    error
}

// Subscribe installs the exact native event before returning. It never enables
// a protocol domain implicitly. The caller must close it or cancel ctx.
func Subscribe[E any](ctx context.Context, page *Page, event cdp.Event[E]) (*Subscription[E], *adapters.Receipt[Result], error) {
	return subscribe[E](ctx, page, func(ctx context.Context, session *native.Session) (*native.Subscription, error) {
		return native.SubscribeEvent(ctx, session, event)
	})
}

// Console preserves the SDK's ordered formatting for console calls, uncaught
// exceptions and browser log entries, under managed subscription ownership.
func Console(ctx context.Context, page *Page) (*Subscription[sdk.ConsoleMessage], *adapters.Receipt[Result], error) {
	return subscribe[sdk.ConsoleMessage](ctx, page, native.SubscribeConsole)
}

func subscribe[E any](ctx context.Context, page *Page, create func(context.Context, *native.Session) (*native.Subscription, error)) (*Subscription[E], *adapters.Receipt[Result], error) {
	if ctx == nil || page == nil || page.context == nil {
		return nil, nil, fail(ErrInput, "subscribe")
	}
	c := page.context
	var subscription *Subscription[E]
	var primary error
	receipt, err := c.endpoint.Child(ctx, c.call.Scope(), request("subscribe", c.id, 0, c.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
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
		value, err := create(call.Context(), page.native)
		primary = translate(err, "subscribe")
		if value == nil {
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			_ = guard.Release()
			return
		}
		state := &subscriptionState{native: value, context: c, page: page, call: call, guard: guard, gate: make(chan struct{}, 1)}
		subscription = &Subscription[E]{state: state}
		c.mu.Lock()
		if c.subscriptions == nil {
			c.subscriptions = make(map[*subscriptionState]struct{})
		}
		c.subscriptions[state] = struct{}{}
		c.mu.Unlock()
		go func() {
			<-value.Done()
			_ = state.close(context.Background())
		}()
	})
	return subscription, receipt, errors.Join(err, receiptError(receipt, primary))
}

// Next reserves output evidence before receiving or decoding the event. Native
// payloads are deliberate sensitive data. A failed receive never retries a trigger.
func (subscription *Subscription[E]) Next(ctx context.Context) (value E, receipt *adapters.Receipt[Result], err error) {
	if ctx == nil || subscription == nil || subscription.state == nil {
		return value, nil, fail(ErrInput, "event")
	}
	state, c := subscription.state, subscription.state.context
	var primary error
	receipt, err = c.endpoint.Child(ctx, state.call.Scope(), request("event", c.id, 0, c.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := state.page.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		defer finish()
		state.mu.Lock()
		if state.closing || state.active {
			state.mu.Unlock()
			primary = fail(ErrState, "event-reader")
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		state.active = true
		state.idle = make(chan struct{})
		state.mu.Unlock()
		defer func() {
			state.mu.Lock()
			state.active = false
			close(state.idle)
			state.mu.Unlock()
		}()
		var event native.Event
		value, event, err = native.ReceiveEvent[E](call.Context(), state.native)
		primary = translate(err, "event")
		state.mu.Lock()
		if primary != nil && state.primary == nil {
			state.primary = primary
		}
		state.mu.Unlock()
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: event.DataCopy(), attribution: attribution(info.Info()), observation: browser.Observation{Returned: err == nil, Effect: browser.NotSubmitted}}, Primary: primary})
	})
	return value, receipt, errors.Join(err, receiptError(receipt, primary))
}

// Events closes the subscription on break or error. It does not acknowledge
// evidence for the caller. A constructor context also stops an unstarted iterator.
// Close waits for a running yield callback. Cancel the constructor context rather
// than synchronously closing the subscription from inside that callback.
func (subscription *Subscription[E]) Events(ctx context.Context) iter.Seq2[E, error] {
	return func(yield func(E, error) bool) {
		if subscription == nil || subscription.state == nil {
			var value E
			yield(value, fail(ErrInput, "events"))
			return
		}
		state := subscription.state
		state.mu.Lock()
		if state.closing || state.iterator {
			state.mu.Unlock()
			var value E
			yield(value, fail(ErrState, "events"))
			return
		}
		state.iterator = true
		state.iteratorDone = make(chan struct{})
		state.mu.Unlock()
		defer func() {
			state.mu.Lock()
			state.iterator = false
			close(state.iteratorDone)
			state.mu.Unlock()
			_ = subscription.Close(context.Background())
		}()
		for {
			value, _, err := subscription.Next(ctx)
			if !yield(value, err) || err != nil {
				return
			}
		}
	}
}

func (subscription *Subscription[E]) Close(ctx context.Context) error {
	if subscription == nil || subscription.state == nil || ctx == nil {
		return fail(ErrInput, "subscription-close")
	}
	return subscription.state.close(ctx)
}
func (subscription *Subscription[E]) Closed() bool {
	return subscription != nil && subscription.state != nil && subscription.state.closed()
}
func (state *subscriptionState) closed() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.complete
}
func (state *subscriptionState) close(ctx context.Context) error {
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "subscription-close")
	}
	state.mu.Lock()
	if state.complete {
		err := state.final
		state.mu.Unlock()
		return err
	}
	state.closing = true
	active, idle := state.active, state.idle
	iterator, iteratorDone := state.iterator, state.iteratorDone
	state.mu.Unlock()
	_ = state.call.Cancel(nil)
	cleanup := translate(state.native.Close(ctx), "subscription-close")
	if !state.native.Closed() {
		return cleanup
	}
	if active {
		select {
		case <-idle:
		case <-ctx.Done():
			return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "subscription-join")
		}
	}
	if iterator {
		select {
		case <-iteratorDone:
		case <-ctx.Done():
			return translate(errors.Join(ctx.Err(), context.Cause(ctx)), "iterator-join")
		}
	}
	state.mu.Lock()
	primary := errors.Join(state.primary, translate(state.native.Err(), "subscription"))
	state.mu.Unlock()
	info, _ := state.call.Receipt().Snapshot()
	if err := state.call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{attribution: attribution(info.Info()), observation: browser.Observation{Returned: primary == nil, Effect: browser.NotSubmitted}}, Primary: primary, Cleanup: cleanup}); err != nil {
		return err
	}
	state.mu.Lock()
	state.complete = true
	state.final = errors.Join(primary, cleanup)
	state.mu.Unlock()
	state.context.mu.Lock()
	delete(state.context.subscriptions, state)
	state.context.mu.Unlock()
	_ = state.guard.Release()
	return state.final
}
