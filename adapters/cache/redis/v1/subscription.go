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

package redis

import (
	"context"

	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// SubscriptionOptions selects channel, pattern or shard mode. Ring patterns use
// an explicit node-local RouteKey; channel sets must map to one Ring shard.
type SubscriptionOptions struct {
	private
	Mode     string
	Channels []string
	RouteKey string
}
type Subscription struct {
	private
	state *subscriptionState
}
type subscriptionState struct {
	op     *operation
	native *native.Subscription
	ended  bool
}

// Subscribe is messaging-only and owns its callback until it actually returns.
// Receive exposes confirmations separately from messages; reconnect can lose
// messages/repeat confirmations. Local close is not acknowledged unsubscribe.
func (view View) Subscribe(ctx, cleanup context.Context, options SubscriptionOptions, run func(context.Context, *Subscription) error) (Result, error) {
	if view.capability != Messaging || cleanup == nil || run == nil {
		return Result{}, problem(view.capability, ErrInput, "subscription")
	}
	return result(view.dispatch(ctx, "subscribe", Lifecycle, nil, func(op *operation, bound *native.Client) {
		var receipt *invocation.Receipt[native.Result]
		var err error
		defer func() { op.finishCallback(receipt, err) }()
		receipt, err = bound.Subscribe(op.lifetime, cleanup, op.correlation(), native.SubscriptionOptions{Mode: options.Mode, Channels: options.Channels, RouteKey: options.RouteKey}, func(ctx context.Context, borrowed *native.Subscription) error {
			if err := op.claimRoot(); err != nil {
				return err
			}
			subscription := &Subscription{state: &subscriptionState{op: op, native: borrowed}}
			defer func() { op.family.gate.Lock(); subscription.state.ended = true; op.family.gate.Unlock() }()
			return run(ctx, subscription)
		})
	}))
}

// Receive reserves independent evidence before reading. The native event is an
// immutable Array: ["message", channel, pattern, payload], [confirmation-kind,
// channel, count], or ["pong", payload]. Empty text remains present.
// The payload position preserves native text or array shape.
func (subscription *Subscription) Receive(ctx context.Context) (Result, error) {
	if subscription == nil || subscription.state == nil {
		return Result{}, fail(ErrMessagingState, "receive")
	}
	state := subscription.state
	if !state.op.family.gate.TryLock() {
		return Result{}, fail(ErrMessagingState, "receive")
	}
	defer state.op.family.gate.Unlock()
	if state.ended || state.op.lifetime.Err() != nil {
		return Result{}, fail(ErrMessagingState, "receive")
	}
	return state.op.child(ctx, "receive", Messaging, SubscriptionEvent, nil, func(ctx context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return state.native.Receive(ctx, id)
	})
}
