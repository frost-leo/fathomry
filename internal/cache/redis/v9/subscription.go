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
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/redis/go-redis/v9"
)

// SubscriptionOptions names channel, pattern or shard subscriptions.
// RouteKey explicitly selects the Ring node for patterns. Ring channel
// subscriptions must all hash to the same shard; use separate sessions otherwise.
// Shard subscriptions require a supporting server and same-slot channels.
type SubscriptionOptions struct {
	private
	Mode     string
	Channels []string
	RouteKey string
}

// Subscription owns no shutdown authority. Receive is pull-based: no native
// Channel goroutine, unbounded producer queue, or silent send-timeout drop is used.
type Subscription struct {
	private
	*subscriptionState
}
type subscriptionState struct {
	client    *Client
	native    *sdk.PubSub
	scope     invocation.Scope
	lifetime  context.Context
	mu        sync.Mutex
	ended     bool
	closeOnce sync.Once
	closeErr  error
}

func (subscription *Subscription) close() error {
	subscription.closeOnce.Do(func() { subscription.closeErr = subscription.native.Close() })
	return subscription.closeErr
}

// Subscribe runs one bounded, callback-owned subscription. Sending SUBSCRIBE is
// not a confirmed establishment; Receive returns native confirmations as events.
// Reconnects may lose messages and repeat confirmations. This is not a durable
// stream or lossless delivery contract. Callback completion closes the local
// connection; it does not acknowledge server-side removal of the subscription.
func (client *Client) Subscribe(ctx, cleanupCtx context.Context, id fault.Correlation, options SubscriptionOptions, run func(context.Context, *Subscription) error) (receipt *invocation.Receipt[Result], err error) {
	if client == nil || client.owner == nil || !client.owner.settings.AllowSubscriptions {
		return nil, failure(ErrAuthority, "subscription")
	}
	value := client.owner.settings
	if run == nil || cleanupCtx == nil || len(options.Channels) == 0 || len(options.Channels) > value.MaxArgs ||
		options.Mode != "channel" && options.Mode != "pattern" && options.Mode != "shard" {
		return nil, failure(ErrInput, "subscription")
	}
	total := len(options.RouteKey)
	if total > value.MaxRequestBytes {
		return nil, failure(ErrLimit, "subscription")
	}
	for _, channel := range options.Channels {
		if len(channel) > value.MaxRequestBytes-total-32 {
			return nil, failure(ErrLimit, "subscription")
		}
		total += len(channel) + 32
	}
	channels := append([]string(nil), options.Channels...)
	lifetime, cancel, err := (invocation.Budget{}).Context(ctx, invocation.Lifetime)
	if err != nil {
		return nil, err
	}
	defer cancel()
	call, err := client.begin(ctx, id, "subscription", invocation.Session, nil)
	if err != nil {
		return nil, err
	}
	receipt = call.Receipt()
	var native *sdk.PubSub
	var subscription *Subscription
	var primary error
	returned := false
	defer func() {
		_ = recover()
		if !returned {
			primary = failure(ErrState, "callback")
		}
		if subscription != nil {
			subscription.mu.Lock()
			subscription.ended = true
			defer subscription.mu.Unlock()
		}
		var cleanup error
		if subscription != nil {
			cleanup = subscription.close()
		} else if native != nil {
			cleanup = native.Close()
		}
		call.Complete(invocation.Outcome[Result]{Present: native != nil, Primary: primary, Cleanup: cleanup})
	}()
	work, stop, workErr := client.work(lifetime)
	if workErr != nil {
		primary = workErr
		returned = true
		return receipt, nil
	}
	backend := client.owner.native
	if ring, ok := backend.(*sdk.Ring); ok {
		key := channels[0]
		if options.Mode == "pattern" {
			key = options.RouteKey
		}
		node, nodeErr := ring.GetShardClientForKey(key)
		if nodeErr != nil {
			stop()
			primary = nativeFailure(work, nodeErr)
			returned = true
			return receipt, nil
		}
		if options.Mode != "pattern" {
			for _, channel := range channels {
				target, targetErr := ring.GetShardClientForKey(channel)
				if targetErr != nil || target != node {
					stop()
					primary = failure(ErrUnsupported, "ring-subscription", targetErr)
					returned = true
					return receipt, nil
				}
			}
		}
		backend = node
	}
	native = backend.Subscribe(work)
	_, _ = call.Attempt()
	switch options.Mode {
	case "channel":
		primary = nativeFailure(work, native.Subscribe(work, channels...))
	case "pattern":
		primary = nativeFailure(work, native.PSubscribe(work, channels...))
	case "shard":
		primary = nativeFailure(work, native.SSubscribe(work, channels...))
	}
	stop()
	if primary == nil {
		subscription = &Subscription{subscriptionState: &subscriptionState{client: client, native: native, scope: call.Scope(), lifetime: lifetime}}
		primary = run(lifetime, subscription)
	}
	returned = true
	return receipt, nil
}

// Receive reserves independent message evidence before reading the socket.
// Value is an Array: ["message", channel, pattern, payload],
// ["subscribe"|"psubscribe"|"ssubscribe", channel, count], or ["pong", payload].
// Empty payload is present text, not a missing reply.
func (subscription *Subscription) Receive(ctx context.Context, id fault.Correlation) (receipt *invocation.Receipt[Result], err error) {
	if subscription == nil || subscription.subscriptionState == nil || ctx == nil || !subscription.mu.TryLock() {
		return nil, failure(ErrState, "subscription")
	}
	defer subscription.mu.Unlock()
	if subscription.ended || subscription.lifetime.Err() != nil {
		return nil, failure(ErrState, "subscription")
	}
	life, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(subscription.lifetime, cancel)
	defer func() { stop(); cancel() }()
	if subscription.lifetime.Err() != nil {
		cancel()
	}
	call, err := subscription.client.begin(life, id, "receive", invocation.Stream, &subscription.scope)
	if err != nil {
		return nil, err
	}
	receipt = call.Receipt()
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			subscription.ended = true
			cleanup := subscription.close()
			call.Complete(invocation.Outcome[Result]{Primary: failure(ErrProtocol, "subscription-reply"), Cleanup: cleanup})
			err = nil
		}
	}()
	work, end, err := subscription.client.work(life)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		returned = true
		return call.Receipt(), nil
	}
	defer end()
	var raw any
	if work.Err() != nil {
		err = work.Err()
	} else {
		raw, err = subscription.native.ReceiveTimeout(work, subscription.client.owner.settings.Timeout)
	}
	result := Result{replies: []Reply{{state: Unknown, err: nativeFailure(work, err)}}}
	if err == nil {
		var data []any
		switch message := raw.(type) {
		case *sdk.Message:
			data = []any{"message", message.Channel, message.Pattern, message.Payload}
		case *sdk.Subscription:
			data = []any{message.Kind, message.Channel, int64(message.Count)}
		case *sdk.Pong:
			data = []any{"pong", message.Payload}
		default:
			err = failure(ErrProtocol, "message")
		}
		result.replies[0] = Reply{state: Replied, value: freeze(data), err: nativeFailure(work, err)}
	}
	call.Complete(invocation.Outcome[Result]{Present: err == nil, Value: result, Primary: nativeFailure(work, err)})
	returned = true
	return call.Receipt(), nil
}
