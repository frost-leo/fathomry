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

package viper

import (
	"context"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
)

// Change is an invalidation, not accepted settings. Index is the original path
// position or -1 for whole-set synchronization. Resync includes dropped events.
type Change struct {
	private
	index  int
	resync bool
	err    error
}

func (change Change) Index() int   { return change.index }
func (change Change) Resync() bool { return change.resync }
func (change Change) Err() error   { return change.err }

// Subscription owns a native file reconciler and its public operation guard.
type Subscription struct {
	private
	native  *native.Subscription
	call    *adapters.Call[Evidence]
	receipt *adapters.Receipt[Evidence]
}

// Watch uses the existing native worker, never another file poller. Its context
// owns the subscription lifetime; successful return is not source health.
func (client *Client) Watch(ctx context.Context, settings WatchSettings) (*Subscription, error) {
	if client == nil {
		return nil, fail(ErrInput, "watch")
	}
	var subscription *Subscription
	var setup error
	receipt, err := client.endpoint.Run(ctx, request("watch"), func(call *adapters.Call[Evidence]) {
		guard, err := call.Hold()
		if err != nil {
			setup = err
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: err})
			return
		}
		selected, err := native.Watch(call.Context(), native.WatchOptionsV1{Paths: settings.Paths, Interval: settings.Interval, QueueCapacity: settings.QueueCapacity})
		if err != nil {
			setup = translate(err, "watch")
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: setup})
			_ = guard.Release()
			return
		}
		subscription = &Subscription{native: selected, call: call, receipt: call.Receipt()}
		go func() {
			<-call.Context().Done()
			cleanup := translate(selected.Close(context.Background()), "close")
			_ = call.Resolve(adapters.Outcome[Evidence]{Value: Evidence{}, Present: true, Cleanup: cleanup})
			_ = guard.Release()
		}()
	})
	if err != nil {
		return nil, err
	}
	if subscription == nil {
		snapshot, _ := receipt.Snapshot()
		if setup != nil {
			return nil, setup
		}
		return nil, snapshot.Err()
	}
	return subscription, nil
}

// Next cancels only its wait. Read failures and overflow preserve native resync.
func (subscription *Subscription) Next(ctx context.Context) (Change, error) {
	if subscription == nil || subscription.native == nil {
		return Change{}, fail(ErrInput, "next")
	}
	value, err := subscription.native.Next(ctx)
	if err != nil {
		return Change{}, translate(err, "next")
	}
	return Change{index: value.Index(), resync: value.Resync(), err: translate(value.Err(), "observe")}, nil
}

// Close requests stop and joins actual cleanup. A wait timeout retains the owner;
// retry with a fresh context. It never closes the borrowed common runtime.
func (subscription *Subscription) Close(ctx context.Context) error {
	if subscription == nil || subscription.call == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	_ = subscription.call.Cancel(nil)
	value, err := subscription.receipt.WaitReleased(ctx)
	if err != nil {
		return fail(ErrState, "close", err)
	}
	return value.Err()
}
