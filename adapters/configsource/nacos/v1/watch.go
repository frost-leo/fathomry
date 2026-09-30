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

package nacos

import (
	"context"
	"math"
	"slices"
	"sync"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

// Change is a native invalidation. A zero Key denotes whole-set synchronization.
// Resync covers registration/recovery/overflow gaps, not accepted configuration.
type Change struct {
	private
	key    Key
	resync bool
	err    error
}

func (value Change) Key() Key     { return value.key }
func (value Change) Resync() bool { return value.resync }
func (value Change) Err() error   { return value.err }

// Batch is one already-acquired complete observation or an acquisition failure.
// Sequence is local delivery order, not remote revision. Gap means observations
// were discarded before this delivery; no complete history is promised.
type Batch struct {
	private
	data     []*native.Document
	failed   int
	err      error
	sequence uint64
	gap      bool
}

func (value Batch) DocumentsCopy() []*Document { return documents(value.data) }
func (value Batch) FailedIndex() int {
	if value.sequence == 0 {
		return -1
	}
	return value.failed
}
func (value Batch) Err() error {
	if value.sequence == 0 {
		return fail(ErrInput, "batch")
	}
	return value.err
}
func (value Batch) Sequence() uint64 { return value.sequence }
func (value Batch) Gap() bool        { return value.gap }

// Subscription owns native invalidation observation and one public operation.
type Subscription struct {
	private
	state *subscriptionState
}

// Observation owns a bounded complete-batch handoff, not an extra native poller.
type Observation struct {
	private
	state *subscriptionState
}
type subscriptionState struct {
	native   *native.Subscription
	owner    *ownerState
	call     *adapters.Call[Evidence]
	receipt  *adapters.Receipt[Evidence]
	lifetime context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	queue    []Batch
	capacity int
	gap      bool
	sequence uint64
	changed  chan struct{}
	terminal error
}

// Watch observes the nonempty default key set; return does not prove registration.
func (client *Client) Watch(ctx context.Context) (*Subscription, error) {
	return client.watch(ctx, nil, false)
}

// WatchKeys freezes exactly one explicit permitted selection without changing defaults.
func (client *Client) WatchKeys(ctx context.Context, keys []Key) (*Subscription, error) {
	return client.watch(ctx, keys, true)
}
func (client *Client) watch(ctx context.Context, keys []Key, explicit bool) (*Subscription, error) {
	state, err := client.observe(ctx, keys, explicit, false, ObserveOptions{})
	if err != nil {
		return nil, err
	}
	return &Subscription{state: state}, nil
}

// ObserveRaw reuses native registration, acquisition and recovery. Batches contain
// its already-read documents; receiving or redelivering never causes another read.
func (client *Client) ObserveRaw(ctx context.Context, options ObserveOptions) (*Observation, error) {
	return client.observeRaw(ctx, nil, false, options)
}

// ObserveRawKeys observes one frozen explicit key set under the same key policy.
func (client *Client) ObserveRawKeys(ctx context.Context, keys []Key, options ObserveOptions) (*Observation, error) {
	return client.observeRaw(ctx, keys, true, options)
}
func (client *Client) observeRaw(ctx context.Context, keys []Key, explicit bool, options ObserveOptions) (*Observation, error) {
	state, err := client.observe(ctx, keys, explicit, true, options)
	if err != nil {
		return nil, err
	}
	return &Observation{state: state}, nil
}
func (client *Client) observe(ctx context.Context, keys []Key, explicit, raw bool, options ObserveOptions) (*subscriptionState, error) {
	capacity := options.QueueCapacity
	if capacity == 0 {
		capacity = 2
	}
	if capacity < 1 || capacity > 16 || len(keys) > MaxKeys || explicit && len(keys) == 0 {
		return nil, fail(ErrInput, "observe")
	}
	selected := make([]native.KeyV1, len(keys))
	for index, key := range keys {
		selected[index] = nativeKey(key)
	}
	workBytes := int64(2 * MaxWireBytes)
	operation := "watch"
	if raw {
		workBytes += int64(capacity) * MaxTotalBytes
		operation = "observe_raw"
	}
	var result *subscriptionState
	var setup error
	receipt, err := client.dispatch(ctx, operation, workBytes, func(call *adapters.Call[Evidence], owner *ownerState) {
		guard, err := call.Hold()
		if err != nil {
			setup = err
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: err})
			return
		}
		lifetime, cancel := context.WithCancel(call.Context())
		stop := context.AfterFunc(owner.call.Context(), cancel)
		if owner.call.Context().Err() != nil {
			cancel()
		}
		state := &subscriptionState{owner: owner, call: call, receipt: call.Receipt(), lifetime: lifetime, cancel: cancel, capacity: capacity, changed: make(chan struct{})}
		var nativeSubscription *native.Subscription
		if raw {
			if explicit {
				nativeSubscription, err = owner.native.ObserveRawKeys(lifetime, selected, state.publish)
			} else {
				nativeSubscription, err = owner.native.ObserveRaw(lifetime, state.publish)
			}
		} else {
			if explicit {
				nativeSubscription, err = owner.native.WatchKeys(lifetime, selected)
			} else {
				nativeSubscription, err = owner.native.Watch(lifetime)
			}
		}
		if err != nil {
			stop()
			cancel()
			setup = translate(err, operation)
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: setup})
			_ = guard.Release()
			return
		}
		state.native = nativeSubscription
		result = state
		go func() {
			<-lifetime.Done()
			cleanup := translate(nativeSubscription.Close(context.Background()), "close")
			stop()
			cancel()
			state.mu.Lock()
			clear(state.queue)
			state.queue = nil
			terminal := state.terminal
			state.mu.Unlock()
			_ = call.Resolve(adapters.Outcome[Evidence]{Value: Evidence{FailedIndex: -1}, Present: true, Primary: terminal, Cleanup: cleanup})
			_ = guard.Release()
		}()
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		if setup != nil {
			return nil, setup
		}
		value, _ := receipt.Snapshot()
		return nil, value.Err()
	}
	return result, nil
}
func (state *subscriptionState) publish(values []*native.Document, failed int, err error) {
	batch := Batch{failed: failed, err: translate(err, "observe")}
	if err == nil {
		batch.data = slices.Clone(values)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.lifetime.Err() != nil {
		return
	}
	if state.sequence == math.MaxUint64 {
		state.terminal = fail(ErrLimit, "observation_sequence")
		state.cancel()
		return
	}
	state.sequence++
	batch.sequence = state.sequence
	if len(state.queue) == state.capacity {
		copy(state.queue, state.queue[1:])
		state.queue[len(state.queue)-1] = Batch{}
		state.queue = state.queue[:len(state.queue)-1]
		state.gap = true
	}
	state.queue = append(state.queue, batch)
	close(state.changed)
	state.changed = make(chan struct{})
}

// Next ends only this wait on cancellation. It preserves native failure/resync.
func (subscription *Subscription) Next(ctx context.Context) (Change, error) {
	if subscription == nil || subscription.state == nil {
		return Change{}, fail(ErrInput, "next")
	}
	value, err := subscription.state.native.Next(ctx)
	if err != nil {
		return Change{}, translate(err, "next")
	}
	return Change{key: publicKey(value.Key()), resync: value.Resync(), err: translate(value.Err(), "observe")}, nil
}

// Next returns one complete batch/error. Failed acquisition is Batch.Err, separate
// from a failed wait. Caller changes to a returned document wrapper cannot alter
// another copy of this batch or the subscription's queued observations.
func (observation *Observation) Next(ctx context.Context) (Batch, error) {
	if observation == nil || observation.state == nil || ctx == nil {
		return Batch{}, fail(ErrInput, "next")
	}
	state := observation.state
	for {
		if ctx.Err() != nil {
			return Batch{}, fail(ErrRead, "next", ctx.Err(), context.Cause(ctx))
		}
		state.mu.Lock()
		if state.lifetime.Err() != nil || state.owner.call.Context().Err() != nil {
			state.mu.Unlock()
			return Batch{}, fail(ErrClosed, "next", state.lifetime.Err(), context.Cause(state.lifetime), state.owner.call.Context().Err(), context.Cause(state.owner.call.Context()))
		}
		if len(state.queue) > 0 {
			value := state.queue[0]
			copy(state.queue, state.queue[1:])
			state.queue[len(state.queue)-1] = Batch{}
			state.queue = state.queue[:len(state.queue)-1]
			value.gap = value.gap || state.gap
			state.gap = false
			state.mu.Unlock()
			return value, nil
		}
		changed := state.changed
		state.mu.Unlock()
		select {
		case <-changed:
		case <-state.lifetime.Done():
			return Batch{}, fail(ErrClosed, "next", state.lifetime.Err(), context.Cause(state.lifetime), state.owner.call.Context().Err(), context.Cause(state.owner.call.Context()))
		case <-ctx.Done():
			return Batch{}, fail(ErrRead, "next", ctx.Err(), context.Cause(ctx))
		}
	}
}

// Close joins local native observation. Expiration does not release the resource
// generation, guard or native owner; retry with a fresh waiting context.
func (subscription *Subscription) Close(ctx context.Context) error {
	if subscription == nil {
		return fail(ErrInput, "close")
	}
	return closeSubscription(ctx, subscription.state)
}
func (observation *Observation) Close(ctx context.Context) error {
	if observation == nil {
		return fail(ErrInput, "close")
	}
	return closeSubscription(ctx, observation.state)
}
func closeSubscription(ctx context.Context, state *subscriptionState) error {
	if state == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	_ = state.call.Cancel(nil)
	value, err := state.receipt.WaitReleased(ctx)
	if err != nil {
		return fail(ErrState, "close", err)
	}
	return value.Err()
}
