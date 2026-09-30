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
	"crypto/sha256"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// WatchOptionsV1 selects explicit local files and bounded content reconciliation.
// Interval defaults to 1 s and accepts 10 ms..5 min. QueueCapacity defaults to 16,
// accepts 1..64. Paths may initially be absent. This uses periodic bounded reads,
// not the native unjoinable WatchConfig worker; it does not observe environment.
type WatchOptionsV1 struct {
	private
	Paths         []string
	Interval      time.Duration
	QueueCapacity int
}

// Change is a file invalidation, not a validated/applied configuration. Index is
// -1 for a full resync; otherwise it identifies the original Paths entry.
type Change struct {
	private
	index  int
	resync bool
	err    error
}

func (change Change) Index() int   { return change.index }
func (change Change) Resync() bool { return change.resync }
func (change Change) Err() error   { return change.err }

// Subscription owns one reconciliation worker. Next cancellation affects only
// its wait; Close cancels and joins the worker. Do not copy runtime structs.
type Subscription struct {
	private
	cancel   context.CancelFunc
	done     chan struct{}
	mu       sync.Mutex
	queue    []Change
	capacity int
	gap      bool
	changed  chan struct{}
}
type fingerprint struct {
	missing bool
	sum     [32]byte
	failed  bool
}

// Watch creates an owned polling subscription. Initial observation publishes a
// full resync. Callers reload/validate explicitly. Content hashing detects atomic
// replacement/deletion and target changes behind symlinks, but cannot observe every
// transient intermediate write. Read errors and overflow request full resync.
func Watch(ctx context.Context, options WatchOptionsV1) (*Subscription, error) {
	if ctx == nil || len(options.Paths) == 0 || len(options.Paths) > MaxSources {
		return nil, fail(ErrInput, "watch")
	}
	interval := options.Interval
	if interval == 0 {
		interval = time.Second
	}
	capacity := options.QueueCapacity
	if capacity == 0 {
		capacity = 16
	}
	if interval < 10*time.Millisecond || interval > 5*time.Minute || capacity < 1 || capacity > 64 {
		return nil, fail(ErrInput, "watch")
	}
	paths := make([]string, len(options.Paths))
	seen := map[string]bool{}
	bytes := 0
	for index, path := range options.Paths {
		if !validPath(path) || !utf8.ValidString(path) || strings.ContainsRune(path, 0) || seen[path] {
			return nil, fail(ErrInput, "watch")
		}
		seen[path] = true
		bytes += len(path)
		if bytes > MaxBootstrapBytes {
			return nil, fail(ErrLimit, "watch")
		}
		paths[index] = strings.Clone(path)
	}
	if ctx.Err() != nil {
		return nil, fail(ErrClosed, "watch", ctx.Err(), context.Cause(ctx))
	}
	lifetime, cancel := context.WithCancel(ctx)
	subscription := &Subscription{cancel: cancel, done: make(chan struct{}), capacity: capacity, changed: make(chan struct{})}
	go func() {
		defer close(subscription.done)
		defer cancel()
		timer := time.NewTicker(interval)
		defer timer.Stop()
		previous := make([]fingerprint, len(paths))
		initial := true
		for {
			remaining := MaxTotalBytes
			for index, path := range paths {
				if lifetime.Err() != nil {
					return
				}
				raw, missing, err := RawFile(lifetime, path, min(MaxDocumentBytes, remaining))
				if err == nil {
					remaining -= len(raw)
				}
				current := fingerprint{missing: missing, sum: sha256.Sum256(raw), failed: err != nil}
				if err != nil {
					subscription.publish(Change{index: index, resync: true, err: err})
				} else if !initial && current != previous[index] {
					subscription.publish(Change{index: index})
				}
				previous[index] = current
			}
			if initial {
				subscription.publish(Change{index: -1, resync: true})
				initial = false
			}
			select {
			case <-lifetime.Done():
				return
			case <-timer.C:
			}
		}
	}()
	return subscription, nil
}
func (subscription *Subscription) publish(change Change) {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if len(subscription.queue) == subscription.capacity {
		copy(subscription.queue, subscription.queue[1:])
		subscription.queue = subscription.queue[:len(subscription.queue)-1]
		subscription.gap = true
	}
	subscription.queue = append(subscription.queue, change)
	close(subscription.changed)
	subscription.changed = make(chan struct{})
}

// Next returns one bounded invalidation. A resync flag covers any dropped events;
// it is not a complete change log or a guarantee of a common-time file snapshot.
func (subscription *Subscription) Next(ctx context.Context) (Change, error) {
	if subscription == nil || subscription.cancel == nil || ctx == nil {
		return Change{}, fail(ErrInput, "next")
	}
	for {
		if ctx.Err() != nil {
			return Change{}, fail(ErrRead, "next", ctx.Err(), context.Cause(ctx))
		}
		subscription.mu.Lock()
		select {
		case <-subscription.done:
			subscription.mu.Unlock()
			return Change{}, fail(ErrClosed, "next")
		default:
		}
		if len(subscription.queue) > 0 {
			change := subscription.queue[0]
			copy(subscription.queue, subscription.queue[1:])
			subscription.queue[len(subscription.queue)-1] = Change{}
			subscription.queue = subscription.queue[:len(subscription.queue)-1]
			change.resync = change.resync || subscription.gap
			subscription.gap = false
			subscription.mu.Unlock()
			return change, nil
		}
		changed := subscription.changed
		subscription.mu.Unlock()
		select {
		case <-changed:
		case <-subscription.done:
			return Change{}, fail(ErrClosed, "next")
		case <-ctx.Done():
			return Change{}, fail(ErrRead, "next", ctx.Err(), context.Cause(ctx))
		}
	}
}

// Close retains the same owner after timeout. It cannot interrupt a filesystem
// call already blocked in the OS; call again to confirm the worker actually exited.
func (subscription *Subscription) Close(ctx context.Context) error {
	if subscription == nil || subscription.cancel == nil || ctx == nil {
		return fail(ErrInput, "close-watch")
	}
	subscription.cancel()
	select {
	case <-subscription.done:
		return nil
	case <-ctx.Done():
		return fail(ErrState, "close-watch", ctx.Err(), context.Cause(ctx))
	}
}
