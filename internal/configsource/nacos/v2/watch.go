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
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
)

// Change is bounded invalidation metadata, not new application settings.
// Resync requires re-reading ALL selected keys; no durable ordering is promised.
type Change struct {
	private
	selected key
	resync   bool
	err      error
}

// Key is zero for a whole-set observation gap or initial resynchronization.
func (change Change) Key() KeyV1 {
	return KeyV1{Group: change.selected.Group, DataID: change.selected.DataID}
}

// Resync requires reacquiring every selected key, not only Key.
func (change Change) Resync() bool { return change.resync }

// Err retains an observation failure without asserting an application outcome.
func (change Change) Err() error { return change.err }

// Subscription owns a session/reconciliation loop. Next cancellation cancels
// only that wait. Close or client lifetime cancellation stops the subscription.
type Subscription struct {
	private
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	// queue owns only bounded invalidation metadata; resync preserves overflow
	// information after the discarded event is no longer retained.
	queue    []Change
	capacity int
	resync   bool
	// changed is replaced under mu after broadcasting, so multiple Next waiters
	// cannot strand already queued events behind one consumed wakeup token.
	changed chan struct{}
	raw     func([]*Document, int, error)
	// These fields belong only to the serialized native observation loop.
	failedDocument int
	reacquire      bool
}

// Watch starts bounded technical observation of the complete selected key set.
// Local return is not server readiness. Initial registration reports full resync;
// consumers must explicitly acquire required documents.
func (client *Client) Watch(ctx context.Context) (*Subscription, error) {
	return client.watch(ctx, nil)
}

// ObserveRaw reuses the single native registration/reconciliation/recovery loop.
// publish is an internal, bounded synchronous handoff, never a business callback.
// It receives a complete raw batch after validated registration or an error.
// The index identifies a failed selected-key query, or -1 when unknown/success.
func (client *Client) ObserveRaw(ctx context.Context, publish func([]*Document, int, error)) (*Subscription, error) {
	if publish == nil {
		return nil, fail(ErrInput, "observe-raw")
	}
	return client.watch(ctx, publish)
}

func (client *Client) watch(ctx context.Context, publish func([]*Document, int, error)) (*Subscription, error) {
	work, end, err := client.enter(ctx)
	if err != nil {
		return nil, err
	}
	if work.Err() != nil {
		err := fail(ErrClosed, "watch", work.Err(), context.Cause(work))
		end()
		return nil, err
	}
	client.mu.Lock()
	if client.subscriptions >= client.settings.Subscriptions {
		client.mu.Unlock()
		end()
		return nil, fail(ErrLimit, "watch")
	}
	client.subscriptions++
	client.mu.Unlock()
	budget, stop := context.WithTimeout(work, client.settings.Timeout)
	lease, err := client.access.Acquire(budget, reservationBytes)
	stop()
	if err != nil {
		client.mu.Lock()
		client.subscriptions--
		client.mu.Unlock()
		end()
		return nil, err
	}
	lifetime, cancel := context.WithCancel(work)
	subscription := &Subscription{cancel: cancel, done: make(chan struct{}), capacity: client.settings.Queue, changed: make(chan struct{}), raw: publish}
	go func() {
		defer end()
		defer lease.Release()
		defer close(subscription.done)
		defer cancel()
		defer func() { client.mu.Lock(); client.subscriptions--; client.mu.Unlock() }()
		index := int(client.preferred.Load() % uint64(len(client.settings.Servers)))
		for lifetime.Err() == nil {
			subscription.failedDocument = -1
			wake := make(chan struct{}, 1)
			current, err := client.newSession(lifetime, index, func(selected key) {
				subscription.publish(Change{selected: selected})
				select {
				case wake <- struct{}{}:
				default:
				}
			})
			if err == nil {
				err = subscription.observe(current, wake)
				current.close()
			}
			if lifetime.Err() != nil {
				return
			}
			subscription.publish(Change{resync: true, err: err})
			if subscription.raw != nil {
				subscription.raw(nil, subscription.failedDocument, err)
			}
			index = (index + 1) % len(client.settings.Servers)
			if !pause(lifetime, client.settings.Retry) {
				return
			}
		}
	}()
	return subscription, nil
}

// observe keeps hash/presence history only within one registered epoch. A new
// epoch starts without that baseline and reports a full-set resynchronization.
func (subscription *Subscription) observe(current *session, wake <-chan struct{}) error {
	first := true
	known := make(map[key]string)
	for current.ctx.Err() == nil {
		budget, stop := context.WithTimeout(current.ctx, current.owner.settings.Timeout)
		err := subscription.reconcile(budget, current, known)
		stop()
		if err != nil {
			return err
		}
		current.owner.preferred.Store(uint64(current.index))
		if first {
			subscription.publish(Change{resync: true})
			first = false
		}
		if !pause(current.ctx, current.owner.settings.Retry) {
			break
		}
		if subscription.raw != nil && subscription.reacquire {
			continue
		}
		timer := time.NewTimer(current.owner.settings.Reconcile)
		select {
		case <-wake:
			timer.Stop()
		case <-timer.C:
		case <-current.ctx.Done():
			timer.Stop()
		}
	}
	return current.failure("observe", current.ctx.Err())
}

// reconcile compares prior observations as well as the native listen result.
// Sending only freshly queried hashes would silently absorb a missed push.
// Empty hash denotes missing; a present empty document has a nonempty MD5.
func (subscription *Subscription) reconcile(ctx context.Context, current *session, known map[key]string) error {
	listen := request.NewConfigBatchListenRequest(len(current.owner.settings.Keys))
	listen.RequestId = strconv.FormatUint(current.owner.sequence.Add(1), 10)
	observed := make(map[key]string, len(current.owner.settings.Keys))
	var documents []*Document
	if subscription.raw != nil {
		var err error
		documents, err = current.readDocumentsMode(ctx, current.owner.settings.Keys, true, &subscription.failedDocument)
		if err != nil {
			return err
		}
	}
	for index, selected := range current.owner.settings.Keys {
		hash := ""
		if subscription.raw == nil {
			value, err := current.query(ctx, selected)
			if err == nil {
				hash = checksum(value.Content)
			} else if !errors.Is(err, ErrMissing) {
				return err
			}
		} else if !documents[index].missing {
			hash = checksum(documents[index].content)
		}
		observed[selected] = hash
		listen.ConfigListenContexts = append(listen.ConfigListenContexts, model.ConfigListenContext{
			Group: selected.Group, DataId: selected.DataID, Tenant: current.owner.settings.Namespace, Md5: hash})
	}
	value, err := current.call(ctx, listen, "ConfigChangeBatchListenResponse", true)
	if err != nil {
		return err
	}
	changes := value.(*response.ConfigChangeBatchListenResponse).ChangedConfigs
	if len(changes) > len(current.owner.settings.Keys) {
		return fail(ErrDecode, "listen-response")
	}
	seen := make(map[key]bool)
	for _, change := range changes {
		selected := key{change.Group, change.DataId}
		if change.Tenant != current.owner.settings.Namespace || !slices.Contains(current.owner.settings.Keys, selected) || seen[selected] {
			return fail(ErrDecode, "listen-scope")
		}
		seen[selected] = true
	}
	subscription.reacquire = len(changes) != 0
	// Commit the baseline only after every returned identity has been validated.
	for selected, hash := range observed {
		if previous, exists := known[selected]; exists && previous != hash {
			seen[selected] = true
		}
		known[selected] = hash
	}
	for selected := range seen {
		subscription.publish(Change{selected: selected})
	}
	if subscription.raw != nil {
		subscription.raw(documents, -1, nil)
	}
	return nil
}
func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

// publish never waits for consumer processing or stores document payloads. Queue
// overflow keeps a sticky full-set gap instead of pretending complete history.
func (subscription *Subscription) publish(change Change) {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if len(subscription.queue) == subscription.capacity {
		copy(subscription.queue, subscription.queue[1:])
		subscription.queue = subscription.queue[:len(subscription.queue)-1]
		subscription.resync = true
	}
	subscription.queue = append(subscription.queue, change)
	close(subscription.changed)
	subscription.changed = make(chan struct{})
}

// Next waits for one invalidation. Its context controls only this wait; queued
// changes cannot be obtained by a call starting after the subscription exits.
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
			change.resync = change.resync || subscription.resync
			subscription.resync = false
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

// Close cancels and joins local observation. Expiration retains ownership; retry
// Close to wait again. Local exit is not remote instantaneous unsubscribe proof.
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
