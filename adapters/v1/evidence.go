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

package adapters

import (
	"context"
	"io"
	"sync"
)

// Inbox owns reserved, queued and claimed evidence until explicit acknowledgement.
// It starts no goroutine and does not infer durable storage from acknowledgement.
type Inbox[T any] struct {
	state *inboxState[T]
	_     typeIdentity[T]
}
type inboxState[T any] struct {
	mu          sync.Mutex
	options     EvidenceOptions
	closed      bool
	outstanding int
	bytes       int64
	pending     int
	queue       []*evidenceEntry[T]
	entries     map[*evidenceEntry[T]]struct{}
	changed     chan struct{}
}
type evidenceEntry[T any] struct {
	inbox    *inboxState[T]
	bytes    int64
	result   *resultState[T]
	claim    *deliveryState[T]
	reserved bool
}
type deliveryState[T any] struct {
	mu     sync.Mutex
	entry  *evidenceEntry[T]
	inbox  *inboxState[T]
	result *resultState[T]
}

// EvidenceStatus counts local custody, not durable acceptance or remote work.
type EvidenceStatus struct {
	Outstanding int
	Queued      int
	Claimed     int
	Reserved    int
	Bytes       int64
	Closed      bool
}

// Delivery is one exclusive claim. Copies share authority; after Ack or Retry,
// all copies are invalid. Keep the Receipt separately for historical observation.
type Delivery[T any] struct {
	state *deliveryState[T]
	_     typeIdentity[T]
}

// NewInbox defaults to 64 records / 16 MiB. Counts include claimed records and
// admission reservations; a missing receiver never silently drops required facts.
func NewInbox[T any](options EvidenceOptions) (*Inbox[T], error) {
	if options.Capacity == 0 {
		options.Capacity = 64
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = 16 << 20
	}
	if options.Capacity < 1 || options.Capacity > 65536 || options.MaxBytes < 1 || options.MaxBytes > 1<<40 {
		return nil, failureOf(ErrOptions, "inbox", "", Details{})
	}
	return &Inbox[T]{state: &inboxState[T]{options: options, entries: make(map[*evidenceEntry[T]]struct{}), changed: make(chan struct{})}}, nil
}
func (state *inboxState[T]) notifyLocked() { close(state.changed); state.changed = make(chan struct{}) }
func (inbox *Inbox[T]) reserve(bytes int64) (*evidenceEntry[T], error) {
	if inbox == nil || inbox.state == nil {
		return nil, failureOf(ErrHandle, "reserve", "", Details{})
	}
	state := inbox.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return nil, failureOf(ErrClosed, "reserve", "", Details{})
	}
	if bytes < 1 || state.outstanding >= state.options.Capacity || bytes > state.options.MaxBytes-state.bytes {
		return nil, failureOf(ErrEvidence, "reserve", "", Details{})
	}
	entry := &evidenceEntry[T]{inbox: state, bytes: bytes, reserved: true}
	state.outstanding++
	state.pending++
	state.bytes += bytes
	state.entries[entry] = struct{}{}
	state.notifyLocked()
	return entry, nil
}
func (entry *evidenceEntry[T]) abort() {
	state := entry.inbox
	state.mu.Lock()
	defer state.mu.Unlock()
	if !entry.reserved {
		return
	}
	entry.reserved = false
	state.pending--
	state.outstanding--
	state.bytes -= entry.bytes
	delete(state.entries, entry)
	state.notifyLocked()
}
func (entry *evidenceEntry[T]) commit(result *resultState[T]) {
	state := entry.inbox
	state.mu.Lock()
	defer state.mu.Unlock()
	entry.result = result
	entry.reserved = false
	state.pending--
	state.queue = append(state.queue, entry)
	state.notifyLocked()
}

// Next claims the next record; its result may still have live native work.
// Context cancellation affects only this wait. EOF requires a sealed queue with
// no pending reservations; other consumers may still own already-claimed records.
func (inbox *Inbox[T]) Next(ctx context.Context) (Delivery[T], error) {
	return inbox.next(ctx, false)
}

// NextReleased claims the oldest actually released queued record, skipping live
// owners without relinquishing their custody. Release wakes existing waiters.
// This avoids finite evidence being blocked behind a long-lived Open/Watch.
// Existing Next remains admission-order; neither method infers sink success.
func (inbox *Inbox[T]) NextReleased(ctx context.Context) (Delivery[T], error) {
	return inbox.next(ctx, true)
}

func (inbox *Inbox[T]) next(ctx context.Context, releasedOnly bool) (Delivery[T], error) {
	if inbox == nil || inbox.state == nil {
		return Delivery[T]{}, failureOf(ErrHandle, "next", "", Details{})
	}
	if ctx == nil {
		return Delivery[T]{}, failureOf(ErrOptions, "next", "", Details{})
	}
	state := inbox.state
	for {
		if ctx.Err() != nil {
			return Delivery[T]{}, failureOf(ErrWait, "next", "", Details{}, ctx.Err(), context.Cause(ctx))
		}
		state.mu.Lock()
		selected := -1
		for index, entry := range state.queue {
			ready := !releasedOnly
			if releasedOnly {
				select {
				case <-entry.result.node.done:
					ready = true
				default:
				}
			}
			if ready {
				selected = index
				break
			}
		}
		if selected >= 0 {
			entry := state.queue[selected]
			copy(state.queue[selected:], state.queue[selected+1:])
			state.queue[len(state.queue)-1] = nil
			state.queue = state.queue[:len(state.queue)-1]
			claim := &deliveryState[T]{entry: entry, inbox: state, result: entry.result}
			entry.claim = claim
			state.mu.Unlock()
			return Delivery[T]{state: claim}, nil
		}
		if state.closed && state.pending == 0 && len(state.queue) == 0 {
			state.mu.Unlock()
			return Delivery[T]{}, failureOf(ErrClosed, "next", "", Details{}, io.EOF)
		}
		changed := state.changed
		state.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return Delivery[T]{}, failureOf(ErrWait, "next", "", Details{}, ctx.Err(), context.Cause(ctx))
		}
	}
}

// Seal rejects new reservations but neither revokes accepted reservations nor
// acknowledges/discards records. It does not stop any operation runtime.
func (inbox *Inbox[T]) Seal() error {
	if inbox == nil || inbox.state == nil {
		return failureOf(ErrHandle, "seal", "", Details{})
	}
	state := inbox.state
	state.mu.Lock()
	defer state.mu.Unlock()
	state.closed = true
	state.notifyLocked()
	return nil
}

// Inspect is a detached custody observation, not a service-health query.
func (inbox *Inbox[T]) Inspect() (EvidenceStatus, error) {
	if inbox == nil || inbox.state == nil {
		return EvidenceStatus{}, failureOf(ErrHandle, "inspect", "", Details{})
	}
	state := inbox.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return EvidenceStatus{state.outstanding, len(state.queue), state.outstanding - state.pending - len(state.queue), state.pending, state.bytes, state.closed}, nil
}

// Receipt returns a read-only handle while this claim is valid.
func (delivery Delivery[T]) Receipt() (*Receipt[T], error) {
	if delivery.state == nil {
		return nil, failureOf(ErrHandle, "receipt", "", Details{})
	}
	state := delivery.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.entry == nil {
		return nil, failureOf(ErrReleased, "receipt", "", Details{})
	}
	return &Receipt[T]{state: state.result}, nil
}

// Ack relinquishes local evidence custody only after actual work has released.
// A successful sink write/required business handling must precede this declaration.
func (delivery Delivery[T]) Ack() error { return delivery.end(false) }

// Retry requeues these same facts without freeing their reservation or invoking
// the producer again. Old/copy claims cannot acknowledge the new consumer's claim.
func (delivery Delivery[T]) Retry() error { return delivery.end(true) }
func (delivery Delivery[T]) end(retry bool) error {
	if delivery.state == nil {
		return failureOf(ErrHandle, "delivery", "", Details{})
	}
	claim := delivery.state
	claim.mu.Lock()
	defer claim.mu.Unlock()
	if claim.entry == nil {
		return failureOf(ErrReleased, "delivery", "", Details{})
	}
	if !retry {
		select {
		case <-claim.result.node.done:
		default:
			return failureOf(ErrPending, "ack", "", Details{Pending: true})
		}
	}
	state, entry := claim.inbox, claim.entry
	state.mu.Lock()
	if entry.claim != claim {
		state.mu.Unlock()
		return failureOf(ErrReleased, "delivery", "", Details{})
	}
	entry.claim = nil
	if retry {
		state.queue = append(state.queue, entry)
	} else {
		delete(state.entries, entry)
		state.outstanding--
		state.bytes -= entry.bytes
		entry.result = nil
	}
	state.notifyLocked()
	state.mu.Unlock()
	claim.entry = nil
	claim.inbox = nil
	claim.result = nil
	return nil
}

// DeliverOne waits for actual release then invokes a bounded, non-panicking sink
// outside all locks. Waiting/sink failure requeues the same record and returns;
// there is no retry loop, implicit SDK resend, or inferred durable acknowledgement.
func (inbox *Inbox[T]) DeliverOne(ctx context.Context, sink func(context.Context, Snapshot[T]) error) error {
	if sink == nil {
		return failureOf(ErrOptions, "deliver", "", Details{})
	}
	delivery, err := inbox.Next(ctx)
	if err != nil {
		return err
	}
	receipt, _ := delivery.Receipt()
	value, err := receipt.WaitReleased(ctx)
	if err != nil {
		_ = delivery.Retry()
		return err
	}
	if ctx.Err() != nil {
		_ = delivery.Retry()
		return failureOf(ErrWait, "deliver", "", Details{}, ctx.Err(), context.Cause(ctx))
	}
	if err = sink(ctx, value); err != nil {
		_ = delivery.Retry()
		return failureOf(ErrEvidence, "deliver", "", Details{}, err)
	}
	return delivery.Ack()
}
