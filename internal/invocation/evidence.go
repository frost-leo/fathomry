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

package invocation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

// runtimeValue prevents ordinary presentation and accidental JSON reconstruction
// from treating runtime handles, native causes or capability data as a wire DTO.
type runtimeValue struct{}

func (runtimeValue) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, "operation[restricted]")
}
func (runtimeValue) LogValue() slog.Value { return slog.StringValue("operation[restricted]") }
func (runtimeValue) MarshalJSON() ([]byte, error) {
	return nil, errors.New("operation: runtime serialization is unsupported")
}
func (*runtimeValue) UnmarshalJSON([]byte) error {
	return errors.New("operation: runtime reconstruction is unsupported")
}

// Inbox is the required in-process evidence boundary, independently owned by
// composition/framework code, NOT by a business handler or an exporter. It owns
// a bounded number of outstanding deliveries and declared bytes. Reservation
// precedes SDK work; completion never calls a receiver or waits for queue space.
// No goroutine is started. Consumers own their Next contexts and processing loops.
// This is not a durable ledger: process loss, replay, late/duplicate reconciliation
// and acknowledgement-before-business-advancement require a separate protocol.
type Inbox[T any] struct {
	runtimeValue
	mu       sync.Mutex
	queue    chan *DeliveryRecord[T]
	maxBytes int64
	count    int
	bytes    int64
}

// InboxUsage includes queued, active and already-received deliveries until their
// explicit release. Removing a delivery from the queue does not free its capacity.
type InboxUsage struct {
	Outstanding   int
	ReservedBytes int64
}

// NewInbox requires positive capacities. Bytes bound Provider-declared evidence
// reservations, not arbitrary native-error graphs or unmeasured process memory.
// Providers must reject unsupported payload sizes before SDK execution.
func NewInbox[T any](capacity int, bytes int64) (*Inbox[T], error) {
	if capacity <= 0 || bytes <= 0 {
		return nil, ErrEvidence.New(fault.Context{})
	}
	return &Inbox[T]{queue: make(chan *DeliveryRecord[T], capacity), maxBytes: bytes}, nil
}

func (inbox *Inbox[T]) reserve(bytes int64) error {
	if inbox == nil || inbox.queue == nil || bytes <= 0 {
		return ErrEvidence.New(fault.Context{})
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if inbox.count == cap(inbox.queue) || bytes > inbox.maxBytes-inbox.bytes {
		return ErrEvidence.New(fault.Context{})
	}
	inbox.count++
	inbox.bytes += bytes
	return nil
}

func (inbox *Inbox[T]) unreserve(bytes int64) {
	inbox.mu.Lock()
	inbox.count--
	inbox.bytes -= bytes
	inbox.mu.Unlock()
}

func (inbox *Inbox[T]) publish(receipt *Receipt[T], bytes int64) {
	// Every queued record already owns a slot, including concurrent reservations.
	// Received-but-unreleased records also consume slots, so this send cannot wait.
	inbox.queue <- &DeliveryRecord[T]{receipt: receipt, inbox: inbox, bytes: bytes}
}

// Next transfers an accepted operation's live receipt to the evidence receiver;
// it can arrive BEFORE technical completion. The receiver retains unresolved
// records, inspects missing facts explicitly, and releases only after appropriate
// handling. There is no implicit acknowledgement, retry, drain worker or close.
func (inbox *Inbox[T]) Next(ctx context.Context) (*DeliveryRecord[T], error) {
	if inbox == nil || inbox.queue == nil || ctx == nil {
		return nil, ErrEvidence.New(fault.Context{})
	}
	if err := ctx.Err(); err != nil {
		return nil, ErrWait.New(fault.Context{}, err, context.Cause(ctx))
	}
	select {
	case delivery := <-inbox.queue:
		return delivery, nil
	case <-ctx.Done():
		return nil, ErrWait.New(fault.Context{}, ctx.Err(), context.Cause(ctx))
	}
}

func (inbox *Inbox[T]) Usage() InboxUsage {
	if inbox == nil {
		return InboxUsage{}
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	return InboxUsage{Outstanding: inbox.count, ReservedBytes: inbox.bytes}
}

// DeliveryRecord is the receiver's local evidence responsibility. Its Receipt is
// the same read-only outcome offered to the integration caller. Do not copy this owner;
// use its pointer. Release relinquishes only inbox memory accounting, not a
// durable acknowledgement, external effect, or Item/Run disposition.
type DeliveryRecord[T any] struct {
	runtimeValue
	mu       sync.Mutex
	receipt  *Receipt[T]
	inbox    *Inbox[T]
	bytes    int64
	released bool
}

func (delivery *DeliveryRecord[T]) Receipt() *Receipt[T] {
	if delivery == nil || delivery.receipt == nil {
		return nil
	}
	return delivery.receipt
}

// Release refuses while any local work in the delivered subtree remains. A failed
// recording attempt leaves the delivery owned and capacity charged. Call only
// after the framework's necessary evidence handling; nil is not proof of storage.
// Repeated calls are idempotent. Retained caller references are caller-owned memory.
func (delivery *DeliveryRecord[T]) Release() error {
	if delivery == nil || delivery.receipt == nil || delivery.inbox == nil {
		return ErrEvidence.New(fault.Context{})
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if delivery.released {
		return nil
	}
	select {
	case <-delivery.receipt.state.scope.lease.Done():
	default:
		return ErrPending.New(delivery.receipt.state.location)
	}
	delivery.released = true
	delivery.inbox.unreserve(delivery.bytes)
	return nil
}

// Event is deliberately low-cardinality and payload-free. No Run/Item/source/
// account/name, arbitrary labels, native text, error graph or data value is sent.
// Duration runs from admission through final outcome/cleanup, excluding the queue;
// it is not SDK latency or total resource occupancy. Nested identifies operations
// sharing a parent's reservation. Attempts are separate from logical completion.
type Event struct {
	Shape         Shape
	Nested        bool
	Duration      time.Duration
	Failed        bool
	CleanupFailed bool
	Attempts      Attempts
}

// Observer is an optional bounded lossy diagnostic queue. It owns no exporter,
// worker or goroutine. Failed/slow export cannot hold SDK completion or resource
// cleanup. Its fixed projection is NOT the required evidence channel.
type Observer struct {
	runtimeValue
	queue chan Event
}

// NewObserver requires positive event capacity. A nil Observer disables diagnostics.
func NewObserver(capacity int) (*Observer, error) {
	if capacity <= 0 {
		return nil, ErrObservation.New(fault.Context{})
	}
	return &Observer{queue: make(chan Event, capacity)}, nil
}

func (observer *Observer) offer(event Event, suppressed bool) Observation {
	if suppressed {
		return ObservationSuppressed
	}
	if observer == nil || observer.queue == nil {
		return ObservationDisabled
	}
	select {
	case observer.queue <- event:
		return ObservationQueued
	default:
		return ObservationDropped
	}
}

type observationKey struct{}

func observationSuppressed(ctx context.Context) bool {
	suppressed, _ := ctx.Value(observationKey{}).(bool)
	return suppressed
}

// ExportOne runs a consumer's exporter on the CONSUMER's stack. It supplies a
// context suppressing diagnostic recursion in Begin, including through different
// Observers. Propagate that context through exporter I/O. Replacing it with a
// background context bypasses the integration contract. Export errors are returned
// directly, not exported again; the original operation result is untouched.
// Panics are contained as diagnostic failure without retaining arbitrary panic
// values. A noncooperative exporter may block this caller, never SDK completion.
func (observer *Observer) ExportOne(ctx context.Context, export func(context.Context, Event) error) (err error) {
	if observer == nil || observer.queue == nil || ctx == nil || export == nil {
		return ErrObservation.New(fault.Context{})
	}
	if ctx.Err() != nil {
		return ErrObservation.New(fault.Context{}, ctx.Err(), context.Cause(ctx))
	}
	var event Event
	select {
	case event = <-observer.queue:
	case <-ctx.Done():
		return ErrObservation.New(fault.Context{}, ctx.Err(), context.Cause(ctx))
	}
	returned := false
	defer func() {
		_ = recover()
		if !returned {
			err = ErrObservation.New(fault.Context{})
		}
	}()
	exportErr := export(context.WithValue(ctx, observationKey{}, true), event)
	returned = true
	if exportErr != nil {
		return ErrObservation.New(fault.Context{}, exportErr)
	}
	return nil
}
