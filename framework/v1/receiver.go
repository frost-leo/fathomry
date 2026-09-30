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

package framework

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

// ReceiverOptions bounds failed-delivery pacing. Zero selects 100ms;
// valid 1ms..1min. There is one sink invocation at a time, never a worker per fact.
type ReceiverOptions struct {
	RetryDelay time.Duration `json:"retry_delay_ns"`
}

// ReceiverStatus is local custody progress, not business/service completion.
// Counters saturate at MaxUint64. LastError retains the latest historical failure.
type ReceiverStatus struct {
	Delivered uint64
	Failures  uint64
	LastError error
	Stopped   bool
}

// Receiver owns one required-evidence delivery loop. The Inbox remains caller
// owned; Close does not discard its records. The sink must be bounded, concurrent-
// lifetime-correct, non-panicking and tolerate redelivery after unknown reception.
type Receiver[T any] struct {
	private
	state *receiverState[T]
	_     typeIdentity[T]
}
type typeIdentity[T any] [0]func() T
type receiverState[T any] struct {
	inbox  *adapters.Inbox[T]
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	status ReceiverStatus
}

// StartReceiver selects only released records using the existing Inbox custody.
// Waiting for live Open/Watch records cannot starve completed finite operations.
func StartReceiver[T any](ctx context.Context, inbox *adapters.Inbox[T], options ReceiverOptions, sink func(context.Context, adapters.Snapshot[T]) error) (*Receiver[T], error) {
	if ctx == nil || sink == nil {
		return nil, fail(ErrOptions, "receive")
	}
	if _, err := inbox.Inspect(); err != nil {
		return nil, err
	}
	if options.RetryDelay == 0 {
		options.RetryDelay = 100 * time.Millisecond
	}
	if options.RetryDelay < time.Millisecond || options.RetryDelay > time.Minute {
		return nil, fail(ErrOptions, "receive")
	}
	if ctx.Err() != nil {
		return nil, fail(ErrClosed, "receive", ctx.Err(), context.Cause(ctx))
	}
	lifetime, cancel := context.WithCancel(ctx)
	copied := *inbox
	state := &receiverState[T]{inbox: &copied, ctx: lifetime, cancel: cancel, done: make(chan struct{})}
	go state.run(options.RetryDelay, sink)
	return &Receiver[T]{state: state}, nil
}
func (state *receiverState[T]) run(delay time.Duration, sink func(context.Context, adapters.Snapshot[T]) error) {
	defer close(state.done)
	defer state.cancel()
	defer func() { state.mu.Lock(); state.status.Stopped = true; state.mu.Unlock() }()
	for {
		delivery, err := state.inbox.NextReleased(state.ctx)
		if err != nil {
			if state.ctx.Err() == nil && !errors.Is(err, io.EOF) {
				state.failed(err)
			}
			return
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			_ = delivery.Retry()
			state.failed(err)
			return
		}
		value, _ := receipt.Snapshot()
		if state.ctx.Err() != nil {
			_ = delivery.Retry()
			return
		}
		if err = sink(state.ctx, value); err != nil {
			_ = delivery.Retry()
			state.failed(err)
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-state.ctx.Done():
				timer.Stop()
				return
			}
			continue
		}
		if err := delivery.Ack(); err != nil {
			_ = delivery.Retry()
			state.failed(err)
			return
		}
		state.mu.Lock()
		if state.status.Delivered < math.MaxUint64 {
			state.status.Delivered++
		}
		state.mu.Unlock()
	}
}
func (state *receiverState[T]) failed(err error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.status.Failures < math.MaxUint64 {
		state.status.Failures++
	}
	state.status.LastError = fail(ErrDelivery, "receive", err)
}

// Status returns detached counters and deliberately inspectable original causes.
func (receiver *Receiver[T]) Status() (ReceiverStatus, error) {
	if receiver == nil || receiver.state == nil {
		return ReceiverStatus{}, fail(ErrHandle, "receive_status")
	}
	receiver.state.mu.Lock()
	defer receiver.state.mu.Unlock()
	return receiver.state.status, nil
}

// Close stops this receiver and joins its actual callback stack, without sealing,
// acknowledging or discarding the Inbox. A timeout retains the same owner.
func (receiver *Receiver[T]) Close(ctx context.Context) error {
	if receiver == nil || receiver.state == nil {
		return fail(ErrHandle, "receive_close")
	}
	if ctx == nil {
		return fail(ErrOptions, "receive_close")
	}
	receiver.state.cancel()
	return receiver.wait(ctx)
}

// Finish seals new reservations and waits for delivery of accepted records. It
// never cancels producers or the sink. Stop producers first. Other receivers'
// claimed/unacknowledged records produce ErrPending even after this loop exits.
func (receiver *Receiver[T]) Finish(ctx context.Context) error {
	if receiver == nil || receiver.state == nil {
		return fail(ErrHandle, "receive_finish")
	}
	if ctx == nil {
		return fail(ErrOptions, "receive_finish")
	}
	if err := receiver.state.inbox.Seal(); err != nil {
		return err
	}
	if err := receiver.wait(ctx); err != nil {
		return err
	}
	status, err := receiver.state.inbox.Inspect()
	if err != nil {
		return err
	}
	if status.Outstanding != 0 {
		return fail(ErrPending, "receive_finish")
	}
	return nil
}
func (receiver *Receiver[T]) wait(ctx context.Context) error {
	select {
	case <-receiver.state.done:
		return nil
	default:
	}
	select {
	case <-receiver.state.done:
		return nil
	case <-ctx.Done():
		return fail(ErrWait, "receive_wait", ctx.Err(), context.Cause(ctx))
	}
}
