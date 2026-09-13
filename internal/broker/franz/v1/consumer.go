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

package franz

import (
	"context"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Consumer owns one bounded direct-partition cursor and a source reservation.
// It is not a consumer-group member: there are no heartbeats, automatic commits
// or rebalance rights. Next and Commit reject concurrent use; Close is concurrent
// and idempotent. Copies of this owner must not be made.
type Consumer struct {
	private
	client   *Client
	call     *invocation.Call[Result]
	interval Range
	lifetime context.Context
	cancel   context.CancelFunc
	mutex    sync.Mutex
	next     int64
	received uint64
	active   bool
	closing  bool
	idle     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// ConsumerProgress is a technical scan summary, not completed business work.
// Received counts records returned by successful pages; it is not an Item count.
type ConsumerProgress struct {
	private
	Next     Position
	End      int64
	Received uint64
	Complete bool
}

func (result Result) ConsumerProgress() ConsumerProgress {
	if result.data == nil {
		return ConsumerProgress{}
	}
	return result.data.consumer
}

// Consume opens a direct consumer for a declared physical range. Use End=max
// int64 only when intentionally following a growing partition. The caller must
// supply a cancellable context or deadline and eventually Close; cancellation
// also requests closure. Each Next/Commit has its own finite call budget.
// An open consumer requires inbox capacity for its root AND incremental calls.
func (client *Client) Consume(ctx context.Context, id fault.Correlation, interval Range) (*Consumer, error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, "consume")
	}
	if err := client.owner.validPosition(interval.Start); err != nil {
		return nil, err
	}
	if interval.End < interval.Start.Offset {
		return nil, failure(ErrInput, "consume-range")
	}
	lifetime, cancel, err := (invocation.Budget{}).Context(ctx, invocation.Lifetime)
	if err != nil {
		return nil, err
	}
	call, err := client.begin(ctx, id, "consume", invocation.Stream)
	if err != nil {
		cancel()
		return nil, err
	}
	idle := make(chan struct{})
	close(idle)
	consumer := &Consumer{client: client, call: call, interval: interval, lifetime: lifetime, cancel: cancel, next: interval.Start.Offset,
		idle: idle, stop: make(chan struct{}), done: make(chan struct{})}
	go consumer.run()
	return consumer, nil
}
func (consumer *Consumer) run() {
	defer close(consumer.done)
	defer consumer.cancel()
	select {
	case <-consumer.stop:
	case <-consumer.lifetime.Done():
	}
	consumer.mutex.Lock()
	consumer.closing = true
	idle := consumer.idle
	consumer.mutex.Unlock()
	<-idle
	consumer.mutex.Lock()
	progress := ConsumerProgress{Next: consumer.interval.Start, End: consumer.interval.End, Received: consumer.received, Complete: consumer.next >= consumer.interval.End}
	progress.Next.Offset = consumer.next
	consumer.mutex.Unlock()
	var primary error
	if consumer.lifetime.Err() != nil {
		primary = failure(ErrRead, "consumer-lifetime", consumer.lifetime.Err(), context.Cause(consumer.lifetime))
	}
	consumer.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{consumer: progress}}, Primary: primary})
}

// Receipt is the independently owned consumer-lifetime result. Per-page data and
// checkpoint results have their own receipts in the same evidence inbox.
func (consumer *Consumer) Receipt() *invocation.Receipt[Result] {
	if consumer == nil {
		return nil
	}
	return consumer.call.Receipt()
}

// Close stops new pages and requests local cursor release; it never closes the
// source or commits a checkpoint. The receipt waits for an already-issued native
// page/commit to finish before reporting final cursor state and releasing use.
func (consumer *Consumer) Close() *invocation.Receipt[Result] {
	if consumer == nil || consumer.call == nil || consumer.stop == nil {
		return nil
	}
	consumer.mutex.Lock()
	consumer.closing = true
	consumer.mutex.Unlock()
	consumer.stopOnce.Do(func() { close(consumer.stop) })
	return consumer.Receipt()
}

func (consumer *Consumer) operationContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(consumer.lifetime, func() { cancel(context.Cause(consumer.lifetime)) })
	if consumer.lifetime.Err() != nil {
		cancel(context.Cause(consumer.lifetime))
	}
	return ctx, func() { stop(); cancel(nil) }
}
func (consumer *Consumer) enter() error {
	if consumer == nil || consumer.call == nil {
		return failure(ErrInput, "consumer")
	}
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	if consumer.closing || consumer.active || consumer.lifetime.Err() != nil {
		return failure(ErrState, "consumer")
	}
	consumer.active = true
	consumer.idle = make(chan struct{})
	return nil
}
func (consumer *Consumer) leave() {
	consumer.mutex.Lock()
	consumer.active = false
	close(consumer.idle)
	consumer.mutex.Unlock()
}

// Next reads one bounded page and advances ONLY after successful validated
// scanning. It does not replace unavailable output with empty business data.
// For a growing partition, ErrUnavailable remains explicit; callers choose when
// to poll again within their execution policy rather than create a hidden retry.
func (consumer *Consumer) Next(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if ctx == nil {
		return nil, failure(ErrInput, "next")
	}
	if err := consumer.enter(); err != nil {
		return nil, err
	}
	defer consumer.leave()
	ctx, cancel := consumer.operationContext(ctx)
	defer cancel()
	call, err := consumer.client.beginWithin(ctx, id, "consume-page", consumer.call)
	if err != nil {
		return nil, err
	}
	consumer.mutex.Lock()
	interval := consumer.interval
	interval.Start.Offset = consumer.next
	consumer.mutex.Unlock()
	_ = call.Execute(ctx, invocation.Budget{Limit: consumer.client.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		if consumer.lifetime.Err() != nil {
			return invocation.Outcome[Result]{Primary: failure(ErrRead, "consumer-lifetime", consumer.lifetime.Err(), context.Cause(consumer.lifetime))}
		}
		page, err := consumer.client.fetchPage(work, interval)
		if err == nil {
			consumer.mutex.Lock()
			consumer.next = page.Next()
			consumer.received += uint64(len(page.records))
			consumer.mutex.Unlock()
		}
		return invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{page: page}}, Primary: err}
	})
	return call.Receipt(), nil
}

// Commit explicitly declares the successfully consumed prefix as the desired
// checkpoint. Call only after the outer processing/effect policy permits it.
// Reading or committing still does not prove business completion. No implicit
// commit occurs on Next, Close or lifetime cancellation.
func (consumer *Consumer) Commit(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if ctx == nil {
		return nil, failure(ErrInput, "commit")
	}
	if err := consumer.enter(); err != nil {
		return nil, err
	}
	defer consumer.leave()
	ctx, cancel := consumer.operationContext(ctx)
	defer cancel()
	consumer.mutex.Lock()
	checkpoint := Checkpoint{Topic: consumer.interval.Start.Topic, TopicID: consumer.interval.Start.TopicID, Partition: consumer.interval.Start.Partition, Next: consumer.next}
	consumer.mutex.Unlock()
	return consumer.client.checkpoints(ctx, id, []Checkpoint{checkpoint}, true, consumer.call)
}
