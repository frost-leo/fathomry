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
	"errors"
	"sync/atomic"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kgo"
)

// WriteState describes record-level technical evidence, never business validity.
type WriteState uint8

const (
	WriteUnobserved WriteState = iota
	WriteNotSubmitted
	WriteUnknown
	WriteAcknowledged
)

// Write retains one input index's ACK or uncertainty, without copying payloads.
// Position is usable only after acknowledgement, PositionKnown and IdentityChecked. That check
// is metadata before/after delivery, NOT an atomic producer-observed topic ID;
// concurrent administrative recreation during production is unsupported.
type Write struct {
	private
	State           WriteState
	Position        Position
	PositionKnown   bool
	IdentityChecked bool
	Err             error
}

// Result is immutable shared in-process evidence. Methods returning lists copy
// storage; nested Record values share immutable private data. Native causes are
// borrowed for deliberate inspection, never printed by this boundary.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	writes      []Write
	reads       []Read
	topics      []Topic
	transaction TransactionState
	page        Page
	checkpoints []CheckpointResult
	consumer    ConsumerProgress
}

func (result Result) WritesCopy() []Write {
	if result.data == nil {
		return nil
	}
	return append([]Write{}, result.data.writes...)
}
func (result Result) ReadsCopy() []Read {
	if result.data == nil {
		return nil
	}
	return append([]Read{}, result.data.reads...)
}
func (result Result) TopicsCopy() []Topic {
	if result.data == nil {
		return nil
	}
	return append([]Topic{}, result.data.topics...)
}
func (result Result) Transaction() TransactionState {
	if result.data == nil {
		return TransactionUnobserved
	}
	return result.data.transaction
}

// Produce copies a bounded batch and returns its asynchronous receipt. The
// receipt/inbox completes only after EVERY native promise and the identity check.
// Wait cancellation does not cancel delivery. Delivery context cancellation can
// still leave stored records; non-nil native errors conservatively mean unknown.
// Input validation/admission failure returns no receipt and submits nothing.
func (client *Client) Produce(ctx context.Context, correlation fault.Correlation, messages []Message) (*invocation.Receipt[Result], error) {
	return client.produce(ctx, correlation, messages, false)
}
func (client *Client) produce(ctx context.Context, correlation fault.Correlation, messages []Message, transaction bool) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, "produce")
	}
	if transaction && client.owner.transaction == nil {
		return nil, failure(ErrUnsupported, "transaction")
	}
	if err := client.owner.validMessages(messages); err != nil {
		return nil, err
	}
	name := "produce"
	if transaction {
		name = "transaction"
	}
	call, err := client.begin(ctx, correlation, name, invocation.Async)
	if err != nil {
		return nil, err
	}
	records := copyMessages(messages)
	work, cancel, err := (invocation.Budget{Limit: client.owner.settings.Timeout}).Context(ctx, invocation.Delivery)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	go func() {
		defer cancel()
		data := &resultData{writes: make([]Write, len(records))}
		for index, record := range records {
			data.writes[index] = Write{State: WriteNotSubmitted, Position: Position{
				ClusterID: client.owner.settings.ClusterID, Topic: record.Topic, TopicID: client.owner.topics[record.Topic].ID,
				Partition: record.Partition, Offset: -1}}
		}
		var primary, cleanup error
		if transaction {
			primary, cleanup = client.transact(work, call, records, data)
		} else if primary = client.owner.writer.failure(); primary == nil {
			if primary = client.owner.checkIdentityWith(work, client.owner.writer.Client); primary == nil {
				stopWatch := client.owner.writer.watch(work, client.owner.settings.CleanupTimeout)
				primary = client.deliver(work, call, client.owner.writer, records, data)
				if fenced := stopWatch(); fenced != nil {
					primary = failure(ErrProduce, "producer-fenced", primary, fenced)
				}
			}
		}
		call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanup})
	}()
	return call.Receipt(), nil
}
func (client *Client) deliver(ctx context.Context, call *invocation.Call[Result], native *managedClient, records []*kgo.Record, data *resultData) error {
	guard, err := call.Scope().Hold()
	if err != nil {
		return err
	}
	type completion struct {
		index     int
		partition int32
		offset    int64
		err       error
	}
	replies := make(chan completion, len(records))
	var remaining atomic.Int32
	remaining.Store(int32(len(records)))
	// Each record sends once to its preallocated slot. No caller, log exporter or
	// evidence receiver runs on the SDK's serial promise path.
	for index, record := range records {
		if fenced := native.failure(); ctx.Err() != nil || fenced != nil {
			replies <- completion{index: index, err: failure(ErrProduce, "not-submitted", ctx.Err(), context.Cause(ctx), fenced)}
			if remaining.Add(-1) == 0 {
				guard.End()
			}
			continue
		}
		data.writes[index].State = WriteUnknown
		native.TryProduce(nativeContext{ctx}, record, func(record *kgo.Record, err error) {
			replies <- completion{index, record.Partition, record.Offset, err}
			if remaining.Add(-1) == 0 {
				guard.End()
			}
		})
	}
	var failures []error
	for range records {
		reply := <-replies
		write := &data.writes[reply.index]
		if reply.err != nil {
			write.Err = nativeFailure(ErrProduce, "delivery", ctx, reply.err)
			failures = append(failures, write.Err)
		} else {
			write.State = WriteAcknowledged
			write.PositionKnown = reply.offset >= 0 && reply.partition == write.Position.Partition
			write.Position.Partition = reply.partition
			write.Position.Offset = reply.offset
			if !write.PositionKnown {
				write.Err = failure(ErrProduce, "position-unobserved")
				failures = append(failures, write.Err)
			}
		}
	}
	// A canceled caller does not authorize detached new metadata I/O. Raw ACK
	// coordinates remain evidence, but unverified identities cannot be published.
	identity := client.owner.checkIdentityWith(ctx, native.Client)
	if identity != nil {
		failures = append(failures, identity)
	} else {
		for index := range data.writes {
			data.writes[index].IdentityChecked = data.writes[index].State == WriteAcknowledged && data.writes[index].PositionKnown
		}
	}
	if err := errors.Join(failures...); err != nil {
		return failure(ErrProduce, "batch", err)
	}
	return nil
}
