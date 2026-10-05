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

package kafka

import (
	"context"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
)

// Header preserves ordered duplicate keys and nil versus present-empty values.
type Header struct {
	private
	Key   string
	Value []byte
}

// Message is borrowed only during submission, then copied. Nil Value is a
// tombstone. Manual routing uses explicit Partition; keyed routing requires -1.
type Message struct {
	private
	Topic      string
	Partition  int32
	Key, Value []byte
	Headers    []Header
	Timestamp  time.Time
}

// WriteState separates no observation, no submission, uncertainty and native ACK.
type WriteState uint8

const (
	WriteUnobserved WriteState = iota
	WriteNotSubmitted
	WriteUnknown
	WriteAcknowledged
)

// Write keeps the actual ACK position. IdentityChecked is metadata before/after
// delivery, not an atomic topic-ID observation in the native producer ACK.
type Write struct {
	private
	State                          WriteState
	Position                       Position
	PositionKnown, IdentityChecked bool
	Err                            error
}

func (value Result) WritesCopy() []Write {
	input := value.native.WritesCopy()
	if input == nil {
		return nil
	}
	output := make([]Write, len(input))
	for index, item := range input {
		output[index] = Write{State: WriteState(item.State), Position: position(item.Position),
			PositionKnown: item.PositionKnown, IdentityChecked: item.IdentityChecked, Err: translate(item.Err, "produce")}
	}
	return output
}

// Produce returns read-only observation after bounded input copying, not after
// delivery. Waiting cancellation does not abandon the owner or discard late ACKs.
func (client *Client) Produce(ctx context.Context, messages []Message) (*adapters.Receipt[Result], error) {
	return client.produce(ctx, messages, false)
}
func (client *Client) produce(ctx context.Context, messages []Message, transaction bool) (*adapters.Receipt[Result], error) {
	name := "produce"
	if transaction {
		name = "transaction"
	}
	return client.dispatch(ctx, name, func(op *operation, bound *native.Client) {
		if len(messages) == 0 || len(messages) > op.state.maxRecords {
			op.finish(nil, fail(ErrLimit, name))
			return
		}
		for _, message := range messages {
			if len(message.Headers) > 64 {
				op.finish(nil, fail(ErrLimit, "headers"))
				return
			}
		}
		input := make([]native.Message, len(messages))
		for index, message := range messages {
			input[index] = native.Message{Topic: message.Topic, Partition: message.Partition, Key: message.Key, Value: message.Value, Timestamp: message.Timestamp, Headers: make([]native.Header, len(message.Headers))}
			for header, item := range message.Headers {
				input[index].Headers[header] = native.Header{Key: item.Key, Value: item.Value}
			}
		}
		produce := bound.Produce
		if transaction {
			produce = bound.ProduceTransaction
		}
		receipt, err := produce(op.lifetime, op.correlation(), input)
		if receipt == nil {
			op.finish(nil, err)
			return
		}
		if err := op.keep(receipt, err, nil); err != nil {
			op.finish(nil, err)
		}
	})
}
