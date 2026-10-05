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

	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
)

// Position is a process-local physical coordinate, not a versioned durable DTO.
type Position struct {
	private
	ClusterID, Topic string
	TopicID          [16]byte
	Partition        int32
	Offset           int64
}

func position(value native.Position) Position {
	return Position{ClusterID: value.ClusterID, Topic: value.Topic, TopicID: value.TopicID, Partition: value.Partition, Offset: value.Offset}
}
func (value Position) native() native.Position {
	return native.Position{ClusterID: value.ClusterID, Topic: value.Topic, TopicID: value.TopicID, Partition: value.Partition, Offset: value.Offset}
}

// Range is a physical interval [Start.Offset,End), not a logical output batch.
type Range struct {
	private
	Start Position
	End   int64
}

func (value Range) native() native.Range {
	return native.Range{Start: value.Start.native(), End: value.End}
}

// Record is immutable; byte and header getters return caller-owned copies.
type Record struct {
	private
	value native.Record
}

func (value Record) Position() Position   { return position(value.value.Position()) }
func (value Record) Timestamp() time.Time { return value.value.Timestamp() }
func (value Record) LeaderEpoch() int32   { return value.value.LeaderEpoch() }
func (value Record) KeyCopy() []byte      { return value.value.KeyCopy() }
func (value Record) ValueCopy() []byte    { return value.value.ValueCopy() }
func (value Record) HeadersCopy() []Header {
	input := value.value.HeadersCopy()
	output := make([]Header, len(input))
	for index, item := range input {
		output[index] = Header{Key: item.Key, Value: item.Value}
	}
	return output
}

// Page retains a validated physical scan, including gaps/control records.
type Page struct {
	private
	value native.Page
}

func (value Page) Next() int64    { return value.value.Next() }
func (value Page) Complete() bool { return value.value.Complete() }
func (value Page) Watermarks() (logStart, highWatermark, lastStable int64, observed bool) {
	return value.value.Watermarks()
}
func (value Page) RecordsCopy() []Record {
	input := value.value.RecordsCopy()
	output := make([]Record, len(input))
	for index, item := range input {
		output[index] = Record{value: item}
	}
	return output
}
func (value Result) Page() Page { return Page{value: value.native.Page()} }

// ReadState describes physical existence/visibility, never business completion.
type ReadState uint8

const (
	ReadUnobserved ReadState = iota
	ReadFound
	ReadMissing
	ReadExpired
	ReadUnavailable
)

// Read retains one exact-address observation and its independent error.
type Read struct {
	private
	Position Position
	State    ReadState
	Record   Record
	Err      error
}

func (value Result) ReadsCopy() []Read {
	input := value.native.ReadsCopy()
	if input == nil {
		return nil
	}
	output := make([]Read, len(input))
	for index, item := range input {
		output[index] = Read{Position: position(item.Position), State: ReadState(item.State), Record: Record{value: item.Record}, Err: translate(item.Err, "read")}
	}
	return output
}

// ReadExact never substitutes a later record for a missing exact offset.
func (client *Client) ReadExact(ctx context.Context, position Position) (Result, error) {
	return client.ReadPositions(ctx, []Position{position})
}

// ReadPositions preserves bounded input order and per-position partial failures.
// It is synchronous through actual native release, not a hard cancellation deadline.
func (client *Client) ReadPositions(ctx context.Context, positions []Position) (Result, error) {
	return result(client.dispatch(ctx, "read-exact", func(op *operation, bound *native.Client) {
		if len(positions) == 0 || len(positions) > op.state.maxRecords {
			op.finish(nil, fail(ErrLimit, "positions"))
			return
		}
		input := make([]native.Position, len(positions))
		for index, value := range positions {
			input[index] = value.native()
		}
		receipt, err := bound.ReadPositions(op.lifetime, op.correlation(), input)
		op.finish(receipt, err)
	}))
}

// ReadRange returns a bounded physical prefix and resumable scan position.
func (client *Client) ReadRange(ctx context.Context, interval Range) (Result, error) {
	return result(client.dispatch(ctx, "read-range", func(op *operation, bound *native.Client) {
		receipt, err := bound.ReadRange(op.lifetime, op.correlation(), interval.native())
		op.finish(receipt, err)
	}))
}
