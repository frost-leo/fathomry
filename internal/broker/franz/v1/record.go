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
	"bytes"
	"math"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Header preserves ordered duplicate names and nil versus present-empty values.
type Header struct {
	private
	Key   string
	Value []byte
}

// Message is borrowed during Produce/ProduceTransaction only. On acceptance its
// bytes and headers are copied before return. Callers must not concurrently
// mutate input during that method. Partition is explicit; key does not route.
// A zero timestamp uses SDK time; nonzero is truncated to milliseconds.
// Nil Value is a Kafka tombstone, NOT a framework successful-empty output.
type Message struct {
	private
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// Position is a process-local physical address, not Item/attempt attribution or
// a durable reference format. TopicID and ClusterID prevent historical name reuse.
// A producer ACK does not itself observe TopicID; see Write.IdentityChecked.
type Position struct {
	private
	ClusterID string
	Topic     string
	TopicID   [16]byte
	Partition int32
	Offset    int64
}

// Record is immutable after transfer to receipts; all byte access returns copies.
type Record struct {
	private
	position                 Position
	key, value               string
	keyPresent, valuePresent bool
	headers                  []header
	timestamp                time.Time
	leaderEpoch              int32
}
type header struct {
	key, value string
	present    bool
}

func (record Record) Position() Position { return record.position }

// Timestamp is the broker-retained record time. LogAppendTime topic policy can
// replace the submitted timestamp; this is not an Activity or source-time proof.
func (record Record) Timestamp() time.Time { return record.timestamp }
func (record Record) LeaderEpoch() int32   { return record.leaderEpoch }
func (record Record) KeyCopy() []byte {
	if !record.keyPresent {
		return nil
	}
	return []byte(record.key)
}
func (record Record) ValueCopy() []byte {
	if !record.valuePresent {
		return nil
	}
	return []byte(record.value)
}
func (record Record) HeadersCopy() []Header {
	result := make([]Header, len(record.headers))
	for index, item := range record.headers {
		result[index].Key = item.key
		if item.present {
			result[index].Value = []byte(item.value)
		}
	}
	return result
}
func freezeRecord(position Position, raw *kgo.Record) Record {
	record := Record{position: position, key: string(raw.Key), value: string(raw.Value), keyPresent: raw.Key != nil, valuePresent: raw.Value != nil,
		timestamp: raw.Timestamp, leaderEpoch: raw.LeaderEpoch, headers: make([]header, len(raw.Headers))}
	for index, item := range raw.Headers {
		record.headers[index] = header{strings.Clone(item.Key), string(item.Value), item.Value != nil}
	}
	return record
}
func messageSize(message Message) int64 {
	total := int64(len(message.Topic)) + int64(len(message.Key)) + int64(len(message.Value)) + 128
	for _, item := range message.Headers {
		total += int64(len(item.Key)) + int64(len(item.Value)) + 32
	}
	return total
}
func (owner *connection) validMessages(messages []Message) error {
	value := owner.settings
	if len(messages) == 0 || len(messages) > value.MaxRecords {
		return failure(ErrLimit, "records")
	}
	var total int64
	for _, message := range messages {
		if _, ok := owner.topics[message.Topic]; !ok || message.Partition < 0 || len(message.Headers) > 64 ||
			!message.Timestamp.IsZero() && (message.Timestamp.Before(time.UnixMilli(0)) || message.Timestamp.After(time.UnixMilli(math.MaxInt64/1000000))) {
			return failure(ErrInput, "record")
		}
		for _, item := range message.Headers {
			if len(item.Key) > 1024 {
				return failure(ErrLimit, "headers")
			}
		}
		size := messageSize(message)
		if size > int64(value.MaxRecordBytes) {
			return failure(ErrLimit, "record")
		}
		total += size
	}
	if total > int64(value.MaxBatchBytes) {
		return failure(ErrLimit, "batch")
	}
	return nil
}
func copyMessages(messages []Message) []*kgo.Record {
	records := make([]*kgo.Record, len(messages))
	for index, message := range messages {
		record := &kgo.Record{Topic: strings.Clone(message.Topic), Partition: message.Partition, Key: bytes.Clone(message.Key), Value: bytes.Clone(message.Value),
			Timestamp: message.Timestamp, Headers: make([]kgo.RecordHeader, len(message.Headers))}
		for index, item := range message.Headers {
			record.Headers[index] = kgo.RecordHeader{Key: strings.Clone(item.Key), Value: bytes.Clone(item.Value)}
		}
		records[index] = record
	}
	return records
}
