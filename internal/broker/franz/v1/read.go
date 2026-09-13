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
	"math"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ReadState is technical existence/visibility evidence, not output validity.
type ReadState uint8

const (
	ReadUnobserved ReadState = iota
	ReadFound
	ReadMissing
	ReadExpired
	ReadUnavailable
)

// Read is one exact-address observation. Missing means a checked scan passed the
// address without a visible data record (including aborted/control records).
// Expired means below the observed log start; it does not diagnose retention vs
// explicit deletion. Unavailable means at/above LSO, not successful empty output.
type Read struct {
	private
	Position Position
	State    ReadState
	Record   Record
	Err      error
}

// Range selects physical offsets [Start.Offset, End) of ONE topic partition.
// This is not a declaration that the interval belongs to one batch or Item.
type Range struct {
	private
	Start Position
	End   int64
}

// Page owns immutable visible records and a resumable physical scan cursor.
type Page struct {
	private
	records                         []Record
	next                            int64
	complete                        bool
	observed                        bool
	logStart, highWatermark, stable int64
}

func (page Page) RecordsCopy() []Record { return append([]Record{}, page.records...) }

// Next is the next offset to scan, including gaps/control records already passed.
func (page Page) Next() int64 { return page.next }

// Complete means this physical interval was scanned, not that every offset exists.
func (page Page) Complete() bool { return page.complete }

// Watermarks are one validated fetch observation, not a retention or recovery
// SLA. observed=false returns -1 values, including a zero Page or local empty scan.
func (page Page) Watermarks() (logStart, highWatermark, lastStable int64, observed bool) {
	if !page.observed {
		return -1, -1, -1, false
	}
	return page.logStart, page.highWatermark, page.stable, true
}
func (result Result) Page() Page {
	if result.data == nil {
		return Page{}
	}
	return result.data.page
}

func (owner *connection) validPosition(position Position) error {
	expected, ok := owner.topics[position.Topic]
	if !ok || position.ClusterID != owner.settings.ClusterID || position.TopicID != expected.ID {
		return failure(ErrIdentity, "position")
	}
	if position.Partition < 0 || position.Offset < 0 || position.Offset == math.MaxInt64 {
		return failure(ErrInput, "position")
	}
	return nil
}

// ReadExact never substitutes the next available record at another offset.
func (client *Client) ReadExact(ctx context.Context, correlation fault.Correlation, position Position) (*invocation.Receipt[Result], error) {
	return client.ReadPositions(ctx, correlation, []Position{position})
}

// ReadPositions reads a bounded exact set in input order, preserving per-address
// failures. The outer framework can read a large/noncontiguous output in chunks.
// No group, autocommit, auto-reset, prefetch or caller callback is involved.
// Cancellation stops new requests; issued requests finish under native network
// deadlines before this synchronous call releases its resource reservation.
func (client *Client) ReadPositions(ctx context.Context, correlation fault.Correlation, positions []Position) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, "read")
	}
	if len(positions) == 0 || len(positions) > client.owner.settings.MaxRecords {
		return nil, failure(ErrLimit, "positions")
	}
	for _, position := range positions {
		if err := client.owner.validPosition(position); err != nil {
			return nil, err
		}
	}
	call, err := client.begin(ctx, correlation, "read-exact", invocation.Finite)
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: client.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		data := &resultData{reads: make([]Read, len(positions))}
		var failures []error
		total := int64(0)
		for index, position := range positions {
			read := Read{Position: position}
			page, err := client.fetchPage(work, Range{Start: position, End: position.Offset + 1})
			switch {
			case errors.Is(err, ErrExpired):
				read.State = ReadExpired
			case errors.Is(err, ErrUnavailable):
				read.State = ReadUnavailable
			case err != nil:
			case len(page.records) == 1 && page.records[0].Position().Offset == position.Offset:
				record := page.records[0]
				total += record.size()
				if total > int64(client.owner.settings.MaxBatchBytes) {
					err = failure(ErrLimit, "read-result")
				} else {
					read.State = ReadFound
					read.Record = record
				}
			case page.next > position.Offset:
				read.State = ReadMissing
				err = failure(ErrMissing, "exact-offset")
			default:
				err = failure(ErrRead, "no-progress")
			}
			read.Err = err
			data.reads[index] = read
			if err != nil {
				failures = append(failures, err)
			}
		}
		return invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: joinFailures(ErrRead, "exact-set", failures)}
	})
	return call.Receipt(), nil
}

// ReadRange returns at most MaxRecords/MaxBatchBytes of visible data with a
// continuation cursor. A later gap is not evidence of membership/completeness of
// a business output. An explicit zero-length interval is a successful empty scan;
// missing or not-yet-stable requested offsets are not silently treated as empty.
func (client *Client) ReadRange(ctx context.Context, correlation fault.Correlation, interval Range) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, "read")
	}
	if err := client.owner.validPosition(interval.Start); err != nil {
		return nil, err
	}
	if interval.End < interval.Start.Offset {
		return nil, failure(ErrInput, "range")
	}
	call, err := client.begin(ctx, correlation, "read-range", invocation.Finite)
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: client.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		page, err := client.fetchPage(work, interval)
		return invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{page: page}}, Primary: err}
	})
	return call.Receipt(), nil
}
func joinFailures(kind fault.Kind, operation string, failures []error) error {
	if err := errors.Join(failures...); err != nil {
		return failure(kind, operation, err)
	}
	return nil
}

func (client *Client) fetchPage(ctx context.Context, interval Range) (Page, error) {
	start := interval.Start
	page := Page{next: start.Offset, logStart: -1, highWatermark: -1, stable: -1}
	topics, err := client.owner.metadata(ctx)
	if err != nil {
		return page, err
	}
	topic := topics[start.Topic]
	if topic.ID != start.TopicID {
		return page, failure(ErrIdentity, "topic-incarnation")
	}
	if int(start.Partition) >= len(topic.leaders) {
		return page, failure(ErrInput, "partition")
	}
	if interval.End == start.Offset {
		page.complete = true
		return page, nil
	}
	value := client.owner.settings
	raw, err := client.fetchPartition(ctx, start, topic.leaders[start.Partition])
	if err != nil {
		return page, err
	}
	page.logStart, page.highWatermark, page.stable = raw.LogStartOffset, raw.HighWatermark, raw.LastStableOffset
	nativeErr := kerr.ErrorForCode(raw.ErrorCode)
	if nativeErr != nil {
		if errors.Is(nativeErr, kerr.UnknownTopicID) {
			return page, failure(ErrIdentity, "topic-incarnation", nativeErr)
		}
		if errors.Is(nativeErr, kerr.OffsetOutOfRange) {
			return page, client.offsetFailure(ctx, start, topic.leaders[start.Partition], nativeErr)
		}
		return page, failure(ErrRead, "fetch-partition", nativeErr)
	}
	if page.logStart < 0 || page.highWatermark < page.stable || page.stable < page.logStart {
		return page, failure(ErrRead, "watermarks")
	}
	page.observed = true
	if start.Offset < page.logStart {
		return page, failure(ErrExpired, "log-start")
	}
	if start.Offset >= page.stable {
		return page, failure(ErrUnavailable, "stable-offset")
	}
	prepared, decoded, err := prepareFetch(value, raw.RecordBatches)
	if err != nil {
		return page, err
	}
	raw.RecordBatches = prepared
	parsed, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: start.Offset, Topic: start.Topic, Partition: start.Partition,
		IsolationLevel: kgo.ReadCommitted(), KeepControlRecords: true}, &raw, decoded, nil)
	if parsed.Err != nil {
		return page, failure(ErrRead, "decode", parsed.Err)
	}
	if next > page.stable {
		return page, failure(ErrRead, "scan-watermark")
	}
	total := int64(0)
	for _, record := range parsed.Records {
		if record.Offset >= interval.End {
			page.next = interval.End
			page.complete = true
			return page, nil
		}
		if record.Offset < start.Offset || record.Offset >= page.stable {
			return page, failure(ErrRead, "record-offset")
		}
		if record.Attrs.IsControl() {
			continue
		}
		if len(page.records) == value.MaxRecords {
			page.next = record.Offset
			return page, nil
		}
		size := nativeRecordSize(record)
		if size > int64(value.MaxRecordBytes) {
			if len(page.records) > 0 {
				page.next = record.Offset
				return page, nil
			}
			return page, failure(ErrLimit, "record")
		}
		if size > int64(value.MaxBatchBytes)-total {
			page.next = record.Offset
			return page, nil
		}
		total += size
		position := start
		position.Offset = record.Offset
		page.records = append(page.records, freezeRecord(position, record))
	}
	if next > page.next {
		page.next = min(next, interval.End)
	}
	page.complete = page.next >= interval.End
	if page.next == start.Offset {
		return page, failure(ErrRead, "no-progress")
	}
	return page, nil
}

func (client *Client) offsetFailure(ctx context.Context, position Position, leader int32, original error) error {
	if ctx.Err() != nil {
		return failure(ErrRead, "offset-range", original, ctx.Err(), context.Cause(ctx))
	}
	request := kmsg.NewPtrListOffsetsRequest()
	request.IsolationLevel = 1
	partition := kmsg.NewListOffsetsRequestTopicPartition()
	partition.Partition, partition.Timestamp = position.Partition, -2
	request.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: position.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{partition}}}
	response, err := client.owner.native.Broker(int(leader)).Request(nativeContext{context.WithoutCancel(ctx)}, request)
	if err != nil {
		return failure(ErrRead, "offset-range", original, err)
	}
	offsets := response.(*kmsg.ListOffsetsResponse)
	if len(offsets.Topics) != 1 || offsets.Topics[0].Topic != position.Topic || len(offsets.Topics[0].Partitions) != 1 {
		return failure(ErrIdentity, "offset-range", original)
	}
	observed := offsets.Topics[0].Partitions[0]
	if observed.Partition != position.Partition || observed.Offset < 0 || observed.ErrorCode != 0 {
		return failure(ErrRead, "offset-range", original, kerr.ErrorForCode(observed.ErrorCode))
	}
	// Name lookup only supplies a candidate valid offset. The confirming
	// watermarks must come from a fresh fetch addressed by the original ID.
	candidate := position
	candidate.Offset = observed.Offset
	raw, err := client.fetchPartition(ctx, candidate, leader)
	if err != nil {
		return failure(ErrRead, "offset-range", original, err)
	}
	if errors.Is(kerr.ErrorForCode(raw.ErrorCode), kerr.UnknownTopicID) {
		return failure(ErrIdentity, "topic-incarnation", original, kerr.UnknownTopicID)
	}
	if raw.ErrorCode != 0 || raw.LogStartOffset < 0 || raw.LastStableOffset < raw.LogStartOffset || raw.HighWatermark < raw.LastStableOffset {
		return failure(ErrRead, "offset-range", original, kerr.ErrorForCode(raw.ErrorCode))
	}
	if position.Offset < raw.LogStartOffset {
		return failure(ErrExpired, "log-start", original)
	}
	if position.Offset >= raw.LastStableOffset {
		return failure(ErrUnavailable, "stable-offset", original)
	}
	return failure(ErrRead, "offset-range", original)
}
func nativeRecordSize(record *kgo.Record) int64 {
	size := int64(len(record.Topic)+len(record.Key)+len(record.Value)) + 128
	for _, header := range record.Headers {
		size += int64(len(header.Key)+len(header.Value)) + 32
	}
	return size
}
func (record Record) size() int64 {
	size := int64(len(record.position.Topic)+len(record.key)+len(record.value)) + 128
	for _, header := range record.headers {
		size += int64(len(header.key)+len(header.value)) + 32
	}
	return size
}
