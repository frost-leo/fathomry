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
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (client *Client) fetchPartition(ctx context.Context, position Position, leader int32) (kmsg.FetchResponseTopicPartition, error) {
	if ctx.Err() != nil {
		return kmsg.FetchResponseTopicPartition{}, failure(ErrRead, "fetch", ctx.Err(), context.Cause(ctx))
	}
	request := kmsg.NewPtrFetchRequest()
	request.MinBytes, request.MaxWaitMillis = 1, 100
	request.MaxBytes = int32(client.owner.settings.MaxWireBytes - 1024)
	request.IsolationLevel = 1
	partition := kmsg.NewFetchRequestTopicPartition()
	partition.Partition, partition.FetchOffset, partition.PartitionMaxBytes = position.Partition, position.Offset, request.MaxBytes
	request.Topics = []kmsg.FetchRequestTopic{{TopicID: position.TopicID, Partitions: []kmsg.FetchRequestTopicPartition{partition}}}
	response, err := client.owner.native.Broker(int(leader)).Request(nativeContext{context.WithoutCancel(ctx)}, request)
	if err != nil {
		return kmsg.FetchResponseTopicPartition{}, nativeFailure(ErrRead, "fetch", ctx, err)
	}
	fetched := response.(*kmsg.FetchResponse)
	if fetched.Version != 13 {
		return kmsg.FetchResponseTopicPartition{}, failure(ErrUnsupported, "fetch-version")
	}
	if err := kerr.ErrorForCode(fetched.ErrorCode); err != nil {
		return kmsg.FetchResponseTopicPartition{}, failure(ErrRead, "fetch", err)
	}
	if len(fetched.Topics) != 1 || fetched.Topics[0].TopicID != position.TopicID || len(fetched.Topics[0].Partitions) != 1 {
		return kmsg.FetchResponseTopicPartition{}, failure(ErrIdentity, "fetch-topic")
	}
	raw := fetched.Topics[0].Partitions[0]
	if raw.Partition != position.Partition {
		return kmsg.FetchResponseTopicPartition{}, failure(ErrIdentity, "fetch-partition")
	}
	return raw, nil
}

// Decoding is staged before SDK record allocation. The map owns only this
// response's validated expansions; it is not a cross-call cache or pool.
type decodedBatches map[[32]byte][]byte

func (decoded decodedBatches) Decompress(src []byte, codec kgo.CompressionCodecType) ([]byte, error) {
	if codec != kgo.CodecGzip {
		return nil, failure(ErrUnsupported, "compression")
	}
	data, ok := decoded[sha256.Sum256(src)]
	if !ok {
		return nil, failure(ErrRead, "decode-state")
	}
	return data, nil
}
func expandGzip(src []byte, limit int) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, failure(ErrRead, "gzip", err)
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil {
		return nil, failure(ErrRead, "gzip", err)
	}
	if len(decoded) > limit {
		return nil, failure(ErrLimit, "decoded-bytes")
	}
	return decoded, nil
}

// validateRecords checks framing/cardinality BEFORE kmsg/kgo allocate headers
// and record slabs. Native parsing still owns actual values and CRC semantics.
func validateRecords(data []byte, count int32, lastDelta int32, baseTimestamp int64, appendTime bool) error {
	if count < 0 || lastDelta < 0 {
		return failure(ErrRead, "record-count")
	}
	previous := int64(-1)
	for range count {
		length, used := binary.Varint(data)
		if used <= 0 || used > 5 || length < 0 || length > int64(len(data)-used) || length > math.MaxInt32 {
			return failure(ErrRead, "record-frame")
		}
		frame := data[used : used+int(length)]
		data = data[used+int(length):]
		if len(frame) < 1 {
			return failure(ErrRead, "record-frame")
		}
		frame = frame[1:]
		take := func(maximumBytes int) (int64, bool) {
			value, used := binary.Varint(frame)
			if used <= 0 || used > maximumBytes {
				return 0, false
			}
			frame = frame[used:]
			return value, true
		}
		timestamp, ok := take(10)
		if !ok || !appendTime && (timestamp < -baseTimestamp || timestamp > math.MaxInt64/1000000-baseTimestamp) {
			return failure(ErrRead, "timestamp")
		}
		offset, ok := take(5)
		if !ok || offset <= previous || offset > int64(lastDelta) {
			return failure(ErrRead, "offset-delta")
		}
		previous = offset
		skip := func(nullable bool, maximum int) bool {
			length, ok := take(5)
			if !ok || length < -1 || length == -1 && !nullable || length > int64(len(frame)) || length > int64(maximum) {
				return false
			}
			if length > 0 {
				frame = frame[int(length):]
			}
			return true
		}
		if !skip(true, math.MaxInt32) || !skip(true, math.MaxInt32) {
			return failure(ErrRead, "record-bytes")
		}
		headers, ok := take(5)
		if !ok || headers < 0 || headers > 64 {
			return failure(ErrLimit, "headers")
		}
		for range headers {
			if !skip(false, 1024) || !skip(true, math.MaxInt32) {
				return failure(ErrRead, "header")
			}
		}
		if len(frame) != 0 {
			return failure(ErrRead, "record-trailing")
		}
	}
	if len(data) != 0 {
		return failure(ErrRead, "batch-trailing")
	}
	return nil
}

func prepareFetch(value settings, raw []byte) ([]byte, decodedBatches, error) {
	decoded := make(decodedBatches)
	remaining := value.MaxDecodedBatchBytes
	remainingRecords := value.MaxDecodedRecords
	position, batches := 0, 0
	previousEnd := int64(-1)
	for position < len(raw) {
		tail := raw[position:]
		// Kafka may truncate the LAST batch at the fetch byte target. Only the
		// complete prefix is eligible; its parsed cursor, not LSO, proves progress.
		if len(tail) < 12 {
			break
		}
		length := int64(int32(binary.BigEndian.Uint32(tail[8:12]))) + 12
		if length < 61 {
			return nil, nil, failure(ErrRead, "batch-frame")
		}
		if length > int64(len(tail)) {
			break
		}
		batchBytes := tail[:int(length)]
		if batchBytes[16] != 2 {
			return nil, nil, failure(ErrUnsupported, "record-magic")
		}
		batches++
		if batches > value.MaxDecodedRecords+16 || remaining == 0 {
			break
		}
		var batch kmsg.RecordBatch
		if err := batch.ReadFrom(batchBytes); err != nil {
			return nil, nil, failure(ErrRead, "batch", err)
		}
		if crc32.Checksum(batchBytes[21:], crc32.MakeTable(crc32.Castagnoli)) != uint32(batch.CRC) {
			return nil, nil, failure(ErrRead, "crc")
		}
		if batch.FirstOffset < 0 || batch.LastOffsetDelta < 0 || int64(batch.LastOffsetDelta) > math.MaxInt64-batch.FirstOffset-1 ||
			batch.NumRecords < 0 {
			return nil, nil, failure(ErrRead, "batch-records")
		}
		if int64(batch.NumRecords) > int64(remainingRecords) {
			if position > 0 {
				break
			}
			return nil, nil, failure(ErrLimit, "batch-records")
		}
		if batch.FirstOffset <= previousEnd {
			return nil, nil, failure(ErrRead, "batch-order")
		}
		previousEnd = batch.FirstOffset + int64(batch.LastOffsetDelta)
		remainingRecords -= int(batch.NumRecords)
		if batch.FirstTimestamp < 0 || batch.MaxTimestamp < 0 ||
			batch.FirstTimestamp > math.MaxInt64/1000000 || batch.MaxTimestamp > math.MaxInt64/1000000 {
			return nil, nil, failure(ErrUnsupported, "timestamp")
		}
		records := batch.Records
		switch kgo.CompressionCodecType(batch.Attributes & 7) {
		case kgo.CodecNone:
		case kgo.CodecGzip:
			var err error
			records, err = expandGzip(records, remaining)
			if err != nil {
				if position > 0 && errors.Is(err, ErrLimit) {
					return raw[:position], decoded, nil
				}
				return nil, nil, err
			}
			decoded[sha256.Sum256(batch.Records)] = records
		default:
			return nil, nil, failure(ErrUnsupported, "compression")
		}
		if len(records) > remaining {
			if position > 0 {
				break
			}
			return nil, nil, failure(ErrLimit, "decoded-bytes")
		}
		remaining -= len(records)
		if err := validateRecords(records, batch.NumRecords, batch.LastOffsetDelta, batch.FirstTimestamp, batch.Attributes&8 != 0); err != nil {
			return nil, nil, err
		}
		position += int(length)
	}
	return raw[:position], decoded, nil
}
