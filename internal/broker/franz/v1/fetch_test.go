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
	"encoding/binary"
	"hash/crc32"
	"testing"
	"unsafe"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDecodedHeaderStorageFitsWorkingReservation(t *testing.T) {
	value := defaults(OptionsV1{MaxRecords: 64, MaxRecordBytes: 4096, MaxBatchBytes: 4096, MaxWireBytes: 8192, MaxDecodedBatchBytes: 32768})
	const count = 144
	var records []byte
	for index := range count {
		record := kmsg.Record{OffsetDelta: int32(index), Headers: make([]kmsg.Header, 64)}
		record.Length = int32(len(record.AppendTo(nil)) - 1)
		records = record.AppendTo(records)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(records); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	raw := testBatch(0, count-1, count, compressed.Bytes())
	binary.BigEndian.PutUint16(raw[21:23], 1)
	binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	prepared, decoded, err := prepareFetch(value, raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Topic: "records", IsolationLevel: kgo.ReadCommitted()},
		&kmsg.FetchResponseTopicPartition{HighWatermark: count, LastStableOffset: count, RecordBatches: prepared}, decoded, nil)
	if parsed.Err != nil || len(parsed.Records) != count {
		t.Fatal("native decoded fixture changed")
	}
	var headerBytes int64
	for _, record := range parsed.Records {
		headerBytes += int64(len(record.Headers)) * int64(unsafe.Sizeof(kgo.RecordHeader{})+unsafe.Sizeof(kmsg.Header{}))
	}
	if headerBytes+int64(len(records)) > value.reservation() {
		t.Fatal("simultaneously owned native header storage exceeds the working reservation")
	}
}

func testBatch(first int64, delta, count int32, records []byte) []byte {
	raw := make([]byte, 61)
	binary.BigEndian.PutUint64(raw[0:8], uint64(first))
	binary.BigEndian.PutUint32(raw[8:12], uint32(49+len(records)))
	raw[16] = 2
	binary.BigEndian.PutUint32(raw[23:27], uint32(delta))
	binary.BigEndian.PutUint32(raw[57:61], uint32(count))
	raw = append(raw, records...)
	binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	return raw
}

func testRecord(value []byte) []byte {
	body := []byte{0, 0, 0, 1}
	body = binary.AppendVarint(body, int64(len(value)))
	body = append(body, value...)
	body = append(body, 0)
	return append(binary.AppendVarint(nil, int64(len(body))), body...)
}

func TestDecodedLimitRetainsPrefix(t *testing.T) {
	value := defaults(OptionsV1{})
	raw := testBatch(0, 0, 1, testRecord([]byte("target")))
	for index := range 6 {
		raw = append(raw, testBatch(int64(index+1), 0, 1, testRecord(bytes.Repeat([]byte("x"), 768<<10)))...)
	}
	if len(raw) >= value.MaxWireBytes {
		t.Fatal("fixture exceeded wire cap")
	}
	prefix, _, err := prepareFetch(value, raw)
	if err != nil || len(prefix) == 0 {
		t.Fatalf("valid bounded prefix discarded: wire=%d decoded_cap=%d prefix=%d err=%v", len(raw), value.MaxDecodedBatchBytes, len(prefix), err)
	}
}

func TestOverlongRecordLengthRejected(t *testing.T) {
	records := []byte{0x8c, 0x80, 0x80, 0x80, 0x80, 0, 0, 0, 0, 1, 1, 0}
	raw := testBatch(0, 0, 1, records)
	prepared, decoded, err := prepareFetch(defaults(OptionsV1{}), raw)
	if err != nil {
		return
	}
	defer func() {
		if panicValue := recover(); panicValue != nil {
			t.Errorf("malformed int32 record length accepted and reached SDK panic: %v", panicValue)
		}
	}()
	parsed, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: 0, Topic: "records", IsolationLevel: kgo.ReadCommitted()},
		&kmsg.FetchResponseTopicPartition{HighWatermark: 1, LastStableOffset: 1, RecordBatches: prepared}, decoded, nil)
	t.Errorf("malformed int32 record length accepted: records=%d next=%d err=%v", len(parsed.Records), next, parsed.Err)
}

func TestOverlongOffsetRejected(t *testing.T) {
	body := []byte{0, 0, 0x80, 0x80, 0x80, 0x80, 0x80, 0, 1, 1, 0}
	records := append(binary.AppendVarint(nil, int64(len(body))), body...)
	raw := testBatch(0, 0, 1, records)
	prepared, decoded, err := prepareFetch(defaults(OptionsV1{}), raw)
	if err != nil {
		return
	}
	parsed, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: 0, Topic: "records", IsolationLevel: kgo.ReadCommitted()},
		&kmsg.FetchResponseTopicPartition{HighWatermark: 1, LastStableOffset: 1, RecordBatches: prepared}, decoded, nil)
	t.Errorf("malformed int32 record offset accepted: records=%d next=%d err=%v", len(parsed.Records), next, parsed.Err)
}

func TestReorderedBatchRejected(t *testing.T) {
	raw := append(testBatch(10, 0, 1, testRecord([]byte("later"))), testBatch(0, 0, 1, testRecord([]byte("target")))...)
	prepared, decoded, err := prepareFetch(defaults(OptionsV1{}), raw)
	if err != nil {
		return
	}
	parsed, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: 0, Topic: "records", IsolationLevel: kgo.ReadCommitted()},
		&kmsg.FetchResponseTopicPartition{HighWatermark: 11, LastStableOffset: 11, RecordBatches: prepared}, decoded, nil)
	t.Errorf("out-of-order batches accepted; exact target hidden by native cursor: records=%d first=%d next=%d err=%v", len(parsed.Records), parsed.Records[0].Offset, next, parsed.Err)
}

func TestNativeBatchCountExceedsPageLimit(t *testing.T) {
	var records []byte
	for index := range 600 {
		body := []byte{0, 0}
		body = binary.AppendVarint(body, int64(index))
		body = append(body, 1, 1, 0)
		records = append(records, binary.AppendVarint(nil, int64(len(body)))...)
		records = append(records, body...)
	}
	_, _, err := prepareFetch(defaults(OptionsV1{}), testBatch(0, 599, 600, records))
	if err != nil {
		t.Fatalf("valid ~5 KiB batch below byte bounds cannot be read with default output page bound 256: %v", err)
	}
}

func TestRecordOwnershipPositive(t *testing.T) {
	native := &kgo.Record{Key: nil, Value: []byte{}, Headers: []kgo.RecordHeader{{Key: "same", Value: nil}, {Key: "same", Value: []byte{}}, {Key: "binary", Value: []byte{0, 255}}}}
	frozen := freezeRecord(Position{Topic: "records", Offset: 4}, native)
	native.Headers[0].Key = "mutated"
	native.Headers[2].Value[1] = 0
	if frozen.KeyCopy() != nil || frozen.ValueCopy() == nil || len(frozen.ValueCopy()) != 0 {
		t.Fatal("null versus present-empty values changed")
	}
	first := frozen.HeadersCopy()
	if len(first) != 3 || first[0].Key != "same" || first[0].Value != nil || first[1].Value == nil || first[2].Value[1] != 255 {
		t.Fatal("header order/content/ownership changed")
	}
	first[2].Value[1] = 0
	if frozen.HeadersCopy()[2].Value[1] != 255 {
		t.Fatal("mutable returned header alias")
	}
}
