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

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func FuzzRecordPayloadRoundTrip(f *testing.F) {
	f.Add([]byte("data"), false, false)
	f.Add([]byte{0, 255, 1}, true, false)
	f.Add([]byte{}, true, true)
	f.Fuzz(func(t *testing.T, payload []byte, compressed, null bool) {
		if len(payload) > 32<<10 {
			t.Skip()
		}
		record := kmsg.Record{Value: payload, Headers: []kmsg.Header{{Key: "duplicate", Value: nil}, {Key: "duplicate", Value: []byte{}}}}
		if null {
			record.Value = nil
		}
		record.Length = int32(len(record.AppendTo(nil)) - 1)
		records := record.AppendTo(nil)
		if compressed {
			var encoded bytes.Buffer
			writer := gzip.NewWriter(&encoded)
			if _, err := writer.Write(records); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			records = encoded.Bytes()
		}
		raw := testBatch(0, 0, 1, records)
		if compressed {
			binary.BigEndian.PutUint16(raw[21:23], 1)
			binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
		}
		prepared, decoded, err := prepareFetch(defaults(OptionsV1{}), raw)
		if err != nil {
			t.Fatal("valid generated record refused", err)
		}
		parsed, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Topic: "records", IsolationLevel: kgo.ReadCommitted()},
			&kmsg.FetchResponseTopicPartition{HighWatermark: 1, LastStableOffset: 1, RecordBatches: prepared}, decoded, nil)
		if parsed.Err != nil || len(parsed.Records) != 1 || next != 1 {
			t.Fatal("valid record did not round trip")
		}
		got := freezeRecord(Position{Topic: "records"}, parsed.Records[0])
		if null {
			if got.ValueCopy() != nil {
				t.Fatal("null changed")
			}
		} else if got.ValueCopy() == nil || !bytes.Equal(got.ValueCopy(), payload) {
			t.Fatal("value changed")
		}
		headers := got.HeadersCopy()
		if len(headers) != 2 || headers[0].Value != nil || headers[1].Value == nil {
			t.Fatal("header presence changed")
		}
	})
}
