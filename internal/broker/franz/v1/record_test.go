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
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"math"
	"testing"
	"time"
)

func TestMessageTimeOverflowRejected(t *testing.T) {
	timestamp := time.Unix(math.MaxInt64/500+1, 0)
	owner := connection{settings: defaults(OptionsV1{}), topics: map[string]Topic{"records": {}}}
	if err := owner.validMessages([]Message{{Topic: "records", Timestamp: timestamp}}); err == nil {
		t.Fatalf("out-of-range timestamp accepted: unix_seconds=%d wrapped_millis=%d", timestamp.Unix(), timestamp.UnixMilli())
	}
}

func TestRecordTimestampAndHeadersRoundTrip(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.Compression = "gzip"
	fixture := bindFixture(t, options, 8)
	newer, older := time.UnixMilli(2000), time.UnixMilli(1000)
	// Native RecordDeliveryTimeout measures age from the record's business
	// timestamp, not submission time. The provider's lifetime budget must not.
	native := nativeClient(t, cluster.ListenAddrs(), kgo.RecordDeliveryTimeout(time.Second))
	if err := native.ProduceSync(deadline(t), &kgo.Record{Topic: "references", Timestamp: older}).FirstErr(); !errors.Is(err, kgo.ErrRecordTimeout) {
		t.Fatal("native timestamp-coupled timeout control changed", err)
	}
	writes := produce(t, fixture.client, "timestamps", Message{Topic: "records", Timestamp: newer, Key: nil, Value: []byte{},
		Headers: []Header{{Key: "repeat", Value: nil}, {Key: "repeat", Value: []byte{}}, {Key: "binary", Value: []byte{0, 255}}}},
		Message{Topic: "records", Timestamp: older, Key: []byte{}, Value: nil}).WritesCopy()
	for index, want := range []time.Time{newer, older} {
		read := readResult(t, fixture.client, "timestamp-"+string(rune('a'+index)), writes[index].Position)
		if read.State != ReadFound || !read.Record.Timestamp().Equal(want) {
			t.Fatal("valid negative timestamp delta was not preserved", read.Err)
		}
		if index == 0 {
			headers := read.Record.HeadersCopy()
			if read.Record.KeyCopy() != nil || read.Record.ValueCopy() == nil || headers[0].Value != nil || headers[1].Value == nil || !bytes.Equal(headers[2].Value, []byte{0, 255}) {
				t.Fatal("null/empty/header fidelity lost")
			}
			headers[2].Value[1] = 0
			if read.Record.HeadersCopy()[2].Value[1] != 255 {
				t.Fatal("header storage aliased")
			}
		} else if read.Record.KeyCopy() == nil || read.Record.ValueCopy() != nil {
			t.Fatal("empty key/tombstone distinction lost")
		}
	}
	independent := observe(t, cluster.ListenAddrs(), "records", 0, 0, 2)
	if !independent[0].Timestamp.Equal(newer) || !independent[1].Timestamp.Equal(older) || independent[0].ProducerID < 0 {
		t.Fatal("native time/idempotency positive control failed")
	}
}
func TestRecordBoundsBeforeSubmission(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.MaxRecordBytes = 1024
	options.MaxBatchBytes = 2048
	fixture := bindFixture(t, options, 2)
	for _, messages := range [][]Message{
		nil, {{Topic: "unknown"}}, {{Topic: "records", Partition: -1}}, {{Topic: "records", Value: make([]byte, 1024)}},
		{{Topic: "records", Headers: make([]Header, 65)}},
	} {
		if _, err := fixture.client.Produce(deadline(t), correlation("invalid"), messages); err == nil {
			t.Fatal("invalid input admitted")
		}
	}
	if fixture.inbox.Usage().Outstanding != 0 {
		t.Fatal("rejected input created evidence/work")
	}
	reader := nativeClient(t, cluster.ListenAddrs())
	// A valid record after all local refusals must still occupy the first offset.
	record := &kgo.Record{Topic: "records", Value: []byte("first")}
	nativeProduce(t, reader, record)
	if record.Offset != 0 {
		t.Fatal("rejected input produced data")
	}
}
