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
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPageKeepsPrefixBeforeOversizedNextRecord(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.MaxRecords, options.MaxRecordBytes, options.MaxBatchBytes = 1, 1024, 2048
	fixture := bindFixture(t, options, 8)
	position := produce(t, fixture.client, "small", Message{Topic: "records", Value: []byte("small")}).WritesCopy()[0].Position
	native := nativeClient(t, cluster.ListenAddrs())
	nativeProduce(t, native, &kgo.Record{Topic: "records", Value: bytes.Repeat([]byte("x"), 1536)})
	receipt, err := fixture.client.ReadRange(deadline(t), correlation("prefix"), Range{Start: position, End: 2})
	result := settle(t, receipt, err)
	page := result.Outcome.Value.Page()
	if result.Err() != nil || page.Next() != 1 || page.Complete() || len(page.RecordsCopy()) != 1 {
		t.Fatal("supported prefix was poisoned by later oversized output", result.Err())
	}
	position.Offset = page.Next()
	receipt, err = fixture.client.ReadRange(deadline(t), correlation("too-large"), Range{Start: position, End: 2})
	result = settle(t, receipt, err)
	if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Page().Next() != 1 {
		t.Fatal("oversized first record was not explicitly refused")
	}
}

func readResult(t *testing.T, client *Client, id string, position Position) Read {
	t.Helper()
	receipt, err := client.ReadExact(deadline(t), correlation(id), position)
	result := settle(t, receipt, err)
	reads := result.Outcome.Value.ReadsCopy()
	if len(reads) != 1 {
		t.Fatal("exact result missing")
	}
	return reads[0]
}
func TestExactMissingCompactionDoesNotSubstitute(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 16)
	writer := nativeClient(t, cluster.ListenAddrs())
	alter := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = 2
	resource.ResourceName = "records"
	resource.Configs = []kmsg.IncrementalAlterConfigsRequestResourceConfig{{Name: "cleanup.policy", Value: kmsg.StringPtr("compact")}}
	alter.Resources = []kmsg.IncrementalAlterConfigsRequestResource{resource}
	response, err := alter.RequestWith(deadline(t), writer)
	if err != nil || len(response.Resources) != 1 || response.Resources[0].ErrorCode != 0 {
		t.Fatal("compaction fixture configuration failed")
	}
	var writes []Write
	for index, pair := range [][2]string{{"prefix", "prefix"}, {"same", "old"}, {"middle", "middle"}, {"same", "new"}, {"active", "active"}} {
		writes = append(writes, produce(t, fixture.client, string(rune('a'+index)), Message{Topic: "records", Key: []byte(pair[0]), Value: []byte(pair[1])}).WritesCopy()...)
	}
	cluster.Compact()
	// Independent negative control: NoResetOffset itself skips the missing offset.
	native := observe(t, cluster.ListenAddrs(), "records", 0, 1, 1)[0]
	if native.Offset != 2 {
		t.Fatal("compaction did not create the intended interior hole")
	}
	got := readResult(t, fixture.client, "missing", writes[1].Position)
	if got.State != ReadMissing || !errors.Is(got.Err, ErrMissing) {
		t.Fatal("missing position was substituted", got.Err)
	}
	live := readResult(t, fixture.client, "present", writes[2].Position)
	if live.State != ReadFound || string(live.Record.ValueCopy()) != "middle" {
		t.Fatal("positive exact control failed", live.Err)
	}
}
func TestReadRangePaginationAndResultOwnership(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.MaxRecords = 2
	fixture := bindFixture(t, options, 16)
	first := produce(t, fixture.client, "first", Message{Topic: "records", Value: []byte("one")}).WritesCopy()[0].Position
	for _, name := range []string{"two", "three", "four", "five"} {
		produce(t, fixture.client, name, Message{Topic: "records", Value: []byte(name)})
	}
	position := first
	var seen []string
	for pageIndex := range 4 {
		receipt, err := fixture.client.ReadRange(deadline(t), correlation(string(rune('p'+pageIndex))), Range{Start: position, End: 5})
		result := settle(t, receipt, err)
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
		page := result.Outcome.Value.Page()
		records := page.RecordsCopy()
		if len(records) > 2 || page.Next() <= position.Offset {
			t.Fatal("unbounded page or lost progress")
		}
		for _, record := range records {
			content := record.ValueCopy()
			seen = append(seen, string(content))
			content[0] = 'X'
			if record.ValueCopy()[0] == 'X' {
				t.Fatal("record alias")
			}
		}
		position.Offset = page.Next()
		if page.Complete() {
			break
		}
	}
	if position.Offset != 5 || len(seen) != 5 || seen[0] != "one" || seen[4] != "five" {
		t.Fatal("pagination lost or repeated output")
	}
}
func TestDeletedPrefixUnavailableAndEmptyRangeDiffer(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 8)
	writes := produce(t, fixture.client, "records", Message{Topic: "records", Value: []byte("old")}, Message{Topic: "records", Value: []byte("live")}).WritesCopy()
	request := kmsg.NewPtrDeleteRecordsRequest()
	request.TimeoutMillis = 5000
	request.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: "records", Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: 0, Offset: 1}}}}
	native := nativeClient(t, cluster.ListenAddrs())
	response, err := request.RequestWith(deadline(t), native)
	if err != nil || response.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal("delete fixture failed")
	}
	deleted := readResult(t, fixture.client, "deleted", writes[0].Position)
	if deleted.State != ReadExpired || !errors.Is(deleted.Err, ErrExpired) {
		t.Fatal("expired prefix not identified", deleted.Err)
	}
	beyond := writes[1].Position
	beyond.Offset = 20
	unavailable := readResult(t, fixture.client, "beyond", beyond)
	if unavailable.State != ReadUnavailable || !errors.Is(unavailable.Err, ErrUnavailable) {
		t.Fatal("unavailable offset called empty", unavailable.Err)
	}
	receipt, err := fixture.client.ReadRange(deadline(t), correlation("empty-range"), Range{Start: writes[1].Position, End: 1})
	result := settle(t, receipt, err)
	if result.Err() != nil || !result.Outcome.Value.Page().Complete() || len(result.Outcome.Value.Page().RecordsCopy()) != 0 {
		t.Fatal("explicit empty interval failed")
	}
}
func TestTopicRecreationRejectsHistoricalPosition(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 4)
	old := produce(t, fixture.client, "old", Message{Topic: "records", Value: []byte("old")}).WritesCopy()[0].Position
	native := nativeClient(t, cluster.ListenAddrs())
	request := kmsg.NewPtrDeleteTopicsRequest()
	request.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: kmsg.StringPtr("records")}}
	request.TimeoutMillis = 5000
	deleted, err := request.RequestWith(deadline(t), native)
	if err != nil || len(deleted.Topics) != 1 || deleted.Topics[0].ErrorCode != 0 {
		t.Fatal("topic deletion fixture failed")
	}
	create := kmsg.NewPtrCreateTopicsRequest()
	create.TimeoutMillis = 5000
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "records"
	topic.NumPartitions = 2
	topic.ReplicationFactor = 1
	create.Topics = []kmsg.CreateTopicsRequestTopic{topic}
	created, err := create.RequestWith(deadline(t), native)
	if err != nil || created.Topics[0].ErrorCode != 0 {
		t.Fatal("topic recreation fixture failed")
	}
	replacement := nativeClient(t, cluster.ListenAddrs())
	nativeProduce(t, replacement, &kgo.Record{Topic: "records", Value: []byte("replacement")})
	if observe(t, cluster.ListenAddrs(), "records", 0, 0, 1)[0].Offset != old.Offset {
		t.Fatal("numeric coordinate reuse control failed")
	}
	got := readResult(t, fixture.client, "historical", old)
	if !errors.Is(got.Err, ErrIdentity) || got.State == ReadFound {
		t.Fatal("recreated topic accepted")
	}
}
func TestReadRejectsWrongFetchIdentityAndCRC(t *testing.T) {
	for _, bad := range []string{"identity", "crc"} {
		t.Run(bad, func(t *testing.T) {
			cluster := localCluster(t)
			fixture := bindFixture(t, clusterOptions(cluster), 4)
			position := produce(t, fixture.client, "record", Message{Topic: "records", Value: []byte("data")}).WritesCopy()[0].Position
			cluster.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
				response := req.ResponseKind().(*kmsg.FetchResponse)
				topicID := position.TopicID
				if bad == "identity" {
					topicID[0] ^= 1
				}
				part := kmsg.NewFetchResponseTopicPartition()
				part.Partition = 0
				part.HighWatermark = 1
				part.LastStableOffset = 1
				part.LogStartOffset = 0
				if bad == "crc" {
					part.RecordBatches = make([]byte, 61)
					part.RecordBatches[11] = 49
					part.RecordBatches[16] = 2
				}
				response.Topics = []kmsg.FetchResponseTopic{{TopicID: topicID, Partitions: []kmsg.FetchResponseTopicPartition{part}}}
				return response, nil, true
			})
			got := readResult(t, fixture.client, "bad-fetch", position)
			want := ErrRead
			if bad == "identity" {
				want = ErrIdentity
			}
			if !errors.Is(got.Err, want) || got.State == ReadFound {
				t.Fatal("invalid fetch accepted", got.Err)
			}
		})
	}
}
func TestReadCommittedAbortedDataAndPendingAreNotEmpty(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 8)
	native := nativeClient(t, cluster.ListenAddrs(), kgo.TransactionalID("native-abort"))
	if err := native.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "records", Value: []byte("aborted")}
	nativeProduce(t, native, record)
	position := Position{ClusterID: fixture.client.owner.settings.ClusterID, Topic: "records", TopicID: fixture.client.owner.topics["records"].ID, Partition: 0, Offset: record.Offset}
	pending := readResult(t, fixture.client, "pending", position)
	if pending.State != ReadUnavailable {
		t.Fatal("uncommitted output treated as visible/empty", pending.Err)
	}
	if err := native.EndTransaction(deadline(t), kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	aborted := readResult(t, fixture.client, "aborted", position)
	if aborted.State != ReadMissing || !errors.Is(aborted.Err, ErrMissing) {
		t.Fatal("aborted output not rejected", aborted.Err)
	}
	// The native broker error remains inspectable on an invalid partition.
	invalid := position
	invalid.Partition = 99
	receipt, err := fixture.client.ReadExact(deadline(t), correlation("bad-partition"), invalid)
	if result := settle(t, receipt, err); !errors.Is(result.Err(), ErrInput) {
		t.Fatal("invalid partition was not rejected", result.Err())
	}
}
