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
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func fetchWireFixture() *kmsg.FetchResponse {
	response := kmsg.NewPtrFetchResponse()
	response.Version = 13
	partition := kmsg.NewFetchResponseTopicPartition()
	partition.Partition = 0
	partition.HighWatermark, partition.LastStableOffset, partition.LogStartOffset = 8, 7, 0
	partition.RecordBatches = testBatch(0, 0, 1, testRecord([]byte("data")))
	partition.AbortedTransactions = []kmsg.FetchResponseTopicPartitionAbortedTransaction{{ProducerID: 3, FirstOffset: 0}}
	partition.AbortedTransactions[0].UnknownTags.Set(20, []byte("opaque-abort"))
	partition.DivergingEpoch.Epoch, partition.DivergingEpoch.EndOffset = 1, 2
	partition.CurrentLeader.LeaderID, partition.CurrentLeader.LeaderEpoch = 0, 2
	partition.SnapshotID.EndOffset, partition.SnapshotID.Epoch = 2, 1
	partition.UnknownTags.Set(21, []byte("opaque-partition"))
	topic := kmsg.FetchResponseTopic{TopicID: [16]byte{1}, Partitions: []kmsg.FetchResponseTopicPartition{partition}}
	topic.UnknownTags.Set(22, []byte("opaque-topic"))
	response.Topics = []kmsg.FetchResponseTopic{topic}
	response.Brokers = []kmsg.FetchResponseBroker{{NodeID: 0, Host: "127.0.0.1", Port: 9092, Rack: kmsg.StringPtr("rack")}}
	response.Brokers[0].UnknownTags.Set(23, []byte("opaque-broker"))
	response.UnknownTags.Set(24, []byte("opaque-root"))
	return response
}
func TestFetchEnvelopePreservesNativeValuesAndTimeout(t *testing.T) {
	original := fetchWireFixture()
	decoded := &boundedFetchResponse{FetchResponse: kmsg.NewPtrFetchResponse(), wireBytes: 1 << 20, maxAborted: 16}
	decoded.Version = 13
	if err := decoded.ReadFrom(original.AppendTo(nil)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.FetchResponse, original) {
		t.Fatal("bounded admission changed native values/tags")
	}
	request := &boundedFetchRequest{FetchRequest: kmsg.NewPtrFetchRequest(), wireBytes: 1 << 20, maxAborted: 16}
	request.SetVersion(13)
	request.SetTimeout(100)
	if request.Timeout() != 100 || request.MaxWaitMillis != 100 || request.ResponseKind().GetVersion() != 13 {
		t.Fatal("native request timeout or response version changed")
	}
}

type rawFetchEnvelopeResponse struct {
	*kmsg.FetchResponse
	body []byte
}

func (response *rawFetchEnvelopeResponse) AppendTo(dst []byte) []byte {
	return append(dst, response.body...)
}

func TestFetchEnvelopeRejectsPartitionCardinalityBeforeAllocation(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.MaxActive, options.MaxRecords, options.MaxDecodedRecords = 1, 1, 1
	options.MaxRecordBytes, options.MaxBatchBytes = 1024, 1024
	options.MaxDecodedBatchBytes, options.MaxWireBytes = 1536, 512<<10
	fixture := bindFixture(t, options, 1)
	position := Position{ClusterID: options.ClusterID, Topic: "records", TopicID: fixture.client.owner.topics["records"].ID}
	body := append(make([]byte, 10), 2)
	body = append(body, position.TopicID[:]...)
	body = binary.AppendUvarint(body, 65537)
	body = append(body, make([]byte, 65536)...)
	cluster.ControlKey(int16(kmsg.Fetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
		response := kmsg.NewPtrFetchResponse()
		response.SetVersion(request.GetVersion())
		return &rawFetchEnvelopeResponse{FetchResponse: response, body: body}, nil, true
	})
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := fixture.client.fetchPartition(deadline(t), position, fixture.client.owner.topics["records"].leaders[0])
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrLimit) {
		t.Fatal("shape rejected only after native decoding", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > uint64(fixture.client.owner.settings.reservation()) {
		t.Fatalf("rejected frame allocated %d bytes beyond the entire reservation", allocated)
	}
}

func TestFetchEnvelopeRejectsMalformedAndUnboundedTags(t *testing.T) {
	valid := fetchWireFixture().AppendTo(nil)
	for cut := 0; cut < len(valid); cut++ {
		response := &boundedFetchResponse{FetchResponse: kmsg.NewPtrFetchResponse(), wireBytes: 1 << 20, maxAborted: 16}
		response.Version = 13
		if err := response.ReadFrom(valid[:cut]); err == nil {
			t.Fatalf("truncated response accepted at %d", cut)
		}
	}
	response := fetchWireFixture()
	for tag := uint32(100); tag < 100+maxFetchTags; tag++ {
		response.UnknownTags.Set(tag, nil)
	}
	decoded := &boundedFetchResponse{FetchResponse: kmsg.NewPtrFetchResponse(), wireBytes: 1 << 20, maxAborted: 16}
	decoded.Version = 13
	if err := decoded.ReadFrom(response.AppendTo(nil)); !errors.Is(err, ErrLimit) {
		t.Fatal("global tag bound not enforced", err)
	}
	if err := decoded.ReadFrom(append(bytes.Clone(valid), 0)); !errors.Is(err, ErrRead) {
		t.Fatal("trailing response data accepted", err)
	}
}

func TestFetchEnvelopePreservesNativeTopLevelErrors(t *testing.T) {
	response := kmsg.NewPtrFetchResponse()
	response.Version = 13
	response.ErrorCode = 70
	guarded := &boundedFetchResponse{FetchResponse: kmsg.NewPtrFetchResponse(), wireBytes: 1024}
	guarded.Version = 13
	if err := guarded.ReadFrom(response.AppendTo(nil)); err != nil || guarded.ErrorCode != 70 {
		t.Fatal("native error response was lost", err)
	}
}

func FuzzFetchEnvelope(f *testing.F) {
	f.Add(fetchWireFixture().AppendTo(nil))
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 64<<10 {
			t.Skip()
		}
		response := &boundedFetchResponse{FetchResponse: kmsg.NewPtrFetchResponse(), wireBytes: 64 << 10, maxAborted: 16}
		response.Version = 13
		if err := response.ReadFrom(wire); err == nil {
			if len(response.Topics) > 1 {
				t.Fatal("unchecked native topic allocation")
			}
			for _, topic := range response.Topics {
				if len(topic.Partitions) > 1 {
					t.Fatal("unchecked native partition allocation")
				}
			}
		}
	})
}

var _ kmsg.TimeoutRequest = (*boundedFetchRequest)(nil)
var _ kmsg.SetTimeoutRequest = (*boundedFetchRequest)(nil)
var _ kmsg.ThrottleResponse = (*boundedFetchResponse)(nil)

func TestSmallValidAbortListUsesDecodedRecordBudget(t *testing.T) {
	for _, maximum := range []int{1, 2} {
		t.Run(fmt.Sprint(maximum), func(t *testing.T) {
			cluster := localCluster(t)
			options := clusterOptions(cluster)
			options.MaxActive, options.MaxRecords, options.MaxDecodedRecords = 1, 1, maximum
			fixture := bindFixture(t, options, 1)
			position := Position{ClusterID: options.ClusterID, Topic: "records", TopicID: fixture.client.owner.topics["records"].ID, Partition: 0}
			leader := fixture.client.owner.topics["records"].leaders[0]
			response := kmsg.NewPtrFetchResponse()
			response.Version = 13
			partition := kmsg.NewFetchResponseTopicPartition()
			partition.HighWatermark, partition.LastStableOffset, partition.LogStartOffset = 2, 2, 0
			partition.AbortedTransactions = []kmsg.FetchResponseTopicPartitionAbortedTransaction{{ProducerID: 1, FirstOffset: 0}, {ProducerID: 2, FirstOffset: 1}}
			response.Topics = []kmsg.FetchResponseTopic{{TopicID: position.TopicID, Partitions: []kmsg.FetchResponseTopicPartition{partition}}}
			wire := response.AppendTo(nil)
			independent := kmsg.NewPtrFetchResponse()
			independent.Version = 13
			if err := independent.ReadFrom(wire); err != nil || len(independent.Topics[0].Partitions[0].AbortedTransactions) != 2 {
				t.Fatal("positive native wire oracle rejected the small valid response", err)
			}
			cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) { return response, nil, true })
			result, err := fixture.client.fetchPartition(deadline(t), position, leader)
			if maximum == 1 {
				if !errors.Is(err, ErrLimit) {
					t.Fatalf("valid %d-byte response admitted two abort entries with a one-record workspace allowance: native entries=%d error=%v", len(wire), len(result.AbortedTransactions), err)
				}
			} else if err != nil || len(result.AbortedTransactions) != 2 {
				t.Fatal("within-budget abort metadata was refused", err)
			}
		})
	}
}
