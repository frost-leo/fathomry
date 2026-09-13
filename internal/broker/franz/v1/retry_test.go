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
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func produceErrorResponse(request kmsg.Request, code int16) *kmsg.ProduceResponse {
	response := request.ResponseKind().(*kmsg.ProduceResponse)
	for _, topic := range request.(*kmsg.ProduceRequest).Topics {
		out := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
		for _, partition := range topic.Partitions {
			out.Partitions = append(out.Partitions, kmsg.ProduceResponseTopicPartition{Partition: partition.Partition, ErrorCode: code, BaseOffset: -1})
		}
		response.Topics = append(response.Topics, out)
	}
	return response
}
func TestOrdinaryRetryBudgetAndStoredResult(t *testing.T) {
	for _, retries := range []int{0, 1} {
		name := "no-retry"
		if retries == 1 {
			name = "one-retry"
		}
		t.Run(name, func(t *testing.T) {
			cluster := localCluster(t)
			options := clusterOptions(cluster)
			options.Retries = retries
			fixture := bindFixture(t, options, 2)
			var attempts atomic.Int32
			cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
				if attempts.Add(1) == 1 {
					cluster.KeepControl()
					return produceErrorResponse(request, kerr.NotEnoughReplicas.Code), nil, true
				}
				return nil, nil, false
			})
			receipt, err := fixture.client.Produce(deadline(t), correlation(name), []Message{{Topic: "records", Value: []byte("retry-data")}})
			result := settle(t, receipt, err)
			if retries == 0 {
				if result.Err() == nil || attempts.Load() != 1 {
					t.Fatal("zero ordinary retry budget ignored")
				}
			} else {
				if result.Err() != nil || attempts.Load() != 2 {
					t.Fatal("bounded retry did not recover", result.Err())
				}
				if string(observe(t, cluster.ListenAddrs(), "records", 0, 0, 1)[0].Value) != "retry-data" {
					t.Fatal("retry result not stored")
				}
			}
			if result.Attempts.Exact || result.Attempts.Observed != 0 {
				t.Fatal("fixture's count was invented as complete SDK attempt evidence")
			}
		})
	}
}
func TestSourceAdmissionRejectsBeforeNativeSubmission(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.MaxActive = 1
	fixture := bindFixture(t, options, 4)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var attempts atomic.Int32
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		attempts.Add(1)
		close(entered)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	receipt, err := fixture.client.Produce(deadline(t), correlation("active"), []Message{{Topic: "records"}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("no active produce")
	}
	if _, err := fixture.client.Produce(deadline(t), correlation("overloaded"), []Message{{Topic: "records"}}); !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("source overload was not rejected", err)
	}
	if fixture.inbox.Usage().Outstanding != 1 {
		t.Fatal("failed admission retained an evidence slot")
	}
	unblock()
	if result := settle(t, receipt, nil); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if attempts.Load() != 1 {
		t.Fatal("refused call reached native produce")
	}
}
