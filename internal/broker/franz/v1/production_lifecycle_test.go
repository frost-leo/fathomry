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
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUncertainDeliveryMustTerminateWithoutBrokerRecovery(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		name := "ordinary"
		if transaction {
			name = "transaction"
		}
		t.Run(name, func(t *testing.T) {
			cluster := localCluster(t)
			options := clusterOptions(cluster)
			options.Timeout, options.CleanupTimeout = time.Second, 100*time.Millisecond
			if transaction {
				options.TransactionalID = "review-uncertain"
			}
			fixture := bindFixture(t, options, 4)
			var faultActive atomic.Bool
			faultActive.Store(true)
			defer faultActive.Store(false)
			var attempts atomic.Int32
			entered := make(chan struct{})
			var once sync.Once
			cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
				if !faultActive.Load() {
					return nil, nil, false
				}
				cluster.KeepControl()
				attempts.Add(1)
				once.Do(func() { close(entered) })
				response := request.ResponseKind().(*kmsg.ProduceResponse)
				for _, topic := range request.(*kmsg.ProduceRequest).Topics {
					out := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
					for _, partition := range topic.Partitions {
						out.Partitions = append(out.Partitions, kmsg.ProduceResponseTopicPartition{Partition: partition.Partition, ErrorCode: kerr.RequestTimedOut.Code, BaseOffset: -1})
					}
					response.Topics = append(response.Topics, out)
				}
				return response, nil, true
			})
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			receipt, err := fixture.client.produce(ctx, correlation("uncertain-delivery"), []Message{{Topic: "records", Value: []byte("uncertain")}}, transaction)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-deadline(t).Done():
				t.Fatal("request never entered fault")
			}
			cancelCause := errors.New("review-cancel")
			cancel(cancelCause)
			wait, waitCancel := context.WithTimeout(context.Background(), 1400*time.Millisecond)
			observed, waitErr := receipt.WaitReleased(wait)
			waitCancel()
			observedAttempts := attempts.Load()
			if waitErr == nil {
				write := observed.Outcome.Value.WritesCopy()[0]
				if write.State == WriteAcknowledged || write.IdentityChecked || transaction && observed.Outcome.Value.Transaction() == TransactionCommitted {
					t.Fatal("fenced uncertainty became an acknowledged effect")
				}
				metadataReceipt, metadataErr := fixture.client.Metadata(deadline(t), correlation("control-survives"))
				if got := settle(t, metadataReceipt, metadataErr); got.Err() != nil {
					t.Fatal("fenced producer killed independent metadata client", got.Err())
				}
				nextReceipt, nextErr := fixture.client.produce(deadline(t), correlation("fenced-next"), []Message{{Topic: "records", Value: []byte("must-not-write")}}, transaction)
				nextResult := settle(t, nextReceipt, nextErr)
				if !errors.Is(nextResult.Err(), ErrState) || nextResult.Outcome.Value.WritesCopy()[0].State != WriteNotSubmitted || attempts.Load() != observedAttempts {
					t.Fatal("permanent fence admitted another native write")
				}
			}
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			closeErr := fixture.assembly.Close(closeCtx)
			closeCancel()
			faultActive.Store(false)
			final := settle(t, receipt, nil)
			if !errors.Is(final.Err(), cancelCause) {
				t.Fatal("cleanup lost caller cause")
			}
			if waitErr != nil {
				t.Errorf("outcome cannot settle without broker recovery; caller canceled, delivery timeout=1s cleanup=100ms retries=0, observed produce attempts=%d, assembly close incomplete=%t", observedAttempts, errors.Is(closeErr, resource.ErrIncomplete))
			}
		})
	}
}

func TestTransactionBeginMustObserveBudget(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.TransactionalID = "begin-context-review"
	options.Timeout = time.Second
	options.CleanupTimeout = 100 * time.Millisecond
	fixture := bindFixture(t, options, 2)
	var failures atomic.Bool
	failures.Store(true)
	defer failures.Store(false)
	var attempts atomic.Int32
	cluster.ControlKey(int16(kmsg.InitProducerID), func(req kmsg.Request) (kmsg.Response, error, bool) {
		if !failures.Load() {
			return nil, nil, false
		}
		cluster.KeepControl()
		attempts.Add(1)
		response := req.ResponseKind().(*kmsg.InitProducerIDResponse)
		response.ErrorCode = kerr.ConcurrentTransactions.Code
		return response, nil, true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	receipt, err := fixture.client.ProduceTransaction(ctx, correlation("begin-context"), []Message{{Topic: "records"}})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	time.Sleep(1200 * time.Millisecond)
	result, resolved := receipt.Result()
	blocked := !resolved || !result.Released
	observedAttempts := attempts.Load()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	closeErr := fixture.assembly.Close(closeCtx)
	closeCancel()
	failures.Store(false)
	final := settle(t, receipt, nil)
	if !errors.Is(final.Err(), context.DeadlineExceeded) {
		t.Fatal("completed after unblock without original deadline cause")
	}
	if blocked {
		t.Errorf("transaction begin retained call 1200 ms after 100 ms caller deadline, exceeding 1 s option budget; attempts=%d; assembly close incomplete=%t", observedAttempts, errors.Is(closeErr, resource.ErrIncomplete))
	}
}

func TestAcknowledgedWriteRequiresKnownOffset(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 2)
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		request := req.(*kmsg.ProduceRequest)
		response := request.ResponseKind().(*kmsg.ProduceResponse)
		for _, topic := range request.Topics {
			reply := kmsg.NewProduceResponseTopic()
			reply.Topic, reply.TopicID = topic.Topic, topic.TopicID
			for _, partition := range topic.Partitions {
				item := kmsg.NewProduceResponseTopicPartition()
				item.Partition = partition.Partition
				item.ErrorCode = kerr.DuplicateSequenceNumber.Code
				item.BaseOffset = -1
				reply.Partitions = append(reply.Partitions, item)
			}
			response.Topics = append(response.Topics, reply)
		}
		return response, nil, true
	})
	receipt, err := fixture.client.Produce(deadline(t), correlation("unknown-offset"), []Message{{Topic: "records", Value: []byte("location-unknown")}})
	result := settle(t, receipt, err)
	write := result.Outcome.Value.WritesCopy()[0]
	if result.Err() == nil || write.Err == nil || write.State != WriteAcknowledged || write.Position.Offset != -1 || write.PositionKnown || write.IdentityChecked {
		t.Fatal("unknown-offset result lost ACK evidence or certified an unavailable position")
	}
}

func TestNativeCloseDuringBegin(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.TransactionalID = "begin-close-review"
	fixture := bindFixture(t, options, 2)
	entered := make(chan struct{})
	var once sync.Once
	cluster.ControlKey(int16(kmsg.InitProducerID), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		once.Do(func() { close(entered) })
		response := req.ResponseKind().(*kmsg.InitProducerIDResponse)
		response.ErrorCode = kerr.ConcurrentTransactions.Code
		return response, nil, true
	})
	receipt, err := fixture.client.ProduceTransaction(deadline(t), correlation("begin-close"), []Message{{Topic: "records"}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("native begin did not start")
	}
	fixture.client.owner.transaction.Close()
	result := settle(t, receipt, nil)
	if result.Err() == nil || result.Outcome.Value.WritesCopy()[0].State == WriteAcknowledged || result.Outcome.Value.Transaction() == TransactionCommitted {
		t.Fatal("Close-during-Begin falsely certified success")
	}
	t.Logf("write state after closed Begin=%d, transaction=%d", result.Outcome.Value.WritesCopy()[0].State, result.Outcome.Value.Transaction())
}
