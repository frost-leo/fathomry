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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func deadline(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func correlation(id string) fault.Correlation { return fault.Correlation{Call: id} }

type fixtureListener struct {
	net.Listener
	closed atomic.Bool
}

func (listener *fixtureListener) Close() error {
	err := listener.Listener.Close()
	listener.closed.Store(true)
	return err
}

func localCluster(t testing.TB, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	var mutex sync.Mutex
	var listeners []*fixtureListener
	base := []kfake.Opt{kfake.NumBrokers(1), kfake.ClusterID("fathomry-kafka-test"), kfake.SeedTopics(2, "records", "references"),
		kfake.ListenFn(func(network, address string) (net.Listener, error) {
			_, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			listener, err := net.Listen(network, net.JoinHostPort("127.0.0.1", port))
			if err != nil {
				return nil, err
			}
			tracked := &fixtureListener{Listener: listener}
			mutex.Lock()
			listeners = append(listeners, tracked)
			mutex.Unlock()
			return tracked, nil
		})}
	cluster, err := kfake.NewCluster(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cluster.Close()
		mutex.Lock()
		defer mutex.Unlock()
		for _, listener := range listeners {
			if !listener.closed.Load() {
				t.Error("test-owned listener close was not observed")
			}
		}
	})
	return cluster
}
func clusterOptions(cluster *kfake.Cluster) OptionsV1 {
	return OptionsV1{Name: "data", Brokers: cluster.ListenAddrs(), ClusterID: "fathomry-kafka-test",
		Topics: []string{"records", "references"}, Plaintext: true}
}

type fixture struct {
	client   *Client
	assembly *resource.Assembly
	selected resource.Selection[Source]
	inbox    *invocation.Inbox[Result]
}

func bindFixture(t testing.TB, options OptionsV1, capacity int, layers ...resource.Layer) fixture {
	t.Helper()
	selected, err := Select(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "test", selected)
	if err != nil {
		if assembly != nil {
			_ = assembly.Close(deadline(t))
		}
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*(64<<20))
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for inbox.Usage().Outstanding > 0 {
			delivery, err := inbox.Next(deadline(t))
			if err != nil {
				t.Error(err)
				break
			}
			if _, err := delivery.Receipt().WaitReleased(deadline(t)); err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Release(); err != nil {
				t.Error(err)
				break
			}
		}
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error(err)
		}
	})
	return fixture{client, assembly, selected, inbox}
}

func logFixtureCauses(t testing.TB, err error) {
	if err == nil {
		return
	}
	if technical, ok := err.(*fault.Error); ok {
		t.Logf("technical kind=%s operation=%s", technical.Diagnostic().Kind, technical.Diagnostic().Context.Operation)
	} else if broker, ok := err.(*kerr.Error); ok {
		t.Logf("native broker error code=%d", broker.Code)
	} else {
		t.Logf("native cause type=%T", err)
	}
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range multi.Unwrap() {
			logFixtureCauses(t, cause)
		}
	} else if single, ok := err.(interface{ Unwrap() error }); ok {
		logFixtureCauses(t, single.Unwrap())
	}
}
func settle(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func produce(t testing.TB, client *Client, id string, messages ...Message) Result {
	t.Helper()
	receipt, err := client.Produce(deadline(t), correlation(id), messages)
	result := settle(t, receipt, err)
	if result.Err() != nil {
		logFixtureCauses(t, result.Err())
		t.Fatalf("produce %s: %v", id, result.Err())
	}
	return result.Outcome.Value
}
func nativeClient(t testing.TB, addresses []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{kgo.SeedBrokers(addresses...), kgo.DisableClientMetrics(), kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted())}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}
func nativeProduce(t testing.TB, client *kgo.Client, records ...*kgo.Record) {
	t.Helper()
	if err := client.ProduceSync(deadline(t), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}
func observe(t testing.TB, addresses []string, topic string, partition int32, offset int64, count int) []*kgo.Record {
	t.Helper()
	reader := nativeClient(t, addresses, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().At(offset)}}))
	var records []*kgo.Record
	for len(records) < count {
		fetches := reader.PollRecords(deadline(t), count-len(records))
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}

func TestConnectionReadinessAndShutdown(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 4)
	receipt, err := fixture.client.Metadata(deadline(t), correlation("metadata"))
	result := settle(t, receipt, err)
	if result.Err() != nil || len(result.Outcome.Value.TopicsCopy()) != 2 {
		t.Fatal("metadata was not verified", result.Err())
	}
	if result.Attempts.Exact || result.Attempts.Observed != 0 {
		t.Fatal("unobserved attempts invented")
	}
	if err := fixture.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.Metadata(deadline(t), correlation("closed")); err == nil {
		t.Fatal("closed assembly admitted work")
	}
}
