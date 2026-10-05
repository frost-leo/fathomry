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

package kafka

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func publicPeer(t testing.TB) *kfake.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.ClusterID("gh105-public"), kfake.SeedTopics(2, "records"),
		kfake.ListenFn(func(network, address string) (net.Listener, error) {
			_, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			return net.Listen(network, net.JoinHostPort("127.0.0.1", port))
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	return cluster
}

func peerSettings(peer *kfake.Cluster) Settings {
	return Settings{Name: "public", Brokers: peer.ListenAddrs(), ClusterID: "gh105-public", Topics: []string{"records"}, Plaintext: true,
		OffsetGroup: "gh105-checkpoints", ConsumerGroup: "gh105-managed", InitialOffset: "earliest", ResetOffset: "error", MaxGroupSessions: 2}
}

func testDependencies(t testing.TB, values ...Settings) (Dependencies, *adapters.Inbox[Result]) {
	t.Helper()
	policy, err := Compose(values...)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		drain(t, ctx, inbox)
	})
	return Dependencies{Runtime: runtime, Evidence: inbox, Transactions: &TransactionIDs{}}, inbox
}

func drain(t testing.TB, ctx context.Context, inbox *adapters.Inbox[Result]) {
	t.Helper()
	for status, _ := inbox.Inspect(); status.Outstanding > 0; status, _ = inbox.Inspect() {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		if err := delivery.Ack(); err != nil {
			t.Error(err)
			return
		}
	}
}

func ack(t testing.TB, inbox *adapters.Inbox[Result]) {
	t.Helper()
	delivery, err := inbox.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}

func testOpen(t testing.TB, settings Settings, deps Dependencies) *Owner {
	t.Helper()
	owner, err := Open(context.Background(), settings, deps)
	if owner != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := owner.Close(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func await(t testing.TB, receipt *adapters.Receipt[Result], err error) Result {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(testContext(t))
	if err != nil || snapshot.Err() != nil {
		logSafeCauses(t, combineErrors(err, snapshot.Err()), 0)
		t.Fatal("native operation", combineErrors(err, snapshot.Err()))
	}
	value, present := snapshot.ValueCopy()
	if !present {
		t.Fatal("missing result")
	}
	return value
}

func logSafeCauses(t testing.TB, err error, depth int) {
	if err == nil || depth > 16 {
		return
	}
	if _, ok := err.(failure.Occurrence); ok {
		value, _ := failure.Inspect(err)
		t.Logf("public code=%s operation=%s", value.Diagnostic().Definition.Code, value.Diagnostic().Location.Operation)
	} else if value, ok := err.(*kerr.Error); ok {
		t.Logf("broker error code=%d", value.Code)
	} else {
		t.Logf("cause type=%T", err)
	}
	switch value := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range value.Unwrap() {
			logSafeCauses(t, cause, depth+1)
		}
	case interface{ Unwrap() error }:
		logSafeCauses(t, value.Unwrap(), depth+1)
	}
}

func send(t testing.TB, client *Client, messages ...Message) Result {
	t.Helper()
	receipt, err := client.Produce(testContext(t), messages)
	return await(t, receipt, err)
}

func observeNative(t testing.TB, peer *kfake.Cluster, position Position) *kgo.Record {
	t.Helper()
	reader, err := kgo.NewClient(kgo.SeedBrokers(peer.ListenAddrs()...), kgo.DisableClientMetrics(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{position.Topic: {position.Partition: kgo.NewOffset().At(position.Offset)}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	fetched := reader.PollRecords(testContext(t), 1)
	if err := fetched.Err(); err != nil {
		t.Fatal(err)
	}
	records := fetched.Records()
	if len(records) != 1 {
		t.Fatal("independent record absent")
	}
	return records[0]
}

func TestFailedOpenRetainsCleanupOwner(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	options.ClusterID = "wrong"
	deps, inbox := testDependencies(t, options)
	partial, err := Open(testContext(t), options, deps)
	if partial == nil || !errors.Is(err, ErrIdentity) {
		t.Fatal("failed readiness lost cleanup owner", err)
	}
	if err := partial.Close(testContext(t)); err != nil || !partial.ShutdownComplete() {
		t.Fatal("partial cleanup did not finish", err)
	}
	ack(t, inbox)
}
