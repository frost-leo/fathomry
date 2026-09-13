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
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestQueuedTransactionCancellationDoesNotFenceActiveProducer(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.TransactionalID, options.CleanupTimeout = "queued-transaction", 50*time.Millisecond
	fixture := bindFixture(t, options, 4)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		close(entered)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	active, err := fixture.client.ProduceTransaction(deadline(t), correlation("active"), []Message{{Topic: "records", Value: []byte("active")}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("active transaction did not submit")
	}
	ctx, cancel := context.WithCancel(context.Background())
	queued, err := fixture.client.ProduceTransaction(ctx, correlation("queued"), []Message{{Topic: "records", Value: []byte("queued")}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	result := settle(t, queued, nil)
	if !errors.Is(result.Err(), context.Canceled) || result.Outcome.Value.Transaction() != TransactionNotStarted {
		t.Fatal("queued cancellation was not explicit")
	}
	time.Sleep(100 * time.Millisecond)
	unblock()
	result = settle(t, active, nil)
	if result.Err() != nil || result.Outcome.Value.Transaction() != TransactionCommitted {
		t.Fatal("queued cancellation stopped another transaction", result.Err())
	}
	if string(observe(t, cluster.ListenAddrs(), "records", 0, 0, 1)[0].Value) != "active" {
		t.Fatal("active transaction's independent effect missing")
	}
}

func TestTransactionCommitAcrossPartitionsAndAbortOnPartialFailure(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.TransactionalID = "product-transaction"
	fixture := bindFixture(t, options, 8)
	receipt, err := fixture.client.ProduceTransaction(deadline(t), correlation("commit"), []Message{
		{Topic: "records", Value: []byte("p0")}, {Topic: "records", Partition: 1, Value: []byte("p1")}})
	result := settle(t, receipt, err)
	if result.Err() != nil || result.Outcome.Value.Transaction() != TransactionCommitted {
		t.Fatal("commit failed", result.Err())
	}
	for partition, want := range []string{"p0", "p1"} {
		if string(observe(t, cluster.ListenAddrs(), "records", int32(partition), 0, 1)[0].Value) != want {
			t.Fatal("committed partition missing")
		}
	}
	receipt, err = fixture.client.ProduceTransaction(deadline(t), correlation("abort"), []Message{{Topic: "records", Value: []byte("abort")}, {Topic: "records", Partition: 99}})
	result = settle(t, receipt, err)
	if result.Err() == nil || result.Outcome.Value.Transaction() != TransactionAborted {
		t.Fatal("partial transaction committed", result.Err())
	}
	writes := result.Outcome.Value.WritesCopy()
	if writes[0].State != WriteAcknowledged {
		t.Fatal("abort erased the actual produce ACK")
	}
	got := readResult(t, fixture.client, "aborted-position", writes[0].Position)
	if got.State != ReadMissing {
		t.Fatal("aborted transaction visible", got.Err)
	}
}
func TestUnconfirmedCommitFencesNextTransaction(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.TransactionalID = "uncertain-product"
	fixture := bindFixture(t, options, 8)
	cluster.ControlKey(int16(kmsg.EndTxn), func(req kmsg.Request) (kmsg.Response, error, bool) {
		response := req.ResponseKind().(*kmsg.EndTxnResponse)
		response.ErrorCode = kerr.UnknownServerError.Code
		return response, nil, true
	})
	receipt, err := fixture.client.ProduceTransaction(deadline(t), correlation("uncertain"), []Message{{Topic: "records", Value: []byte("uncertain")}})
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), kerr.UnknownServerError) || result.Outcome.Value.Transaction() != TransactionUnknown {
		t.Fatal("unconfirmed commit became known")
	}
	receipt, err = fixture.client.ProduceTransaction(deadline(t), correlation("later"), []Message{{Topic: "records", Value: []byte("must-not-join")}})
	later := settle(t, receipt, err)
	if !errors.Is(later.Err(), ErrState) || later.Outcome.Value.Transaction() != TransactionNotStarted || later.Outcome.Value.WritesCopy()[0].State != WriteNotSubmitted {
		t.Fatal("uncertain transaction contaminated later work")
	}
	// Ordinary production uses an independent native producer, not the fenced
	// transaction. This checks that a nil/nontransaction path cannot commit it.
	produce(t, fixture.client, "ordinary", Message{Topic: "references", Value: []byte("separate")})
}
func TestPrimaryAndAbortFailureRemainSeparate(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.TransactionalID = "abort-failure"
	fixture := bindFixture(t, options, 4)
	cluster.ControlKey(int16(kmsg.EndTxn), func(req kmsg.Request) (kmsg.Response, error, bool) {
		response := req.ResponseKind().(*kmsg.EndTxnResponse)
		response.ErrorCode = kerr.UnknownServerError.Code
		return response, nil, true
	})
	receipt, err := fixture.client.ProduceTransaction(deadline(t), correlation("both"), []Message{{Topic: "records", Value: []byte("unconfirmed")}, {Topic: "records", Partition: 99}})
	result := settle(t, receipt, err)
	if !errors.Is(result.Outcome.Primary, ErrProduce) || !errors.Is(result.Outcome.Cleanup, ErrCleanup) ||
		!errors.Is(result.Err(), kerr.UnknownServerError) || result.Outcome.Value.Transaction() != TransactionUnknown {
		t.Fatal("primary/cleanup/effects collapsed")
	}
}
