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
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProduceCopiesContentAndPreservesNullEmpty(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 4)
	message := Message{Topic: "records", Partition: 0, Key: []byte("key"), Value: []byte("value"),
		Headers: []Header{{Key: "duplicate", Value: nil}, {Key: "duplicate", Value: []byte{}}, {Key: "binary", Value: []byte{0, 255}}}}
	receipt, err := fixture.client.Produce(deadline(t), correlation("copy"), []Message{message, {Topic: "records", Value: nil}, {Topic: "records", Value: []byte{}}})
	message.Key[0] = 'x'
	message.Value[0] = 'x'
	message.Headers[2].Value[1] = 0
	message.Headers[0].Key = "changed"
	result := settle(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	writes := result.Outcome.Value.WritesCopy()
	if len(writes) != 3 {
		t.Fatal("missing per-record evidence")
	}
	for index, write := range writes {
		if write.State != WriteAcknowledged || !write.IdentityChecked || write.Position.Offset != int64(index) {
			t.Fatal("invalid ACK coordinates")
		}
	}
	got := observe(t, cluster.ListenAddrs(), "records", 0, 0, 3)
	if string(got[0].Key) != "key" || string(got[0].Value) != "value" || got[1].Value != nil || got[2].Value == nil || len(got[2].Value) != 0 ||
		len(got[0].Headers) != 3 || got[0].Headers[0].Key != "duplicate" || got[0].Headers[0].Value != nil ||
		got[0].Headers[1].Value == nil || !bytes.Equal(got[0].Headers[2].Value, []byte{0, 255}) {
		t.Fatal("content/aliasing contract broken")
	}
	writes[0].Position.Offset = 99
	if result.Outcome.Value.WritesCopy()[0].Position.Offset != 0 {
		t.Fatal("evidence was mutable")
	}
}
func TestProducePartialSuccess(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 2)
	receipt, err := fixture.client.Produce(deadline(t), correlation("partial"), []Message{{Topic: "records", Value: []byte("stored")},
		{Topic: "records", Partition: 99, Value: []byte("invalid-partition")}})
	result := settle(t, receipt, err)
	writes := result.Outcome.Value.WritesCopy()
	if result.Err() == nil || len(writes) != 2 || writes[0].State != WriteAcknowledged || writes[1].State == WriteAcknowledged || writes[1].Err == nil {
		t.Fatal("partial effects collapsed")
	}
	if string(observe(t, cluster.ListenAddrs(), "records", 0, 0, 1)[0].Value) != "stored" {
		t.Fatal("independent stored result missing")
	}
}
func TestCancelledWaitRetainsLateEvidenceAndAdmission(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.MaxActive = 1
	fixture := bindFixture(t, options, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		close(entered)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	receipt, err := fixture.client.Produce(deadline(t), correlation("late"), []Message{{Topic: "records", Value: []byte("late")}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("no request")
	}
	stopped, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("synthetic-private-cause")
	cancel(cause)
	if _, err := receipt.Wait(stopped); !errors.Is(err, cause) {
		t.Fatal("wait cause lost")
	}
	if _, err := fixture.client.Produce(deadline(t), correlation("full"), []Message{{Topic: "records"}}); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("saturated evidence admitted produce", err)
	}
	delivery, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); !errors.Is(err, invocation.ErrPending) {
		t.Fatal("early evidence release")
	}
	unblock()
	result := settle(t, receipt, nil)
	if result.Err() != nil || result.Outcome.Value.WritesCopy()[0].Position.Offset != 0 {
		t.Fatal("late ACK lost")
	}
	independent, err := delivery.Receipt().WaitReleased(deadline(t))
	if err != nil || independent.Context.Correlation.Call != "late" || independent.Outcome.Value.WritesCopy()[0].State != WriteAcknowledged {
		t.Fatal("independent evidence lost")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	observe(t, cluster.ListenAddrs(), "records", 0, 0, 1)
}

func TestCancelledDeliveryDoesNotProveAbsence(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		close(entered)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	ctx, cancel := context.WithCancelCause(context.Background())
	receipt, err := fixture.client.Produce(ctx, correlation("canceled-delivery"), []Message{{Topic: "records", Value: []byte("stored-late")}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("no request")
	}
	cause := errors.New("private-cancel-cause")
	cancel(cause)
	unblock()
	result := settle(t, receipt, nil)
	if !errors.Is(result.Err(), cause) {
		t.Fatal("cancel cause lost")
	}
	write := result.Outcome.Value.WritesCopy()[0]
	if write.State != WriteAcknowledged || write.Position.Offset != 0 || write.IdentityChecked {
		t.Fatal("late ACK or unavailable identity check misreported")
	}
	if string(observe(t, cluster.ListenAddrs(), "records", 0, 0, 1)[0].Value) != "stored-late" {
		t.Fatal("native effect missing")
	}
}

func TestProduceInterleavingAndPartitionOrder(t *testing.T) {
	cluster := localCluster(t)
	fixture := bindFixture(t, clusterOptions(cluster), 8)
	first := produce(t, fixture.client, "a1", Message{Topic: "records", Value: []byte("a1")}).WritesCopy()[0]
	native := nativeClient(t, cluster.ListenAddrs())
	nativeProduce(t, native, &kgo.Record{Topic: "records", Value: []byte("foreign")})
	last := produce(t, fixture.client, "a2", Message{Topic: "records", Value: []byte("a2")}, Message{Topic: "records", Partition: 1, Value: []byte("p1")}).WritesCopy()
	if first.Position.Offset != 0 || last[0].Position.Offset != 2 || last[1].Position.Offset != 0 {
		t.Fatal("coordinate fixture changed")
	}
	receipt, err := fixture.client.ReadPositions(deadline(t), correlation("exact"), []Position{first.Position, last[0].Position, last[1].Position})
	result := settle(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	for index, want := range []string{"a1", "a2", "p1"} {
		got := result.Outcome.Value.ReadsCopy()[index]
		if got.State != ReadFound || string(got.Record.ValueCopy()) != want {
			t.Fatal("exact-set read included foreign data")
		}
	}
	receipt, err = fixture.client.ReadRange(deadline(t), correlation("range"), Range{Start: first.Position, End: 3})
	result = settle(t, receipt, err)
	page := result.Outcome.Value.Page()
	if result.Err() != nil || !page.Complete() || len(page.RecordsCopy()) != 3 || string(page.RecordsCopy()[1].ValueCopy()) != "foreign" {
		t.Fatal("physical interval was falsely filtered or incomplete", result.Err())
	}
}
