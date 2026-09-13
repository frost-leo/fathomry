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

package otelbridge

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	franz "github.com/frost-leo/fathomry/internal/broker/franz/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kfake"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

func TestKafkaPropagationIntegration(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.ClusterID("trace-test"), kfake.SeedTopics(1, "trace"),
		kfake.ListenFn(func(network, _ string) (net.Listener, error) { return net.Listen(network, "127.0.0.1:0") }))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	options := franz.OptionsV1{Name: "trace", Brokers: cluster.ListenAddrs(), ClusterID: "trace-test", Topics: []string{"trace"}, Plaintext: true}
	selected, err := franz.Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, franz.LimitsV1(options))
	assembly, err := resource.Assemble(ctx, ctx, "trace", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	inbox, err := invocation.NewInbox[franz.Result](2, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	client, err := franz.Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled})
	headers, err := Inject(trace.ContextWithSpanContext(ctx, span), []franz.Header{{Key: "binary", Value: []byte{0, 255}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := client.Produce(ctx, fault.Correlation{Call: "write"}, []franz.Message{{Topic: "trace", Headers: headers, Value: []byte("data")}})
	if err != nil {
		t.Fatal(err)
	}
	written, err := receipt.WaitReleased(ctx)
	if err != nil || written.Err() != nil {
		t.Fatal("Kafka producer failed")
	}
	receipt, err = client.ReadExact(ctx, fault.Correlation{Call: "read"}, written.Outcome.Value.WritesCopy()[0].Position)
	if err != nil {
		t.Fatal(err)
	}
	read, err := receipt.WaitReleased(ctx)
	if err != nil || read.Err() != nil {
		t.Fatal("Kafka exact read failed")
	}
	received, err := Extract(context.Background(), read.Outcome.Value.ReadsCopy()[0].Record.HeadersCopy(), false)
	if err != nil || trace.SpanContextFromContext(received).TraceID() != span.TraceID() || trace.SpanContextFromContext(received).SpanID() != span.SpanID() {
		t.Fatal("context association did not survive the actual Kafka boundary")
	}
	for range 2 {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := delivery.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTraceContextAndOrdinaryHeaders(t *testing.T) {
	span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), span)
	member, err := baggage.NewMember("example", "explicit")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)
	original := []franz.Header{{Key: "duplicate", Value: nil}, {Key: "duplicate", Value: []byte{}}, {Key: "binary", Value: []byte{0, 255}}}
	headers, err := Inject(ctx, original, false)
	if err != nil {
		t.Fatal(err)
	}
	original[2].Value[1] = 0
	if !bytes.Equal(headers[2].Value, []byte{0, 255}) || headers[0].Value != nil || headers[1].Value == nil {
		t.Fatal("ordinary headers changed/aliased")
	}
	received, err := Extract(context.Background(), headers, false)
	got := trace.SpanContextFromContext(received)
	if err != nil || got.TraceID() != span.TraceID() || got.SpanID() != span.SpanID() || baggage.FromContext(received).Len() != 0 {
		t.Fatal("trace round trip or baggage opt-in failed")
	}
	headers, err = Inject(ctx, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	received, err = Extract(context.Background(), headers, true)
	if err != nil || baggage.FromContext(received).Member("example").Value() != "explicit" {
		t.Fatal("explicit baggage missing")
	}
}
func TestTraceConflictsAndBounds(t *testing.T) {
	for _, headers := range [][]franz.Header{
		{{Key: "traceparent", Value: []byte("a")}, {Key: "TraceParent", Value: []byte("b")}},
		make([]franz.Header, 65), {{Key: "binary", Value: make([]byte, maxHeaderBytes+1)}},
	} {
		if _, err := Extract(context.Background(), headers, false); !errors.Is(err, franz.ErrInput) {
			t.Fatal("conflict/bounds accepted", err)
		}
	}
	if _, err := Inject(context.Background(), []franz.Header{{Key: "traceparent"}}, false); !errors.Is(err, franz.ErrInput) {
		t.Fatal("existing context overwritten")
	}
	if _, err := Inject(nil, nil, false); !errors.Is(err, franz.ErrInput) {
		t.Fatal("nil context accepted")
	}
	wrapped := failure("trace-test", errors.New("synthetic-private-cause"))
	var technical *fault.Error
	if !errors.As(wrapped, &technical) || technical.Diagnostic().Context.Provider != franz.ProviderID {
		t.Fatal("technical error context lost")
	}
}
