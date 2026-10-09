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

package otel_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	logpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type failureFixture struct {
	owner     *otel.Owner
	runtime   *adapters.Runtime
	inbox     *adapters.Inbox[otel.Result]
	wantClose error
	records   []adapters.Snapshot[otel.Result]
	finished  bool
}

func failureOwner(t *testing.T, config otel.Settings, wantOpen, wantClose error) (*failureFixture, error) {
	t.Helper()
	prepared, err := otel.Prepare(config)
	if err != nil {
		t.Fatal("offline preparation failed", err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[otel.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, openErr := prepared.Open(context.Background(), otel.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner == nil {
		_ = runtime.Close(context.Background())
		t.Fatal("construction did not return cleanup ownership", openErr)
	}
	fixture := &failureFixture{owner: owner, runtime: runtime, inbox: inbox, wantClose: wantClose}
	t.Cleanup(func() { fixture.finish(t) })
	if wantOpen == nil && openErr != nil || wantOpen != nil && !errors.Is(openErr, wantOpen) {
		t.Fatal("unexpected constructor result", openErr)
	}
	return fixture, openErr
}

func (fixture *failureFixture) receive(t *testing.T) adapters.Snapshot[otel.Result] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	delivery, err := fixture.inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal("independent evidence unavailable", err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	fixture.records = append(fixture.records, snapshot)
	return snapshot
}

func (fixture *failureFixture) completed(t *testing.T, receipt *adapters.Receipt[otel.Result], submit error) adapters.Snapshot[otel.Result] {
	t.Helper()
	if submit != nil || receipt == nil {
		t.Fatal("operation was not publicly admitted", submit)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	direct, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal("accepted work did not terminate", err)
	}
	evidence := fixture.receive(t)
	if evidence.Info().Sequence != direct.Info().Sequence {
		t.Fatal("independent receiver observed different operation")
	}
	return direct
}

func (fixture *failureFixture) finish(t *testing.T) {
	t.Helper()
	if fixture.finished {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := fixture.owner.Close(ctx)
	if fixture.wantClose == nil && err != nil || fixture.wantClose != nil && !errors.Is(err, fixture.wantClose) {
		t.Error("unexpected source cleanup", err)
	}
	if !fixture.owner.ShutdownComplete() {
		t.Error("actual source ownership remained")
		return
	}
	if err := fixture.runtime.Close(ctx); err != nil {
		t.Error(err)
		return
	}
	if err := fixture.inbox.Seal(); err != nil {
		t.Error(err)
		return
	}
	for {
		status, err := fixture.inbox.Inspect()
		if err != nil {
			t.Error(err)
			return
		}
		if status.Outstanding == 0 {
			break
		}
		fixture.receive(t)
	}
	fixture.finished = true
}

func (fixture *failureFixture) sourceEvidence(t *testing.T) adapters.Snapshot[otel.Result] {
	t.Helper()
	fixture.finish(t)
	for _, record := range fixture.records {
		if record.Info().Operation == "telemetry.otel.open" {
			value, present := record.ValueCopy()
			if !present || value.HasData() || len(value.SignalsCopy()) != 0 {
				t.Fatal("cleanup source metadata or no-signal distinction lost")
			}
			return record
		}
	}
	t.Fatal("independent source cleanup evidence missing")
	return adapters.Snapshot[otel.Result]{}
}

func TestPublicSaturatedTraceAndIndependentLogSource(t *testing.T) {
	type correlated struct {
		message     string
		trace, span []byte
	}
	logs := make(chan correlated, 2)
	traces := make(chan string, 2)
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(io.LimitReader(request.Body, 2<<20))
		if err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		switch request.URL.Path {
		case "/logs":
			var message logpb.ExportLogsServiceRequest
			if err := proto.Unmarshal(data, &message); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, resource := range message.ResourceLogs {
				for _, scope := range resource.ScopeLogs {
					for _, record := range scope.LogRecords {
						logs <- correlated{record.GetBody().GetStringValue(), append([]byte(nil), record.TraceId...), append([]byte(nil), record.SpanId...)}
					}
				}
			}
		case "/traces":
			var message tracepb.ExportTraceServiceRequest
			if err := proto.Unmarshal(data, &message); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, resource := range message.ResourceSpans {
				for _, scope := range resource.ScopeSpans {
					for _, span := range scope.Spans {
						traces <- span.Name
					}
				}
			}
		default:
			t.Error("unexpected route")
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(peer.Close)
	active := 1
	traceOwner, _ := failureOwner(t, otel.Settings{Name: "one-trace", ServiceName: "fixture", TracesEndpoint: peer.URL + "/traces", ActiveCalls: &active}, nil, nil)
	logOwner, _ := failureOwner(t, otel.Settings{Name: "one-log", ServiceName: "fixture", LogsEndpoint: peer.URL + "/logs", ActiveCalls: &active}, nil, nil)
	associated, span, err := traceOwner.owner.Client().Start(context.Background(), otel.SpanInput{Name: "held"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = span.End(ctx)
	})
	call, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	refused, err := traceOwner.owner.Client().Flush(call)
	stop()
	if refused != nil || !(errors.Is(err, adapters.ErrEvidence) || errors.Is(err, adapters.ErrLimit)) {
		t.Fatal("saturated source did not report pre-admission refusal", err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatal("saturated refusal depended on timing out the caller", err)
	}
	if !span.IsRecording() {
		t.Fatal("refused Flush ended the retained span")
	}
	receipt, err := logOwner.owner.Client().Emit(associated, otel.LogRecord{Message: "isolated"})
	if result := logOwner.completed(t, receipt, err); result.Err() != nil {
		t.Fatal("separate log source could not admit correlation", result.Err())
	}
	receipt, err = logOwner.owner.Client().Flush(context.Background())
	if result := logOwner.completed(t, receipt, err); result.Err() != nil {
		t.Fatal("separate log source could not export", result.Err())
	}
	select {
	case record := <-logs:
		traceID, spanID := span.SpanContext().TraceID(), span.SpanContext().SpanID()
		if record.message != "isolated" || !bytes.Equal(record.trace, traceID[:]) || !bytes.Equal(record.span, spanID[:]) {
			t.Fatal("separate source lost original trace association")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("isolated log did not reach actual receiver")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := traceOwner.owner.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Close falsely completed", err)
	}
	if traceOwner.owner.ShutdownComplete() || !span.IsRecording() {
		t.Fatal("Close released or implicitly ended the live span")
	}
	fresh, cancelFresh := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFresh()
	receipt, err = span.End(fresh)
	if err != nil {
		t.Fatal("fresh cleanup context could not end retained span", err)
	}
	if snapshot, err := receipt.WaitReleased(fresh); err != nil || snapshot.Err() != nil {
		t.Fatal("span work did not actually join", err, snapshot.Err())
	}
	source := traceOwner.sourceEvidence(t)
	if source.Err() != nil {
		t.Fatal("trace cleanup evidence failed", source.Err())
	}
	select {
	case name := <-traces:
		if name != "held" {
			t.Fatal("wrong retired trace")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("final trace export missing")
	}
	if source := logOwner.sourceEvidence(t); source.Err() != nil {
		t.Fatal("log cleanup evidence failed", source.Err())
	}
}

func TestPublicSingleQueueItemKeepsAcceptedRecord(t *testing.T) {
	records := make(chan managedRecord, 4)
	server := managedPeer(t, "queue", records)
	queue := 1
	config := otel.Settings{Name: "queue-one", ServiceName: "fixture", LogsEndpoint: server.URL + "/v1/logs", QueueItems: &queue}
	prepared, err := otel.Prepare(config)
	if err != nil || prepared.Metadata().QueueItems != 1 || prepared.Metadata().BatchSize != 1 {
		t.Fatal("dependent default did not use the selected queue size", err)
	}
	zero := 0
	invalid := config
	invalid.BatchSize = &zero
	if err := otel.Validate(invalid); err == nil {
		t.Fatal("explicit zero batch size was silently defaulted")
	}
	fixture, _ := failureOwner(t, config, nil, nil)
	receipt, err := fixture.owner.Client().Emit(context.Background(), otel.LogRecord{Message: "kept"})
	if snapshot := fixture.completed(t, receipt, err); snapshot.Err() != nil {
		t.Fatal(snapshot.Err())
	}
	receipt, err = fixture.owner.Client().Emit(context.Background(), otel.LogRecord{Message: "refused"})
	if snapshot := fixture.completed(t, receipt, err); !errors.Is(snapshot.Err(), otel.ErrLimit) {
		t.Fatal("full native queue did not retain event failure", snapshot.Err())
	}
	receipt, err = fixture.owner.Client().Flush(context.Background())
	if snapshot := fixture.completed(t, receipt, err); snapshot.Err() != nil {
		t.Fatal(snapshot.Err())
	}
	managedAwait(t, records, managedRecord{"queue", "logs", "kept"})
	receipt, err = fixture.owner.Client().Emit(context.Background(), otel.LogRecord{Message: "next"})
	if snapshot := fixture.completed(t, receipt, err); snapshot.Err() != nil {
		t.Fatal("queue refusal damaged subsequent event", snapshot.Err())
	}
	receipt, err = fixture.owner.Client().Flush(context.Background())
	if snapshot := fixture.completed(t, receipt, err); snapshot.Err() != nil {
		t.Fatal(snapshot.Err())
	}
	managedAwait(t, records, managedRecord{"queue", "logs", "next"})
	if source := fixture.sourceEvidence(t); source.Err() != nil {
		t.Fatal(source.Err())
	}
	select {
	case record := <-records:
		t.Fatal("refused event was emitted", record)
	default:
	}
}

func TestPublicPartialNativeConstructionRetainsCleanupOwner(t *testing.T) {
	fixture, original := failureOwner(t, otel.Settings{Name: "invalid-native-tls", ServiceName: "fixture",
		LogsEndpoint: "https://127.0.0.1:1/logs", TLS: &otel.TLS{CA: "not a PEM certificate"}}, otel.ErrInput, nil)
	if fixture.owner.Client() != nil {
		t.Fatal("failed construction exposed a usable client")
	}
	source := fixture.sourceEvidence(t)
	value, _ := source.ValueCopy()
	if !errors.Is(source.Primary(), otel.ErrInput) || !errors.Is(source.Primary(), original) || source.Cleanup() != nil || value.Source().Name != "invalid-native-tls" {
		t.Fatal("failed construction lost original error, source identity or cleanup evidence", source.Primary(), source.Cleanup())
	}
	if released := fixture.owner.Release(context.Background()); !released.Complete || released.Err != nil {
		t.Fatal("failed candidate cleanup remained owned", released.Err)
	}
}

func TestPublicFinalExportReportsUndeliveredAndReleasesSocket(t *testing.T) {
	closed := make(chan struct{}, 2)
	var mu sync.Mutex
	var submitted []string
	peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(io.LimitReader(request.Body, 2<<20))
		if err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var message logpb.ExportLogsServiceRequest
		if err := proto.Unmarshal(data, &message); err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		for _, resource := range message.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				for _, record := range scope.LogRecords {
					submitted = append(submitted, record.GetBody().GetStringValue())
				}
			}
		}
		mu.Unlock()
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	peer.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	peer.Start()
	t.Cleanup(peer.Close)
	queue, batch := 2, 1
	fixture, _ := failureOwner(t, otel.Settings{Name: "undelivered", ServiceName: "fixture", LogsEndpoint: peer.URL + "/logs", QueueItems: &queue, BatchSize: &batch}, nil, otel.ErrUndelivered)
	for _, message := range []string{"submitted-once", "never-submitted"} {
		receipt, err := fixture.owner.Client().Emit(context.Background(), otel.LogRecord{Message: message})
		if snapshot := fixture.completed(t, receipt, err); snapshot.Err() != nil {
			t.Fatal(snapshot.Err())
		}
	}
	source := fixture.sourceEvidence(t)
	if source.Primary() != nil || !errors.Is(source.Cleanup(), otel.ErrUndelivered) {
		t.Fatal("undelivered final evidence missing", source.Primary(), source.Cleanup())
	}
	if released := fixture.owner.Release(context.Background()); !released.Complete || !errors.Is(released.Err, otel.ErrUndelivered) {
		t.Fatal("cleanup errors erased or mistaken for retained ownership", released.Err)
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("source did not physically close its failed-export socket")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(submitted) != 1 || submitted[0] != "submitted-once" {
		t.Fatal("final export retried or submitted the untouched second batch", submitted)
	}
}
