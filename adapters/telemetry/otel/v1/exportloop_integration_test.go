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
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type managedRecord struct{ endpoint, signal, name string }

func managedPeer(t *testing.T, endpoint string, records chan<- managedRecord) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(io.LimitReader(request.Body, 2<<20))
		if err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		switch request.URL.Path {
		case "/v1/logs":
			var message collectorlogs.ExportLogsServiceRequest
			if err := proto.Unmarshal(data, &message); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, resource := range message.ResourceLogs {
				for _, scope := range resource.ScopeLogs {
					for _, record := range scope.LogRecords {
						records <- managedRecord{endpoint, "logs", record.GetBody().GetStringValue()}
					}
				}
			}
		case "/v1/traces":
			var message collectortrace.ExportTraceServiceRequest
			if err := proto.Unmarshal(data, &message); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, resource := range message.ResourceSpans {
				for _, scope := range resource.ScopeSpans {
					for _, span := range scope.Spans {
						records <- managedRecord{endpoint, "traces", span.GetName()}
					}
				}
			}
		default:
			t.Error("unexpected OTLP route", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server
}

func managedRuntime(t *testing.T, policy otel.Policy) (*adapters.Runtime, otel.Dependencies, *framework.Receiver[otel.Result]) {
	t.Helper()
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[otel.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{},
		func(context.Context, adapters.Snapshot[otel.Result]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := receiver.Finish(ctx); err != nil {
			t.Error(err)
		}
	})
	return runtime, otel.Dependencies{Runtime: runtime, Evidence: inbox}, receiver
}

func managedWait(t *testing.T, receipt *adapters.Receipt[otel.Result], err error) {
	t.Helper()
	if err != nil || receipt == nil {
		t.Fatal("telemetry admission failed", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil {
		t.Fatal("telemetry operation failed", err, snapshot.Err())
	}
}

func managedEmit(t *testing.T, client *otel.Client, message string) {
	t.Helper()
	receipt, err := client.Emit(context.Background(), otel.LogRecord{Message: message})
	managedWait(t, receipt, err)
}

func managedAwait(t *testing.T, records <-chan managedRecord, expected ...managedRecord) {
	t.Helper()
	remaining := make(map[managedRecord]bool, len(expected))
	for _, value := range expected {
		remaining[value] = true
	}
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for len(remaining) != 0 {
		select {
		case value := <-records:
			if !remaining[value] {
				t.Fatal("unexpected, duplicated or misrouted telemetry record", value)
			}
			delete(remaining, value)
		case <-timer.C:
			t.Fatal("managed export did not deliver expected records", remaining)
		}
	}
}

func TestManagedExportPublicStartRestartAndFinalCleanup(t *testing.T) {
	records := make(chan managedRecord, 16)
	server := managedPeer(t, "direct", records)
	config := otel.Settings{Name: "managed-direct", ServiceName: "fixture", LogsEndpoint: server.URL + "/v1/logs"}
	policy, err := otel.Recommend(config)
	if err != nil {
		t.Fatal(err)
	}
	_, dependencies, _ := managedRuntime(t, policy)
	owner, err := otel.Open(context.Background(), config, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	for _, options := range []otel.ExportOptions{
		{}, {Interval: time.Millisecond - 1, Timeout: time.Second}, {Interval: 25 * time.Hour, Timeout: time.Second},
		{Interval: time.Second, Timeout: time.Millisecond - 1}, {Interval: time.Second, Timeout: time.Minute + 1},
	} {
		if loop, err := owner.StartExport(context.Background(), options); loop != nil || !errors.Is(err, otel.ErrInput) {
			t.Fatal("invalid export schedule admitted", err)
		}
	}
	options := otel.ExportOptions{Interval: 10 * time.Millisecond, Timeout: time.Second}
	first, err := owner.StartExport(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := owner.StartExport(context.Background(), options); duplicate != nil || !errors.Is(err, otel.ErrState) {
		t.Fatal("duplicate source schedule admitted", err)
	}
	managedEmit(t, owner.Client(), "first")
	managedAwait(t, records, managedRecord{"direct", "logs", "first"})
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		status, err := first.Status()
		if err != nil {
			t.Fatal(err)
		}
		if status.Succeeded > 0 && !status.Running {
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("receiver observation did not become a completed export")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := first.Status()
	if err != nil || !status.Stopped || status.Running || status.Succeeded == 0 {
		t.Fatal("schedule did not join", status, err)
	}
	managedEmit(t, owner.Client(), "restarted")
	second, err := owner.StartExport(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	managedAwait(t, records, managedRecord{"direct", "logs", "restarted"})
	if err := second.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	managedEmit(t, owner.Client(), "final")
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		t.Fatal("final export or native shutdown failed", err)
	}
	managedAwait(t, records, managedRecord{"direct", "logs", "final"})
	if loop, err := owner.StartExport(context.Background(), options); loop != nil || !errors.Is(err, otel.ErrState) {
		t.Fatal("closed source restarted scheduling", err)
	}
}

func TestManagedExportFollowRetainsOldGenerationAndFailedCandidateCleanup(t *testing.T) {
	records := make(chan managedRecord, 32)
	oldPeer := managedPeer(t, "old", records)
	newPeer := managedPeer(t, "new", records)
	active := 2
	prepare := func(name string, peer *httptest.Server) otel.Prepared {
		value, err := otel.Prepare(otel.Settings{Name: name, ServiceName: "fixture", ActiveCalls: &active,
			LogsEndpoint: peer.URL + "/v1/logs", TracesEndpoint: peer.URL + "/v1/traces"})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	oldPrepared, newPrepared := prepare("old", oldPeer), prepare("new", newPeer)
	policy, err := otel.Compose(oldPrepared, newPrepared)
	if err != nil {
		t.Fatal(err)
	}
	_, dependencies, _ := managedRuntime(t, policy)
	scope, err := resource.New(context.Background(), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := scope.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	var mu sync.Mutex
	owners := make(map[int]*otel.Owner)
	loops := make(map[int]*otel.ExportLoop)
	ref, err := resource.Bind(scope, resource.Binding[int, otel.Handle]{
		Name: "telemetry", Policy: resource.Follow,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[int](view)
			if err != nil {
				return 0, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
		Build: func(ctx context.Context, value int) (*resource.Instance[otel.Handle], error) {
			prepared := oldPrepared
			if value == 1 {
				prepared = newPrepared
			}
			owner, err := prepared.Open(ctx, dependencies)
			if owner == nil {
				return nil, err
			}
			mu.Lock()
			owners[value] = owner
			mu.Unlock()
			instance := &resource.Instance[otel.Handle]{Value: owner.Handle(), Release: owner.Release}
			if err != nil {
				return instance, err
			}
			loop, err := owner.StartExport(ctx, otel.ExportOptions{Interval: 10 * time.Millisecond, Timeout: time.Second})
			if err != nil {
				return instance, err
			}
			mu.Lock()
			loops[value] = loop
			mu.Unlock()
			if value == 2 {
				return instance, errors.New("synthetic-candidate-refusal")
			}
			return instance, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(value int) error {
		snapshot, err := settings.New(value, func(value int) int { return value })
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		update, err := scope.Apply(ctx, snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	if err := apply(0); err != nil {
		t.Fatal(err)
	}
	client, err := otel.Using(context.Background(), ref, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	_, span, err := client.Start(context.Background(), otel.SpanInput{Name: "held-old-span"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = span.End(ctx)
	})
	if err := apply(1); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	oldOwner, newOwner := owners[0], owners[1]
	oldLoop, newLoop := loops[0], loops[1]
	mu.Unlock()
	if oldOwner.ShutdownComplete() {
		t.Fatal("retained old span lost its source")
	}
	managedEmit(t, oldOwner.Client(), "old-after-adoption")
	managedEmit(t, client, "new-after-adoption")
	managedAwait(t, records, managedRecord{"old", "logs", "old-after-adoption"}, managedRecord{"new", "logs", "new-after-adoption"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	receipt, err := span.End(ctx)
	managedWait(t, receipt, err)
	managedAwait(t, records, managedRecord{"old", "traces", "held-old-span"})
	waitStopped := func(owner *otel.Owner, loop *otel.ExportLoop) {
		t.Helper()
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			status, err := loop.Status()
			if err != nil {
				t.Fatal(err)
			}
			if owner.ShutdownComplete() && status.Stopped {
				return
			}
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Fatal("retired/candidate generation or export loop was retained")
			}
		}
	}
	waitStopped(oldOwner, oldLoop)
	if err := apply(2); err == nil {
		t.Fatal("failed candidate adopted")
	}
	mu.Lock()
	failedOwner, failedLoop := owners[2], loops[2]
	mu.Unlock()
	if failedOwner == nil || failedLoop == nil {
		t.Fatal("candidate construction control did not execute")
	}
	waitStopped(failedOwner, failedLoop)
	managedEmit(t, client, "last-good")
	managedAwait(t, records, managedRecord{"new", "logs", "last-good"})
	if err := scope.Close(ctx); err != nil {
		t.Fatal(err)
	}
	waitStopped(newOwner, newLoop)
}

func TestManagedExportPartialFailureRemainsHistoricalAfterSuccess(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(io.LimitReader(request.Body, 2<<20))
		if err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var message collectorlogs.ExportLogsServiceRequest
		if err := proto.Unmarshal(data, &message); err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		rejected := int64(0)
		for _, resource := range message.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				for _, record := range scope.LogRecords {
					name := record.GetBody().GetStringValue()
					mu.Lock()
					seen[name]++
					mu.Unlock()
					if name == "reject-once" {
						rejected++
					}
				}
			}
		}
		response := &collectorlogs.ExportLogsServiceResponse{}
		if rejected > 0 {
			response.PartialSuccess = &collectorlogs.ExportLogsPartialSuccess{RejectedLogRecords: rejected, ErrorMessage: "synthetic-private-receiver-detail"}
		}
		encoded, err := proto.Marshal(response)
		if err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = writer.Write(encoded)
	}))
	t.Cleanup(server.Close)
	config := otel.Settings{Name: "partial", ServiceName: "fixture", LogsEndpoint: server.URL + "/v1/logs"}
	policy, err := otel.Recommend(config)
	if err != nil {
		t.Fatal(err)
	}
	_, dependencies, _ := managedRuntime(t, policy)
	owner, err := otel.Open(context.Background(), config, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	loop, err := owner.StartExport(context.Background(), otel.ExportOptions{Interval: 10 * time.Millisecond, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	wait := func(check func(otel.ExportStatus) bool) otel.ExportStatus {
		t.Helper()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		for {
			status, err := loop.Status()
			if err != nil {
				t.Fatal(err)
			}
			if check(status) {
				return status
			}
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Fatal("export status did not progress", status)
			}
		}
	}
	managedEmit(t, owner.Client(), "reject-once")
	wait(func(status otel.ExportStatus) bool { return status.Failures > 0 })
	managedEmit(t, owner.Client(), "healthy-next")
	status := wait(func(status otel.ExportStatus) bool {
		mu.Lock()
		defer mu.Unlock()
		return seen["healthy-next"] == 1 && status.Succeeded > 0 && !status.Running
	})
	if !errors.Is(status.LastError, otel.ErrPartial) {
		t.Fatal("partial failure was erased by success", status.LastError)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		t.Fatal("historical loop failure blocked cleanup", err)
	}
	status, err = loop.Status()
	if err != nil || !status.Stopped || !errors.Is(status.LastError, otel.ErrPartial) {
		t.Fatal("shutdown erased failure history", status, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen["reject-once"] != 1 || seen["healthy-next"] != 1 {
		t.Fatal("failed event was retried or later event lost", seen)
	}
}

func TestManagedExportConcurrentStartsCannotOutliveOwnerClose(t *testing.T) {
	records := make(chan managedRecord, 1)
	server := managedPeer(t, "close-race", records)
	config := otel.Settings{Name: "close-race", ServiceName: "fixture", LogsEndpoint: server.URL + "/v1/logs"}
	policy, err := otel.Recommend(config)
	if err != nil {
		t.Fatal(err)
	}
	_, dependencies, _ := managedRuntime(t, policy)
	owner, err := otel.Open(context.Background(), config, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	type attempted struct {
		loop *otel.ExportLoop
		err  error
	}
	outcomes := make(chan attempted, 8)
	for range 8 {
		go func() {
			<-start
			loop, err := owner.StartExport(context.Background(), otel.ExportOptions{Interval: time.Second, Timeout: time.Second})
			outcomes <- attempted{loop, err}
		}()
	}
	close(start)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	accepted := 0
	for range 8 {
		outcome := <-outcomes
		if outcome.err != nil {
			if !errors.Is(outcome.err, otel.ErrState) {
				t.Fatal(outcome.err)
			}
			continue
		}
		accepted++
		status, err := outcome.loop.Status()
		if err != nil || !status.Stopped || status.Running {
			t.Fatal("accepted schedule survived source close", status, err)
		}
	}
	if accepted > 1 {
		t.Fatal("concurrent starts multiplied source scheduling", accepted)
	}
}
