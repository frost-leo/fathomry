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

package otel

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

type fixture struct {
	client          *Client
	owner           *owner
	inbox           *invocation.Inbox[Result]
	assembly        *resource.Assembly
	selected        resource.Selection[Source]
	server          *httptest.Server
	options         OptionsV1
	allowCloseError bool
}

type cleanupWitness struct {
	sdklog.Exporter
	calls *int
	cause error
}

func (witness cleanupWitness) Shutdown(ctx context.Context) error {
	*witness.calls++
	return errors.Join(witness.Exporter.Shutdown(ctx), witness.cause)
}

func TestFailureAfterNativeAcquisitionRetainsPrimaryAndCleanup(t *testing.T) {
	options := validOptions()
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaulted(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: "partial"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	primary, cleanup := &nativeCanary{}, errors.New("private-cleanup-canary")
	calls := 0
	var native sdklog.Exporter
	selection := resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		owned, err := construct(ctx, value)
		if err != nil {
			return owned, err
		}
		native = owned.Capability.owner.logExporter
		owned.Capability.owner.logExporter = cleanupWitness{native, &calls, cleanup}
		return owned, failure(ErrInput, "injected-after-acquisition", primary)
	})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "partial", selection)
	if !errors.Is(err, primary) || !errors.Is(err, cleanup) || calls != 1 || assembly.Snapshot().Sources[0].Pending {
		t.Fatal("partial acquisition lost primary/cleanup ownership")
	}
	if err := native.Export(context.Background(), nil); !errors.Is(err, sdklog.ErrExporterShutdown) {
		t.Fatal("native exporter was not actually shut down")
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, cleanup) || calls != 1 {
		t.Fatal("cleanup retried or history erased")
	}
}

func TestCanceledNativeConstructionCanCleanUp(t *testing.T) {
	options := validOptions()
	options.TracesEndpoint = options.LogsEndpoint
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	owned, err := construct(ctx, defaulted(options))
	if err == nil {
		t.Fatal("canceled construction accepted")
	}
	result := owned.Release(context.Background())
	if !result.Quiescent || !result.Released {
		t.Fatal("canceled partial constructor leaked")
	}
}

func newFixture(t *testing.T, options OptionsV1, handler http.Handler) *fixture {
	t.Helper()
	if handler == nil {
		handler = http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/x-protobuf")
		})
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	if options.Name == "" {
		options.Name = "telemetry"
	}
	if options.ServiceName == "" {
		options.ServiceName = "test-service"
	}
	if options.LogsEndpoint == "" && options.TracesEndpoint == "" && options.MetricsEndpoint == "" {
		options.LogsEndpoint = "logs"
	}
	for _, item := range []struct {
		value *string
		name  string
	}{{&options.LogsEndpoint, "logs"}, {&options.TracesEndpoint, "traces"}, {&options.MetricsEndpoint, "metrics"}} {
		if *item.value == item.name {
			*item.value = server.URL + "/v1/" + item.name
		}
	}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "test", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](512, 512*defaulted(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := &fixture{client: client, owner: client.owner, inbox: inbox, assembly: assembly, selected: selected, server: server, options: options}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := assembly.Close(ctx)
		if err != nil && !result.allowCloseError {
			t.Errorf("cleanup: %v", err)
		}
		if assembly.Snapshot().Sources[0].Pending {
			t.Error("test leaked owned resource")
		}
	})
	return result
}
func outcomeOf(t *testing.T, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	result, found := receipt.Result()
	if !found || !result.Final {
		t.Fatal("missing completed evidence")
	}
	return result
}
func emitOne(t *testing.T, fixture *fixture, call, message string) invocation.Result[Result] {
	t.Helper()
	receipt, err := fixture.client.Emit(context.Background(), fault.Correlation{Call: call}, LogRecord{Message: message})
	return outcomeOf(t, receipt, err)
}
func flushOne(t *testing.T, fixture *fixture) invocation.Result[Result] {
	t.Helper()
	receipt, err := fixture.client.Flush(context.Background(), fault.Correlation{Call: "flush"})
	return outcomeOf(t, receipt, err)
}

func TestAdmissionEvidenceAndBorrowing(t *testing.T) {
	fixture := newFixture(t, OptionsV1{}, nil)
	tiny, _ := invocation.NewInbox[Result](1, fixture.owner.settings.evidenceReservation())
	client, err := Bind(fixture.assembly, fixture.selected, tiny, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Emit(context.Background(), fault.Correlation{Call: "first"}, LogRecord{Message: "one"})
	result := outcomeOf(t, first, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	if _, err := client.Emit(context.Background(), fault.Correlation{Call: "second"}, LogRecord{Message: "two"}); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("evidence saturation did not reject")
	}
	if len(fixture.owner.pendingLogs) != 1 {
		t.Fatal("rejected call reached SDK")
	}
	delivery, err := tiny.Next(context.Background())
	if err != nil || delivery.Receipt() == nil {
		t.Fatal("independent evidence absent")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	alias := resource.Borrow("borrowed", fixture.assembly, fixture.selected)
	borrower, err := resource.Assemble(context.Background(), context.Background(), "borrower", alias)
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := Bind(borrower, alias, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := borrowed.Emit(context.Background(), fault.Correlation{Call: "borrowed"}, LogRecord{Message: "two"})
	observed := outcomeOf(t, receipt, err)
	if observed.Source.Scope != "test" || observed.Source.Configuration.Identity.Name != "telemetry" {
		t.Fatal("alias invented source")
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("owner released borrowed dependency")
	}
	if err := borrower.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseWaitsForLiveSpanAndEndCanResume(t *testing.T) {
	fixture := newFixture(t, OptionsV1{TracesEndpoint: "traces"}, nil)
	ctx, cancel := context.WithCancelCause(context.Background())
	_, span, err := fixture.client.Start(ctx, fault.Correlation{Call: "span"}, SpanInput{Name: "work"})
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("cancel-canary")
	cancel(cause)
	if _, err := span.End(ctx); !errors.Is(err, cause) {
		t.Fatal("cancellation cause lost")
	}
	if _, found := span.Receipt().Result(); found {
		t.Fatal("cancellation ended span")
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("live span did not retain resource")
	}
	receipt, err := span.End(context.Background())
	result := outcomeOf(t, receipt, err)
	if result.Err() != nil || !result.Released {
		t.Fatal("span did not end")
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := span.End(context.Background()); err != nil {
		t.Fatal("repeated End should remain complete")
	}
}

func TestConcurrentOperationsAndFlush(t *testing.T) {
	fixture := newFixture(t, OptionsV1{ActiveCalls: 32, QueuedCalls: 32, QueueItems: 4096}, nil)
	var group sync.WaitGroup
	failures := make(chan error, 64)
	for range 8 {
		group.Go(func() {
			for range 20 {
				receipt, err := fixture.client.Emit(context.Background(), fault.Correlation{Call: "parallel"}, LogRecord{Message: "safe"})
				if err == nil {
					result, _ := receipt.Result()
					err = result.Err()
				}
				if err != nil {
					failures <- err
					return
				}
			}
		})
	}
	group.Go(func() {
		for range 10 {
			receipt, err := fixture.client.Flush(context.Background(), fault.Correlation{Call: "flush"})
			if err == nil {
				result, _ := receipt.Result()
				err = result.Err()
			}
			if err != nil {
				failures <- err
				return
			}
		}
	})
	group.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if result := flushOne(t, fixture); result.Err() != nil {
		t.Fatal(result.Err())
	}
}

func TestPartialConstructionCleansEarlierOwnedResource(t *testing.T) {
	fixture := newFixture(t, OptionsV1{}, nil)
	bad := fixture.options
	bad.Name = "bad"
	bad.LogsEndpoint = "https://127.0.0.1:1/v1/logs"
	bad.TLS = &TLSV1{CA: "invalid-pem"}
	selected, err := Select(bad)
	if err != nil {
		t.Fatal(err)
	}
	assembly, err := resource.Assemble(context.Background(), context.Background(), "failed", resource.WithLimits(selected, LimitsV1(bad)))
	if err == nil || assembly == nil || !errors.Is(err, ErrInput) {
		t.Fatal("invalid CA construction was accepted")
	}
	if assembly.Snapshot().Sources[0].Pending {
		t.Fatal("partial initialization leaked")
	}
	if _, err := Bind(assembly, selected, fixture.inbox, nil); err == nil {
		t.Fatal("failed assembly bound")
	}
}
