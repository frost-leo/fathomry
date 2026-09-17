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

package temporal_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	nativelog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type metricReading struct {
	name, kind string
	tags       map[string]string
	value      any
}

type nativeMetricRecorder struct {
	mu       sync.Mutex
	readings []metricReading
	overflow bool
}

type nativeTestMetrics struct {
	state *nativeMetricRecorder
	tags  map[string]string
}

func newNativeTestMetrics() *nativeTestMetrics {
	return &nativeTestMetrics{state: &nativeMetricRecorder{}}
}

func (handler *nativeTestMetrics) WithTags(tags map[string]string) sdk.MetricsHandler {
	merged := make(map[string]string, len(handler.tags)+len(tags))
	maps.Copy(merged, handler.tags)
	maps.Copy(merged, tags)
	return &nativeTestMetrics{state: handler.state, tags: merged}
}

type testCounter func(int64)

func (counter testCounter) Inc(value int64) { counter(value) }

type testGauge func(float64)

func (gauge testGauge) Update(value float64) { gauge(value) }

type testTimer func(time.Duration)

func (timer testTimer) Record(value time.Duration) { timer(value) }

func (handler *nativeTestMetrics) Counter(name string) sdk.MetricsCounter {
	return testCounter(func(value int64) { handler.record(name, "counter", value) })
}

func (handler *nativeTestMetrics) Gauge(name string) sdk.MetricsGauge {
	return testGauge(func(value float64) { handler.record(name, "gauge", value) })
}

func (handler *nativeTestMetrics) Timer(name string) sdk.MetricsTimer {
	return testTimer(func(value time.Duration) { handler.record(name, "timer", value) })
}

func (handler *nativeTestMetrics) record(name, kind string, value any) {
	if !strings.HasPrefix(name, "gh61_") && name != "temporal_request" {
		return
	}
	handler.state.mu.Lock()
	defer handler.state.mu.Unlock()
	if len(handler.state.readings) >= 512 {
		handler.state.overflow = true
		return
	}
	handler.state.readings = append(handler.state.readings, metricReading{name, kind, maps.Clone(handler.tags), value})
}

func (handler *nativeTestMetrics) readings() []metricReading {
	handler.state.mu.Lock()
	defer handler.state.mu.Unlock()
	result := make([]metricReading, len(handler.state.readings))
	for index, entry := range handler.state.readings {
		result[index] = metricReading{entry.name, entry.kind, maps.Clone(entry.tags), entry.value}
	}
	return result
}

func (handler *nativeTestMetrics) sum(name string) int64 {
	var result int64
	for _, entry := range handler.readings() {
		if entry.name == name && entry.kind == "counter" {
			result += entry.value.(int64)
		}
	}
	return result
}

type nativeContextKey struct{}

type nativeTestPropagator struct{ injectError error }

const nativeHeaderKey = "gh61-context"

func (propagator *nativeTestPropagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	if propagator.injectError != nil {
		return propagator.injectError
	}
	if value, ok := ctx.Value(nativeContextKey{}).(string); ok {
		writer.Set(nativeHeaderKey, &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: []byte(value)})
	}
	return nil
}

func (*nativeTestPropagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	if payload, ok := reader.Get(nativeHeaderKey); ok {
		return context.WithValue(ctx, nativeContextKey{}, string(payload.Data)), nil
	}
	return ctx, nil
}

func (*nativeTestPropagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	if value, ok := ctx.Value(nativeContextKey{}).(string); ok {
		writer.Set(nativeHeaderKey, &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: []byte(value)})
	}
	return nil
}

func (*nativeTestPropagator) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	if payload, ok := reader.Get(nativeHeaderKey); ok {
		return workflow.WithValue(ctx, nativeContextKey{}, string(payload.Data)), nil
	}
	return ctx, nil
}

type nativeSpanKey struct{}

type spanObservation struct {
	id, parent, operation, name, key string
	finished                         bool
	err                              error
}

type nativeTestTracer struct {
	interceptor.BaseTracer
	mu         sync.Mutex
	spans      []*nativeTestSpan
	startError error
}

type nativeTestSpan struct {
	tracer      *nativeTestTracer
	observation spanObservation
}

func (*nativeTestTracer) Options() interceptor.TracerOptions {
	return interceptor.TracerOptions{SpanContextKey: nativeSpanKey{}, HeaderKey: "gh61-trace"}
}

func (*nativeTestTracer) UnmarshalSpan(fields map[string]string) (interceptor.TracerSpanRef, error) {
	if fields["id"] == "" {
		return nil, errors.New("missing synthetic span identity")
	}
	return fields["id"], nil
}

func (*nativeTestTracer) MarshalSpan(span interceptor.TracerSpan) (map[string]string, error) {
	value, ok := span.(*nativeTestSpan)
	if !ok {
		return nil, errors.New("unknown synthetic span")
	}
	return map[string]string{"id": value.observation.id}, nil
}

func (*nativeTestTracer) SpanFromContext(ctx context.Context) interceptor.TracerSpan {
	value, _ := ctx.Value(nativeSpanKey{}).(interceptor.TracerSpan)
	return value
}

func (*nativeTestTracer) ContextWithSpan(ctx context.Context, span interceptor.TracerSpan) context.Context {
	return context.WithValue(ctx, nativeSpanKey{}, span)
}

func (tracer *nativeTestTracer) StartSpan(options *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	if tracer.startError != nil {
		return nil, tracer.startError
	}
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	if len(tracer.spans) >= 256 {
		return nil, errors.New("synthetic tracing capacity reached")
	}
	var parent string
	switch value := options.Parent.(type) {
	case *nativeTestSpan:
		parent = value.observation.id
	case string:
		parent = value
	}
	span := &nativeTestSpan{tracer: tracer, observation: spanObservation{id: fmt.Sprintf("span-%d", len(tracer.spans)+1), parent: parent,
		operation: options.Operation, name: options.Name, key: options.IdempotencyKey}}
	tracer.spans = append(tracer.spans, span)
	return span, nil
}

func (span *nativeTestSpan) Finish(options *interceptor.TracerFinishSpanOptions) {
	span.tracer.mu.Lock()
	defer span.tracer.mu.Unlock()
	span.observation.finished = true
	span.observation.err = options.Error
}

func (tracer *nativeTestTracer) observations() []spanObservation {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	result := make([]spanObservation, len(tracer.spans))
	for index, span := range tracer.spans {
		result[index] = span.observation
	}
	return result
}

func recordExecutionMetrics(handler sdk.MetricsHandler, name string, contextValue any) {
	value, _ := contextValue.(string)
	handler = handler.WithTags(map[string]string{"propagated": value})
	handler.Counter(name).Inc(3)
	handler.Gauge(name).Update(7.5)
	handler.Timer(name).Record(125 * time.Millisecond)
}

type nativeLogEntry struct {
	level, message string
	fields         []any
}

type nativeLogRecorder struct {
	mu                          sync.Mutex
	entries                     []nativeLogEntry
	withs, skips, closes, syncs atomic.Int32
	afterClose, overflow        atomic.Bool
	blockMessage                string
	entered, release            chan struct{}
	once                        sync.Once
}

type nativeTestLogger struct {
	state  *nativeLogRecorder
	fields []any
}

func newNativeTestLogger() *nativeTestLogger {
	return &nativeTestLogger{state: &nativeLogRecorder{}}
}

func (logger *nativeTestLogger) Debug(message string, fields ...any) {
	logger.record("debug", message, fields)
}

func (logger *nativeTestLogger) Info(message string, fields ...any) {
	logger.record("info", message, fields)
}

func (logger *nativeTestLogger) Warn(message string, fields ...any) {
	logger.record("warn", message, fields)
}

func (logger *nativeTestLogger) Error(message string, fields ...any) {
	logger.record("error", message, fields)
}

func (logger *nativeTestLogger) With(fields ...any) nativelog.Logger {
	logger.state.withs.Add(1)
	return &nativeTestLogger{state: logger.state, fields: append(append([]any(nil), logger.fields...), fields...)}
}

func (logger *nativeTestLogger) WithCallerSkip(count int) nativelog.Logger {
	logger.state.skips.Add(int32(count))
	return &nativeTestLogger{state: logger.state, fields: append([]any(nil), logger.fields...)}
}

func (logger *nativeTestLogger) Close() { logger.state.closes.Add(1) }

func (logger *nativeTestLogger) Sync() error { logger.state.syncs.Add(1); return nil }

func (logger *nativeTestLogger) record(level, message string, fields []any) {
	if logger.state.closes.Load() != 0 {
		logger.state.afterClose.Store(true)
	}
	if strings.HasPrefix(message, "gh61.") || message == "Started Worker" || message == "Stopped Worker" {
		entry := nativeLogEntry{level: level, message: message, fields: append(append([]any(nil), logger.fields...), fields...)}
		logger.state.mu.Lock()
		if len(logger.state.entries) < 128 {
			logger.state.entries = append(logger.state.entries, entry)
		} else {
			logger.state.overflow.Store(true)
		}
		logger.state.mu.Unlock()
	}
	if message == logger.state.blockMessage && logger.state.entered != nil {
		logger.state.once.Do(func() { close(logger.state.entered) })
		<-logger.state.release
	}
}

func (logger *nativeTestLogger) entries() []nativeLogEntry {
	logger.state.mu.Lock()
	defer logger.state.mu.Unlock()
	result := make([]nativeLogEntry, len(logger.state.entries))
	for index, entry := range logger.state.entries {
		result[index] = nativeLogEntry{level: entry.level, message: entry.message, fields: append([]any(nil), entry.fields...)}
	}
	return result
}

func logField(entry nativeLogEntry, key string) any {
	for index := len(entry.fields) - 2; index >= 0; index -= 2 {
		if entry.fields[index] == key {
			return entry.fields[index+1]
		}
	}
	return nil
}

type unformattedLogValue struct{}

type loggerDependencyOptions struct {
	Enabled bool `json:"enabled"`
}

func (*unformattedLogValue) String() string {
	panic("native extension value was formatted by the provider")
}

func TestNativeLoggerIsBorrowedAndRetainedUntilCallbackExit(t *testing.T) {
	logger := newNativeTestLogger()
	logger.state.blockMessage = "gh61.blocked-log"
	logger.state.entered, logger.state.release = make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(logger.state.release) })
	prepared, err := resource.Prepare(resource.Schema[loggerDependencyOptions]{Format: 1, Defaults: loggerDependencyOptions{Enabled: true}},
		resource.Input{Identity: resource.Identity{Provider: "test.logger", Name: "logger"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	logSelection := resource.Select(prepared, func(context.Context, loggerDependencyOptions) (resource.Resource[*nativeTestLogger], error) {
		return resource.Resource[*nativeTestLogger]{Capability: logger, Acquired: true, Release: func(context.Context) resource.ReleaseResult {
			logger.Close()
			return resource.ReleaseResult{Quiescent: true, Released: true}
		}}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logOwner, err := resource.Assemble(ctx, ctx, "logger-owner", logSelection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := logOwner.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	borrowed := resource.Borrow("logger-dependency", logOwner, logSelection)
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{Logger: logger}, []resource.Spec{borrowed},
		func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) { peer.activityName = "logged" })
	executions, _ := executionBinding(t, fixture)
	workers, tasks := workerInboxes(t)
	original := errors.New("native-error-is-not-rewritten")
	unformatted := &unformattedLogValue{}
	body := func(ctx context.Context) error {
		log := nativelog.With(activity.GetLogger(ctx), "custom-type", unformatted)
		log = nativelog.Skip(log, 2)
		log.Info("gh61.blocked-log", "attempt-note", int64(7), "error", original)
		return nil
	}
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	managed, err := executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "logged-worker"}, temporal.WorkerSpec{
		TaskQueue: "unit", MaxHandlers: 1, Bytes: fixture.client.RPCReservation(),
		Options:    worker.Options{DisableWorkflowWorker: true, MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1, WorkerStopTimeout: time.Millisecond},
		Activities: []temporal.ActivityRegistration{{Definition: body, Options: activity.RegisterOptions{Name: "logged"}}},
	}, workers, tasks)
	if managed != nil {
		t.Cleanup(func() {
			unblock.Do(func() { close(logger.state.release) })
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-logger.state.entered:
	case <-ctx.Done():
		t.Fatal("native Activity logger did not receive the call")
	}
	short, stopWaiting := context.WithTimeout(ctx, 20*time.Millisecond)
	err = managed.Stop(short)
	stopWaiting()
	if !errors.Is(err, context.DeadlineExceeded) || managed.Status().Joined {
		t.Fatal("blocked native logger was mistaken for completed Worker cleanup")
	}
	if err := fixture.assembly.Close(ctx); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("Temporal scope released its live logger dependency")
	}
	if err := logOwner.Close(ctx); !errors.Is(err, resource.ErrIncomplete) || logger.state.closes.Load() != 0 {
		t.Fatal("logger owner released a borrowed live callback")
	}
	unblock.Do(func() { close(logger.state.release) })
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if logger.state.syncs.Load() != 0 || logger.state.closes.Load() != 0 {
		t.Fatal("Temporal acquired logger Sync/Close authority")
	}
	found := false
	for _, entry := range logger.entries() {
		if entry.message != "gh61.blocked-log" {
			continue
		}
		found = true
		if entry.level != "info" || logField(entry, "custom-type") != unformatted || logField(entry, "error") != original ||
			logField(entry, "attempt-note") != int64(7) || logField(entry, "Namespace") != "test" || logField(entry, "ActivityID") != "fixture-activity" {
			t.Fatal("native logger fields or error identity were rewritten")
		}
	}
	if !found || logger.state.withs.Load() == 0 || logger.state.skips.Load() < 2 || logger.state.overflow.Load() {
		t.Fatal("native optional logging contracts were lost")
	}
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if logger.state.closes.Load() != 0 {
		t.Fatal("returning a logger borrow closed its owner")
	}
	if err := logOwner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if logger.state.closes.Load() != 1 || logger.state.afterClose.Load() {
		t.Fatal("logger ownership/termination ordering failed")
	}
}

func TestRuntimeLoggerValidationAndPrivacy(t *testing.T) {
	var typedNil *nativeTestLogger
	_, err := temporal.SelectWithRuntime(temporal.OptionsV1{}, temporal.RuntimeOptions{Logger: typedNil})
	if !errors.Is(err, temporal.ErrInput) {
		t.Fatal("typed nil native logger accepted")
	}
	logger := newNativeTestLogger()
	value := temporal.RuntimeOptions{Logger: logger}
	conformance.Private(t, value, "nativeLogRecorder", "nativeTestLogger")
	fixture := newRuntimeFixture(t, 1, value, nil)
	found := false
	for _, option := range fixture.client.Profile().Options {
		if option.Name == "native-logger" {
			found = option.Value == "true"
		}
	}
	if !found {
		t.Fatal("native logger selection is absent from the runtime profile")
	}
}
