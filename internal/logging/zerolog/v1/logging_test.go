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

package zerolog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type fixture struct {
	logger          *Logger
	assembly        *resource.Assembly
	selected        resource.Selection[Source]
	inbox           *invocation.Inbox[Result]
	allowCloseError bool
}

func bindFixture(t testing.TB, options OptionsV1, capacity int, layers ...resource.Layer) *fixture {
	t.Helper()
	selected, err := Select(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "fixture", selected)
	if err != nil {
		if assembly != nil {
			_ = assembly.Close(context.Background())
		}
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*EvidenceBytesV1())
	if err != nil {
		t.Fatal(err)
	}
	logger, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		_ = assembly.Close(context.Background())
		t.Fatal(err)
	}
	value := &fixture{logger: logger, assembly: assembly, selected: selected, inbox: inbox}
	t.Cleanup(func() {
		err := assembly.Close(context.Background())
		if err != nil && !value.allowCloseError {
			t.Error("unexpected cleanup error", err)
		}
		for _, status := range assembly.Snapshot().Sources {
			if status.Pending {
				t.Error("fixture leaked live resource responsibility")
			}
		}
	})
	return value
}
func correlation(call string) fault.Correlation {
	return fault.Correlation{Call: call, Owner: "execution-1"}
}
func observed(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil || receipt == nil {
		t.Fatal("operation setup failed", err)
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || !result.Released {
		t.Fatal("synchronous result not finalized/released")
	}
	return result
}
func emit(t testing.TB, f *fixture, id string, level Level, message string, attrs ...slog.Attr) invocation.Result[Result] {
	t.Helper()
	receipt, err := f.logger.Log(context.Background(), correlation(id), level, message, attrs...)
	return observed(t, receipt, err)
}
func drain(t testing.TB, inbox *invocation.Inbox[Result]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for inbox.Usage().Outstanding > 0 {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = delivery.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err = delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
}
func decodeRecords(t testing.TB, data []byte) []map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var result []map[string]any
	for {
		var item map[string]any
		if err := decoder.Decode(&item); err == io.EOF {
			return result
		} else if err != nil {
			t.Fatal("invalid JSON log", err)
		}
		result = append(result, item)
	}
}

type recordCollector struct {
	records  []Record
	contexts []any
}
type contextKey struct{}

func (collector *recordCollector) WriteRecord(ctx context.Context, record Record) error {
	collector.records = append(collector.records, record)
	collector.contexts = append(collector.contexts, ctx.Value(contextKey{}))
	return nil
}
func TestStructuredMultiSinkContextAndSnapshots(t *testing.T) {
	var first, second bytes.Buffer
	collector := &recordCollector{}
	options := OptionsV1{Name: "events", MinLevel: Trace, Sinks: []SinkV1{
		{Name: "first", Writer: &first}, {Name: "second", Writer: &second}, {Name: "structured", Records: collector}}}
	f := bindFixture(t, options, 1)
	attrs := []slog.Attr{slog.String("kind", "selected"), slog.Group("nested", slog.Uint64("large", ^uint64(0)), slog.Bool("ok", true)),
		slog.Float64("decimal", 1.25), slog.Duration("elapsed_ns", 123*time.Nanosecond), slog.Time("at", time.Date(2026, 9, 13, 1, 2, 3, 4, time.FixedZone("private-zone", 3600))),
		slog.Any("null", nil), slog.String("level", "cannot-overwrite")}
	ctx := context.WithValue(context.Background(), contextKey{}, "trace-association")
	receipt, err := f.logger.Log(ctx, correlation("structured"), Info, "message-canary", attrs...)
	result := observed(t, receipt, err)
	if result.Err() != nil || result.Attempts != (invocation.Attempts{}) {
		t.Fatal("sink delivery failed", result.Err())
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) || len(collector.records) != 1 || collector.contexts[0] != "trace-association" {
		t.Fatal("fan-out or per-call context association changed")
	}
	record := collector.records[0]
	if record.Level() != Info || record.Message() != "message-canary" || record.Time().IsZero() || record.Time().Location() != time.UTC {
		t.Fatal("structured event properties changed")
	}
	if !bytes.Equal(record.JSONCopy(), first.Bytes()) || record.Correlation() != correlation("structured") || record.Source().Configuration.Identity.Name != "events" {
		t.Fatal("structured snapshot differs from native byte output")
	}
	wire := decodeRecords(t, first.Bytes())[0]
	if wire["time"] != record.Time().Format(time.RFC3339Nano) {
		t.Fatal("typed and encoded timestamps differ")
	}
	values := wire["attributes"].(map[string]any)
	if wire["level"] != "info" || values["level"] != "cannot-overwrite" || values["nested"].(map[string]any)["large"] != json.Number("18446744073709551615") ||
		values["decimal"] != json.Number("1.25") || values["elapsed_ns"] != json.Number("123") || values["at"] != "2026-09-13T00:02:03.000000004Z" || values["null"] != nil {
		t.Fatal("typed metadata changed in encoding")
	}
	attrs[0] = slog.String("kind", "changed")
	attrs[1].Value.Group()[0] = slog.Int("large", 0)
	copyAttrs := record.AttributesCopy()
	copyAttrs[1].Value.Group()[1] = slog.Bool("ok", false)
	jsonCopy := record.JSONCopy()
	jsonCopy[0] = 'X'
	sourceCopy := record.Source()
	sourceCopy.Configuration.Provenance[0].Fields[0] = "changed"
	if record.AttributesCopy()[0].Value.String() != "selected" || record.AttributesCopy()[1].Value.Group()[0].Value.Uint64() != ^uint64(0) ||
		!record.AttributesCopy()[1].Value.Group()[1].Value.Bool() || record.JSONCopy()[0] != '{' || record.Source().Configuration.Provenance[0].Fields[0] == "changed" {
		t.Fatal("mutable alias reached retained record")
	}
	for _, value := range []any{record, &record, result.Outcome.Value, &result.Outcome.Value, result.Err(), f.logger, options} {
		conformance.Private(t, value, "message-canary", "private-zone")
	}
	conformance.Runtime(t, record, new(Record), "message-canary")
	drain(t, f.inbox)
}

type failingWriter struct {
	calls int
	count int
	err   error
}

func (writer *failingWriter) Write(data []byte) (int, error) {
	writer.calls++
	if writer.count == -2 {
		return len(data) - 1, writer.err
	}
	return writer.count, writer.err
}
func TestEverySinkFailureRetainedAndHealthySinkContinues(t *testing.T) {
	original := errors.New("native-error-canary")
	first := &failingWriter{err: original}
	second := &failingWriter{count: -2}
	var healthy bytes.Buffer
	f := bindFixture(t, OptionsV1{Name: "failures", Sinks: []SinkV1{{Name: "first", Writer: first}, {Name: "second", Writer: second}, {Name: "healthy", Writer: &healthy}}}, 1)
	result := emit(t, f, "failed", Error, "payload-canary")
	if !errors.Is(result.Err(), original) || !errors.Is(result.Err(), io.ErrShortWrite) || first.calls != 1 || second.calls != 1 || len(decodeRecords(t, healthy.Bytes())) != 1 {
		t.Fatal("secondary failure or later sink was lost")
	}
	sinks := result.Outcome.Value.SinksCopy()
	if !sinks[0].Attempted || sinks[0].Accepted || !sinks[1].BytesKnown || sinks[1].Accepted || !sinks[2].Accepted {
		t.Fatal("per-sink evidence conflated")
	}
	sinks[0].WriteError = nil
	sinks[2].Accepted = false
	if result.Outcome.Value.SinksCopy()[0].WriteError == nil || !result.Outcome.Value.SinksCopy()[2].Accepted {
		t.Fatal("result aliases consumer mutation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conformance.Receive(t, ctx, f.inbox, []conformance.Expected[Result]{{
		Context: fault.Context{Provider: ProviderID, Source: "failures", Scope: "fixture", Operation: "log", Correlation: correlation("failed")},
		Source:  f.logger.access.Info(), Limits: f.logger.access.Limits(), Shape: invocation.Finite, Present: true, Final: true, Released: true,
		Primary: original, Attempts: invocation.Attempts{},
		Value: func(t testing.TB, value Result) {
			got := value.SinksCopy()
			if len(got) != 3 || !errors.Is(got[1].WriteError, io.ErrShortWrite) || !got[2].Accepted {
				t.Error("independent sink evidence incomplete")
			}
		},
	}})
	conformance.Private(t, result.Err(), "native-error-canary", "payload-canary")
	result = emit(t, f, "later", Info, "later")
	if first.calls != 1 || second.calls != 1 || result.Attempts != (invocation.Attempts{}) || len(decodeRecords(t, healthy.Bytes())) != 2 {
		t.Fatal("failed byte streams retried or healthy stream stopped")
	}
	drain(t, f.inbox)
}
func TestFiltersAndNamedInstancesAreIndependent(t *testing.T) {
	var low, high, other bytes.Buffer
	first := bindFixture(t, OptionsV1{Name: "first", MinLevel: Trace, Sinks: []SinkV1{{Name: "low", Writer: &low}, {Name: "high", MinLevel: Error, Writer: &high}}}, 1)
	second := bindFixture(t, OptionsV1{Name: "second", MinLevel: Warn, Sinks: []SinkV1{{Name: "other", Writer: &other}}}, 1)
	got := emit(t, first, "trace", Trace, "trace")
	if got.Err() != nil || !got.Outcome.Value.SinksCopy()[1].Filtered || !got.Outcome.Value.SinksCopy()[0].Accepted || got.Attempts != (invocation.Attempts{}) {
		t.Fatal("local trace selection failed")
	}
	drain(t, first.inbox)
	got = emit(t, second, "filtered", Info, "not-delivered")
	if got.Err() != nil || !got.Outcome.Value.SinksCopy()[0].Filtered || got.Attempts.Observed != 0 || other.Len() != 0 {
		t.Fatal("filtering was not explicit")
	}
	drain(t, second.inbox)
	for _, level := range []Level{Fatal, Panic} {
		got = emit(t, first, string(level), level, "severity-only")
		if got.Err() != nil {
			t.Fatal("terminal severity changed control flow")
		}
		drain(t, first.inbox)
	}
	if len(decodeRecords(t, low.Bytes())) != 3 || len(decodeRecords(t, high.Bytes())) != 2 {
		t.Fatal("per-sink filtering changed")
	}
}

type blockedWriter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (writer *blockedWriter) Write(data []byte) (int, error) {
	writer.once.Do(func() { close(writer.entered) })
	<-writer.release
	return len(data), nil
}
func TestCancellationDoesNotReleaseBlockedWriterOrLoseLateEvidence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
		var later bytes.Buffer
		f := bindFixture(t, OptionsV1{Name: "blocked", Sinks: []SinkV1{{Name: "blocked", Writer: writer}, {Name: "later", Writer: &later}}}, 1)
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("caller-stopped")
		done := make(chan struct{})
		var receipt *invocation.Receipt[Result]
		var setup error
		go func() { defer close(done); receipt, setup = f.logger.Log(ctx, correlation("blocked"), Info, "message") }()
		<-writer.entered
		cancel(cause)
		synctest.Wait()
		if err := f.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
			t.Fatal("blocked call released early")
		}
		if usage := f.assembly.Snapshot().Sources[0].Usage; usage.Active != 1 || f.inbox.Usage().Outstanding != 1 {
			t.Fatal("blocked work lost accounting")
		}
		delivery, err := f.inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); !errors.Is(err, invocation.ErrPending) {
			t.Fatal("unresolved evidence released")
		}
		close(writer.release)
		<-done
		result := observed(t, receipt, setup)
		sinks := result.Outcome.Value.SinksCopy()
		if !sinks[0].Accepted || sinks[1].Attempted || later.Len() != 0 || !errors.Is(result.Err(), cause) || result.Attempts != (invocation.Attempts{}) {
			t.Fatal("late accepted write or canceled untouched sink misreported")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLastWriterAcknowledgementSurvivesCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
		f := bindFixture(t, OptionsV1{Name: "last", Sinks: []SinkV1{{Name: "out", Writer: writer}}}, 1)
		ctx, cancel := context.WithCancelCause(context.Background())
		var receipt *invocation.Receipt[Result]
		var setup error
		done := make(chan struct{})
		go func() { defer close(done); receipt, setup = f.logger.Log(ctx, correlation("late"), Info, "message") }()
		<-writer.entered
		cancel(errors.New("caller-left"))
		synctest.Wait()
		if f.assembly.Snapshot().Sources[0].Usage.Active != 1 {
			t.Fatal("cancellation released a blocked writer")
		}
		close(writer.release)
		<-done
		result := observed(t, receipt, setup)
		sink := result.Outcome.Value.SinksCopy()[0]
		if result.Err() != nil || !sink.Accepted || !sink.Attempted || !sink.BytesKnown || result.Attempts != (invocation.Attempts{}) {
			t.Fatal("late positive acknowledgement became failure", result.Err())
		}
		drain(t, f.inbox)
	})
}

func TestRequiredEvidenceSaturationPrecedesOutputAndObserverIsOptional(t *testing.T) {
	var output bytes.Buffer
	f := bindFixture(t, OptionsV1{Name: "inbox", Sinks: []SinkV1{{Name: "out", Writer: &output}}}, 1)
	observer, _ := invocation.NewObserver(1)
	f.logger.observer = observer
	first := emit(t, f, "first", Info, "one")
	if first.Observation != invocation.ObservationQueued {
		t.Fatal("observation not queued")
	}
	size := output.Len()
	if receipt, err := f.logger.Log(context.Background(), correlation("rejected"), Info, "two"); receipt != nil || !errors.Is(err, invocation.ErrEvidence) || output.Len() != size {
		t.Fatal("evidence saturation did not reject before output")
	}
	drain(t, f.inbox)
	second := emit(t, f, "second", Info, "two")
	if second.Err() != nil || second.Observation != invocation.ObservationDropped || len(decodeRecords(t, output.Bytes())) != 2 {
		t.Fatal("observer loss affected required facts")
	}
	drain(t, f.inbox)
}
func TestBorrowedScopePreservesIdentityAndSharedAllowance(t *testing.T) {
	collector := &recordCollector{}
	f := bindFixture(t, OptionsV1{Name: "original", Sinks: []SinkV1{{Name: "record", Records: collector}}}, 2)
	alias := resource.Borrow("alias", f.assembly, f.selected)
	assembly, err := resource.Assemble(context.Background(), context.Background(), "borrower", alias)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	logger, err := Bind(assembly, alias, f.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("borrowed source closed")
	}
	receipt, err := logger.Log(context.Background(), correlation("borrow"), Info, "borrow")
	result := observed(t, receipt, err)
	if result.Err() != nil || result.Context.Source != "original" || result.Context.Scope != "fixture" ||
		collector.records[0].Source().Configuration.Identity.Name != "original" || !reflect.DeepEqual(result.Limits, f.logger.access.Limits()) {
		t.Fatal("borrowing relabeled or reset resource")
	}
	drain(t, f.inbox)
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if receipt, err := logger.Log(context.Background(), correlation("closed"), Info, "closed"); receipt != nil || err == nil {
		t.Fatal("closed borrowing facade bypassed admission")
	}
}
func TestQueuedAdmissionIsBoundedAndCanceledWithoutOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
		f := bindFixture(t, OptionsV1{Name: "queue", QueuedCalls: 1, Sinks: []SinkV1{{Name: "out", Writer: writer}}}, 3)
		firstDone := make(chan struct{})
		go func() {
			defer close(firstDone)
			receipt, err := f.logger.Log(context.Background(), correlation("first"), Info, "one")
			if err != nil || receipt == nil {
				t.Error("first call rejected", err)
			}
		}()
		<-writer.entered
		ctx, cancel := context.WithCancel(context.Background())
		queuedDone := make(chan struct{})
		go func() {
			defer close(queuedDone)
			receipt, err := f.logger.Log(ctx, correlation("queued"), Info, "two")
			if receipt != nil || !errors.Is(err, context.Canceled) {
				t.Error("queued cancellation changed")
			}
		}()
		synctest.Wait()
		if usage := f.assembly.Snapshot().Sources[0].Usage; usage.Active != 1 || usage.Queued != 1 {
			t.Fatal("queue not charged")
		}
		if receipt, err := f.logger.Log(context.Background(), correlation("overflow"), Info, "three"); receipt != nil || !errors.Is(err, resource.ErrCapacity) {
			t.Fatal("queue overflow not rejected")
		}
		cancel()
		<-queuedDone
		if usage := f.assembly.Snapshot().Sources[0].Usage; usage.Queued != 0 || f.inbox.Usage().Outstanding != 1 {
			t.Fatal("canceled reservation leaked")
		}
		close(writer.release)
		<-firstDone
		drain(t, f.inbox)
	})
}

type mutatingWriter struct{}

func (mutatingWriter) Write(data []byte) (int, error) {
	for index := range data {
		data[index] = 'X'
	}
	return len(data), nil
}
func TestOneByteSinkCannotMutateOtherSinksOrRetainedRecord(t *testing.T) {
	var healthy bytes.Buffer
	collector := &recordCollector{}
	f := bindFixture(t, OptionsV1{Name: "copies", Sinks: []SinkV1{
		{Name: "mutator", Writer: mutatingWriter{}}, {Name: "healthy", Writer: &healthy}, {Name: "typed", Records: collector}}}, 1)
	if result := emit(t, f, "copies", Info, "copy-canary"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if len(decodeRecords(t, healthy.Bytes())) != 1 || len(collector.records) != 1 ||
		!bytes.Equal(healthy.Bytes(), collector.records[0].JSONCopy()) {
		t.Fatal("borrowed byte buffer aliased another sink")
	}
	drain(t, f.inbox)
}

type retryingRecordWriter struct{ submissions int }

func (writer *retryingRecordWriter) WriteRecord(ctx context.Context, _ Record) error {
	for range 2 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		writer.submissions++
	}
	return nil
}

func TestOpaqueSinksDoNotInventSDKAttempts(t *testing.T) {
	var bytesSink bytes.Buffer
	queue := &recordCollector{}
	retrying := &retryingRecordWriter{}
	f := bindFixture(t, OptionsV1{Name: "attempts", Sinks: []SinkV1{
		{Name: "queued", Records: queue}, {Name: "retrying", Records: retrying}, {Name: "bytes", Writer: &bytesSink},
	}}, 1)
	observer, err := invocation.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	f.logger.observer = observer
	result := emit(t, f, "opaque", Info, "message")
	if result.Err() != nil || retrying.submissions != 2 || len(queue.records) != 1 {
		t.Fatal("independent output controls failed", result.Err())
	}
	if result.Attempts != (invocation.Attempts{}) {
		t.Fatal("opaque handoffs invented exact SDK attempts or a lower bound", result.Attempts)
	}
	for _, sink := range result.Outcome.Value.SinksCopy() {
		if !sink.Attempted || !sink.Accepted {
			t.Fatal("known sink acceptance was erased")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	delivery, err := f.inbox.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	independent, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil || independent.Attempts != (invocation.Attempts{}) {
		t.Fatal("independent evidence invented attempts")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	if err := observer.ExportOne(ctx, func(_ context.Context, event invocation.Event) error {
		if event.Attempts != (invocation.Attempts{}) {
			t.Error("observation invented SDK attempts")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentRecordsStayWhole(t *testing.T) {
	var output bytes.Buffer
	f := bindFixture(t, OptionsV1{Name: "concurrent", QueuedCalls: 64, Sinks: []SinkV1{{Name: "out", Writer: &output}}}, 32)
	var workers sync.WaitGroup
	for index := range 32 {
		workers.Go(func() {
			receipt, err := f.logger.Log(context.Background(), correlation("call-"+strings.Repeat("a", index+1)), Info, "concurrent", slog.Int("index", index))
			if err != nil {
				t.Error("concurrent admission", err)
				return
			}
			if result, _ := receipt.Result(); result.Err() != nil {
				t.Error("concurrent output", result.Err())
			}
		})
	}
	workers.Wait()
	records := decodeRecords(t, output.Bytes())
	seen := make(map[any]bool)
	for _, record := range records {
		seen[record["attributes"].(map[string]any)["index"]] = true
	}
	if len(records) != 32 || len(seen) != 32 {
		t.Fatal("concurrent events interleaved or disappeared")
	}
	drain(t, f.inbox)
}
