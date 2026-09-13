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

package zap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type captured struct {
	ctx    context.Context
	entry  zapcore.Entry
	fields []zapcore.Field
}
type recordingSink struct {
	mu       sync.Mutex
	records  []captured
	writeErr error
	syncErr  error
	syncs    int
	onWrite  func(context.Context)
}

func (sink *recordingSink) Write(ctx context.Context, entry zapcore.Entry, fields []zapcore.Field) error {
	if sink.onWrite != nil {
		sink.onWrite(ctx)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.records = append(sink.records, captured{ctx, entry, fields})
	return sink.writeErr
}
func (sink *recordingSink) Sync(context.Context) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.syncs++
	return sink.syncErr
}
func (sink *recordingSink) count() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return len(sink.records)
}

type fixture struct {
	logger          *Logger
	assembly        *resource.Assembly
	selection       resource.Selection[Source]
	inbox           *invocation.Inbox[Result]
	allowCloseError bool
}

func bindFixture(t testing.TB, options OptionsV1, sink StructuredSink, capacity int) *fixture {
	t.Helper()
	selected, err := Select(options, sink)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "fixture", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*defaults(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	logger, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	value := &fixture{logger: logger, assembly: assembly, selection: selected, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(context.Background()); err != nil && !value.allowCloseError {
			t.Error("fixture cleanup failed", err)
		}
		drain(t, inbox)
	})
	return value
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
		if _, err := delivery.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
}
func resultOf(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil || receipt == nil {
		t.Fatal("operation not admitted", err)
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || !result.Released {
		t.Fatal("operation did not finish and release")
	}
	return result
}
func logResult(t testing.TB, logger *Logger, id string, fields ...zapcore.Field) invocation.Result[Result] {
	t.Helper()
	receipt, err := logger.Log(context.Background(), fault.Correlation{Call: id}, zapcore.InfoLevel, "message", fields...)
	return resultOf(t, receipt, err)
}
func fileOptions(t testing.TB, compress bool) OptionsV1 {
	t.Helper()
	return OptionsV1{Name: "logs", MaxEntryBytes: 1024, Outputs: []OutputV1{{Name: "local", Kind: "file", Directory: privateDirectory(t), MaxFileBytes: 1024, MaxBackups: 3, Compress: compress}}}
}

func privateDirectory(t testing.TB) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	return directory
}
func readJSON(t testing.TB, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var values []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var value map[string]any
		if err := json.Unmarshal(line, &value); err != nil {
			t.Fatal("invalid JSON output", err)
		}
		values = append(values, value)
	}
	return values
}

func TestNativeMultiSinkTypedContextAndFiltering(t *testing.T) {
	options := fileOptions(t, false)
	options.Caller = true
	options.Outputs = append(options.Outputs, OutputV1{Name: "errors", Kind: "file", Level: "error", Directory: privateDirectory(t)})
	sink := &recordingSink{}
	fixture := bindFixture(t, options, sink, 8)
	logger, err := fixture.logger.Named("pipeline")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("before")
	nativeCause := errors.New("private-cause-canary")
	original := fault.Kind("fixture.failure").New(fault.Context{}, nativeCause)
	logger, err = logger.With(sdk.Binary("blob", data), sdk.Int64("count", 42), sdk.Error(original))
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "context-canary")
	receipt, err := logger.Log(ctx, fault.Correlation{Call: "one", Parent: "parent", Owner: "opaque"}, zapcore.InfoLevel, "message", sdk.Bool("ok", true))
	result := resultOf(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	sinks := result.Outcome.Value.SinksCopy()
	if len(sinks) != 3 || sinks[0].State != Written || sinks[1].State != Filtered || sinks[2].State != Written {
		t.Fatal("multi-sink coverage changed")
	}
	values := readJSON(t, filepath.Join(options.Outputs[0].Directory, "current.log"))
	if len(values) != 1 || values[0]["count"] != float64(42) || values[0]["ok"] != true || values[0]["blob"] != "YmVmb3Jl" ||
		values[0]["error"] != "fixture.failure" || values[0]["fathomry.call"] != "one" || values[0]["logger"] != "pipeline" {
		t.Fatal("typed JSON/correlation changed")
	}
	if _, ok := values[0]["caller"]; !ok {
		t.Fatal("opt-in caller absent")
	}
	if len(readJSON(t, filepath.Join(options.Outputs[1].Directory, "current.log"))) != 0 {
		t.Fatal("filtered sink wrote")
	}
	record := sink.records[0]
	if record.ctx.Value(contextKey{}) != "context-canary" || record.entry.LoggerName != "pipeline" || !record.entry.Caller.Defined ||
		record.entry.Caller.Line <= 0 || record.entry.Time.IsZero() {
		t.Fatal("structured context/metadata lost")
	}
	if record.fields[2].Interface != original || !errors.Is(record.fields[2].Interface.(error), nativeCause) {
		t.Fatal("error identity lost")
	}
	record.fields[0].Interface.([]byte)[0] = 'Y'
	sinks[0].Name = "changed"
	if result.Outcome.Value.SinksCopy()[0].Name != "local" || string(logger.fields[0].Interface.([]byte)) != "before" {
		t.Fatal("result or field storage aliases sink")
	}
	conformance.Private(t, result, "context-canary", "private-cause-canary")
}

type shortSink struct {
	cause  error
	count  int
	writes int
}

func (sink *shortSink) Write([]byte) (int, error) { sink.writes++; return sink.count, sink.cause }
func (*shortSink) Sync() error                    { return nil }

func TestPartialFailureSurvivesSuccessfulSyncAndIndependentReception(t *testing.T) {
	options := fileOptions(t, false)
	sink := &recordingSink{}
	fixture := bindFixture(t, options, sink, 4)
	native := errors.New("write-cause-canary")
	broken := &shortSink{cause: native, count: 2}
	fixture.logger.owner.branches[0].writer.native = broken
	info := fixture.logger.access.Info()
	result := logResult(t, fixture.logger, "partial")
	if !errors.Is(result.Err(), native) || broken.writes != 1 || sink.count() != 1 {
		t.Fatal("branch failure hid another sink or original cause")
	}
	receipt, err := fixture.logger.Sync(context.Background(), fault.Correlation{Call: "later-sync"})
	if value := resultOf(t, receipt, err); value.Err() != nil {
		t.Fatal("sync fixture failed")
	}
	if !errors.Is(result.Err(), native) {
		t.Fatal("later Sync erased write evidence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	expectedContext := func(name, id string) fault.Context {
		return fault.Context{Operation: name, Provider: ProviderID, Scope: "fixture", Source: "logs", Correlation: fault.Correlation{Call: id}}
	}
	conformance.Receive(t, ctx, fixture.inbox, []conformance.Expected[Result]{
		{Context: expectedContext("log", "partial"), Source: info, Limits: LimitsV1(options), Shape: invocation.Finite, Present: true, Final: true, Released: true,
			Primary: ErrWrite, Attempts: invocation.Attempts{Observed: 2}, Value: func(t testing.TB, value Result) {
				sinks := value.SinksCopy()
				if len(sinks) != 2 || sinks[0].State != Failed || sinks[0].Bytes != 2 || !errors.Is(sinks[0].Err, native) || sinks[1].State != Written {
					t.Error("partial branch oracle failed")
				}
			}},
		{Context: expectedContext("sync", "later-sync"), Source: info, Limits: LimitsV1(options), Shape: invocation.Finite, Present: true, Final: true, Released: true,
			Attempts: invocation.Attempts{Observed: 2}, Value: func(t testing.TB, value Result) {
				for _, sink := range value.SinksCopy() {
					if sink.State != Synced {
						t.Error("sync oracle failed")
					}
				}
			}},
	})
	conformance.Cause(t, result.Err(), func(value *fault.Error) bool { return errors.Is(value, native) })
}

func TestShortWritesAndEncodedEntryBounds(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int
		cause error
		want  error
	}{
		{"short", 0, nil, io.ErrShortWrite}, {"partial", 1, nil, io.ErrShortWrite}, {"negative", -1, nil, ErrWrite}, {"excess", 10000, nil, ErrWrite},
	} {
		t.Run(test.name, func(t *testing.T) {
			native := &shortSink{count: test.count, cause: test.cause}
			writer := &checkedWriter{native: native, maximum: 4}
			if _, err := writer.Write([]byte("test")); !errors.Is(err, test.want) {
				t.Fatal("invalid native write certified", err)
			}
			if _, err := writer.Write([]byte("large")); !errors.Is(err, ErrLimit) || native.writes != 1 {
				t.Fatal("over-limit record reached writer")
			}
		})
	}
	options := fileOptions(t, false)
	fixture := bindFixture(t, options, nil, 2)
	receipt, err := fixture.logger.Log(context.Background(), fault.Correlation{Call: "escaped"}, zapcore.InfoLevel, string(bytes.Repeat([]byte{0}, 500)))
	result := resultOf(t, receipt, err)
	if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.SinksCopy()[0].Bytes != 0 {
		t.Fatal("JSON escaping bypassed encoded limit")
	}
	if len(readJSON(t, filepath.Join(options.Outputs[0].Directory, "current.log"))) != 0 {
		t.Fatal("oversized native record written")
	}
}

func TestEvidenceSaturationAndPreCanceledCallsDoNotWrite(t *testing.T) {
	sink := &recordingSink{}
	fixture := bindFixture(t, OptionsV1{Name: "logs"}, sink, 1)
	if result := logResult(t, fixture.logger, "first"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	receipt, err := fixture.logger.Log(context.Background(), fault.Correlation{Call: "full"}, zapcore.InfoLevel, "message")
	if receipt != nil || !errors.Is(err, invocation.ErrEvidence) || sink.count() != 1 {
		t.Fatal("evidence capacity bypass")
	}
	drain(t, fixture.inbox)
	cause := errors.New("cancellation-canary")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	receipt, err = fixture.logger.Log(ctx, fault.Correlation{Call: "canceled"}, zapcore.InfoLevel, "message")
	if receipt != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || sink.count() != 1 || fixture.inbox.Usage().Outstanding != 0 {
		t.Fatal("canceled call entered sink or lost cause")
	}
}

func TestBlockedSinkRetainsLeaseAfterCancellationAndShutdown(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	sink := &recordingSink{onWrite: func(context.Context) { close(entered); <-release }}
	fixture := bindFixture(t, OptionsV1{Name: "logs", Timeout: time.Second}, sink, 2)
	ctx, cancel := context.WithCancelCause(context.Background())
	type callReturn struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan callReturn, 1)
	go func() {
		receipt, err := fixture.logger.Log(ctx, fault.Correlation{Call: "blocked"}, zapcore.InfoLevel, "message")
		done <- callReturn{receipt, err}
	}()
	<-entered
	cause := errors.New("stop-canary")
	cancel(cause)
	delivery, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delivery.Receipt().WaitReleased(ctx); !errors.Is(err, cause) {
		t.Fatal("wait lost cancellation")
	}
	if err := delivery.Release(); !errors.Is(err, invocation.ErrPending) {
		t.Fatal("blocked lease discarded")
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("shutdown claimed blocked sink released")
	}
	select {
	case <-done:
		t.Fatal("cancellation invented sink termination")
	default:
	}
	once.Do(func() { close(release) })
	returned := <-done
	result := resultOf(t, returned.receipt, returned.err)
	if result.Err() != nil || result.Outcome.Value.SinksCopy()[0].State != Written {
		t.Fatal("cancellation rewrote actual successful write")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func await(t testing.TB, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("checkpoint not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestAliasSharesAllowanceAndOwnership(t *testing.T) {
	sink := &recordingSink{}
	options := OptionsV1{Name: "logs", QueuedCalls: 1}
	fixture := bindFixture(t, options, sink, 4)
	alias := resource.Borrow("alias", fixture.assembly, fixture.selection)
	borrower, err := resource.Assemble(context.Background(), context.Background(), "borrower", alias)
	if err != nil {
		t.Fatal(err)
	}
	defer borrower.Close(context.Background())
	logger, err := Bind(borrower, alias, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if logger.access.Info().Configuration.Revision != fixture.logger.access.Info().Configuration.Revision {
		t.Fatal("alias changed source identity")
	}
	lease, err := fixture.logger.access.Acquire(context.Background(), defaults(options).reservation())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("queued-canary")
	done := make(chan error, 1)
	go func() {
		_, err := logger.Log(ctx, fault.Correlation{Call: "queued"}, zapcore.InfoLevel, "message")
		done <- err
	}()
	await(t, func() bool { return fixture.assembly.Snapshot().Sources[0].Usage.Queued == 1 })
	cancel(cause)
	if err := <-done; !errors.Is(err, cause) {
		t.Fatal("alias queue cause lost")
	}
	lease.Release()
	if sink.count() != 0 || fixture.inbox.Usage().Outstanding != 0 {
		t.Fatal("queued cancellation wrote or leaked evidence")
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("owner released live borrowing scope")
	}
	if result := logResult(t, logger, "borrower"); result.Err() != nil || result.Context.Source != "logs" || result.Context.Scope != "fixture" {
		t.Fatal("borrower lost native authority or attribution")
	}
	if err := borrower.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := logger.Log(context.Background(), fault.Correlation{Call: "closed"}, zapcore.InfoLevel, "message"); err == nil {
		t.Fatal("closed alias admitted")
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRecursionAndObserverRemainSeparate(t *testing.T) {
	sink := &recordingSink{}
	fixture := bindFixture(t, OptionsV1{Name: "logs"}, sink, 4)
	sink.onWrite = func(ctx context.Context) {
		receipt, err := fixture.logger.Log(ctx, fault.Correlation{Call: "recursive"}, zapcore.InfoLevel, "message")
		if receipt != nil || !errors.Is(err, ErrRecursion) {
			t.Error("recursive sink not rejected before admission")
		}
	}
	observer, err := invocation.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	fixture.logger.observer = observer
	if result := logResult(t, fixture.logger, "observed"); result.Observation != invocation.ObservationQueued {
		t.Fatal("observer was bypassed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := observer.ExportOne(ctx, func(ctx context.Context, _ invocation.Event) error {
		receipt, err := fixture.logger.Log(ctx, fault.Correlation{Call: "export"}, zapcore.InfoLevel, "diagnostic")
		result := resultOf(t, receipt, err)
		if result.Observation != invocation.ObservationSuppressed {
			t.Error("existing Observer recursion contract changed")
		}
		return result.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if sink.count() != 2 || fixture.inbox.Usage().Outstanding != 2 {
		t.Fatal("diagnostics replaced required evidence")
	}
}

func TestConcurrentSourcesAndDerivedLoggers(t *testing.T) {
	sink := &recordingSink{}
	fixture := bindFixture(t, OptionsV1{Name: "first", QueuedCalls: 64}, sink, 64)
	second := bindFixture(t, OptionsV1{Name: "second", QueuedCalls: 64}, sink, 64)
	var failed atomic.Bool
	var group sync.WaitGroup
	for index := range 32 {
		group.Go(func() {
			base := fixture.logger
			if index%2 == 1 {
				base = second.logger
			}
			derived, err := base.With(sdk.Int("worker", index))
			if err != nil {
				failed.Store(true)
				return
			}
			receipt, err := derived.Log(context.Background(), fault.Correlation{Call: "call-" + strconv.Itoa(index)}, zapcore.InfoLevel, "message")
			if err != nil {
				failed.Store(true)
				return
			}
			value, ok := receipt.Result()
			if !ok || value.Err() != nil || !value.Released {
				failed.Store(true)
			}
		})
	}
	group.Wait()
	if failed.Load() || sink.count() != 32 {
		t.Fatal("concurrent source/derivation contract failed")
	}
	for _, record := range sink.records {
		if len(record.fields) != 7 || record.fields[0].Key != "worker" {
			t.Fatal("fields crossed calls")
		}
	}
	if reflect.DeepEqual(fixture.logger.access.Info(), second.logger.access.Info()) {
		t.Fatal("independent sources share preparation")
	}
}
