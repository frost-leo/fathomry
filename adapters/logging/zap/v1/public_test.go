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

package zap_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func ptr[T any](value T) *T { return &value }

type recorder struct {
	mu      sync.Mutex
	records []zap.Record
	writes  int
	syncs   int
	err     error
}

func (sink *recorder) Write(_ context.Context, record zap.Record) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.records = append(sink.records, record)
	sink.writes++
	return sink.err
}
func (sink *recorder) Sync(context.Context) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.syncs++
	return nil
}
func (sink *recorder) snapshot() []zap.Record {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]zap.Record(nil), sink.records...)
}
func loggerOwner(t *testing.T, settings zap.Settings, sink zap.StructuredSink) (*zap.Owner, zap.Dependencies) {
	t.Helper()
	prepared, err := zap.Prepare(settings)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[zap.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	deps := zap.Dependencies{Runtime: runtime, Evidence: inbox, Structured: sink}
	owner, err := prepared.Open(context.Background(), deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error("close", err)
		}
		if !owner.ShutdownComplete() {
			t.Error("source retained")
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error("runtime", err)
		}
		if err := inbox.Seal(); err != nil {
			t.Error(err)
		}
		for {
			delivery, err := inbox.NextReleased(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	return owner, deps
}
func result(t *testing.T, deps zap.Dependencies, receipt *adapters.Receipt[zap.Result], err error) (zap.Result, error) {
	t.Helper()
	if err != nil {
		t.Fatal("submission", err)
	}
	if receipt == nil {
		t.Fatal("missing receipt")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := deps.Evidence.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	evidenceReceipt, receiptErr := delivery.Receipt()
	if receiptErr != nil {
		t.Fatal(receiptErr)
	}
	actual, _ := evidenceReceipt.Snapshot()
	if actual.Info().Sequence != snapshot.Info().Sequence {
		t.Fatal("evidence identity")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	if !present {
		t.Fatal("missing native result", snapshot.Err())
	}
	return value, snapshot.Err()
}
func logResult(t *testing.T, deps zap.Dependencies, client *zap.Client, level zapcore.Level, message string, fields ...zapcore.Field) (zap.Result, error) {
	t.Helper()
	receipt, err := client.Log(context.Background(), level, message, fields...)
	return result(t, deps, receipt, err)
}

func TestFinalPreparationAndPreEffectRefusal(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	settings := zap.Settings{Name: "final", Version: 1, Outputs: []zap.Output{{Name: "file", Kind: "file", Directory: directory}}, QueuedCalls: ptr(2)}
	prepared, err := zap.Prepare(settings)
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	if metadata.ActiveCalls != 1 || metadata.QueuedCalls != 2 || metadata.MaxEntryBytes != 64<<10 || metadata.Files != 1 || metadata.DerivationBytes != 8<<20 || metadata.SourceBytes < metadata.DerivationBytes {
		t.Fatal("wrong final metadata", metadata)
	}
	settings.Outputs[0].Directory = "/not-the-frozen-selection"
	*settings.QueuedCalls = 9
	if prepared.Metadata().QueuedCalls != 2 {
		t.Fatal("mutable preparation")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatal("preparation acquired files", entries, err)
	}
	invalid := zap.Settings{Name: "zero", Version: 1, Structured: true, Timeout: ptr(time.Duration(0))}
	if _, err := zap.Prepare(invalid); !errors.Is(err, zap.ErrInput) {
		t.Fatal("explicit zero defaulted", err)
	}
	policy, _ := prepared.Policy()
	policy.Runtime.MaxWorkBytes--
	runtime, _ := adapters.New(context.Background(), policy.Runtime)
	defer runtime.Close(context.Background())
	inbox, _ := adapters.NewInbox[zap.Result](policy.Evidence)
	owner, err := prepared.Open(context.Background(), zap.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil || !errors.Is(err, zap.ErrLimit) {
		t.Fatal("insufficient policy acquired owner", owner, err)
	}
	entries, _ = os.ReadDir(directory)
	if len(entries) != 0 {
		t.Fatal("insufficient policy touched files")
	}
}

func TestLocalDecodedClosedFieldsAndCaller(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	owner, deps := loggerOwner(t, zap.Settings{Name: "local", Version: 1, Caller: ptr(true), Outputs: []zap.Output{{Name: "file", Kind: "file", Directory: directory, Level: ptr("debug")}}}, nil)
	bytes := []byte{0xff, 0, 1}
	fields := []zapcore.Field{sdk.Uint64("large", math.MaxUint64), sdk.Int8("small", -7), sdk.Float32("f32", 0.1), sdk.ByteString("text", []byte("a\x00b")), sdk.Error(nil),
		zap.Field("tree", logging.Group(logging.Field{Key: "null", Value: logging.Null()}, logging.Field{Key: "empty", Value: logging.Array()}, logging.Field{Key: "binary", Value: logging.Array(logging.Binary(bytes))}))}
	client, err := owner.Client().WithID("public-request")
	if err != nil {
		t.Fatal(err)
	}
	_, _, line, _ := runtime.Caller(0)
	receipt, err := client.Log(context.Background(), zapcore.InfoLevel, "native", fields...)
	value, outcome := result(t, deps, receipt, err)
	if outcome != nil || len(value.SinksCopy()) != 1 || value.SinksCopy()[0].State != zap.Written {
		t.Fatal("write", outcome)
	}
	bytes[0] = 1
	data, err := os.ReadFile(filepath.Join(directory, "current.log"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded["large"]) != "18446744073709551615" || string(decoded["small"]) != "-7" || string(decoded["f32"]) != "0.1" {
		t.Fatal("native scalar fidelity", string(data))
	}
	var caller string
	_ = json.Unmarshal(decoded["caller"], &caller)
	if !strings.HasSuffix(caller, "public_test.go:"+itoa(line+1)) {
		t.Fatal("wrapper caller", caller, line)
	}
	if string(decoded["tree"]) != `{"null":null,"empty":[],"binary":["/wAB"]}` {
		t.Fatal("closed data", string(decoded["tree"]))
	}
	var nativeCall string
	if err := json.Unmarshal(decoded["fathomry.call"], &nativeCall); err != nil {
		t.Fatal(err)
	}
	if nativeCall == "" || nativeCall != value.NativeCorrelation().Call || value.Attribution().ID != "public-request" || nativeCall == value.Attribution().ID || value.SinksCopy()[0].Kind != "file" {
		t.Fatal("native/public correlation or output kind lost")
	}
	receipt, err = owner.Client().Sync(context.Background())
	_, outcome = result(t, deps, receipt, err)
	if outcome != nil {
		t.Fatal(outcome)
	}
}

func TestStructuredOutcomesFrozenViewsAndCanceledDirect(t *testing.T) {
	sink := &recorder{}
	owner, deps := loggerOwner(t, zap.Settings{Name: "structured", Version: 1, Structured: true, ExtensionLevel: ptr("warn"), Caller: ptr(true)}, sink)
	value, err := logResult(t, deps, owner.Client(), zapcore.InfoLevel, "filtered")
	if err != nil || value.SinksCopy()[0].State != zap.Filtered || len(sink.snapshot()) != 0 || value.NativeCorrelation().Call == "" || value.SinksCopy()[0].Kind != "structured" {
		t.Fatal("filter", err)
	}
	view, err := owner.Client().With(context.Background(), sdk.String("bound", "first"))
	if err != nil {
		t.Fatal(err)
	}
	view, err = view.Named("business")
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 123, time.UTC)
	receipt, submit := view.LogEntry(context.Background(), zap.Entry{Time: stamp, Level: zapcore.ErrorLevel, Message: "derived"}, sdk.String("event", "second"))
	value, err = result(t, deps, receipt, submit)
	if err != nil || value.SinksCopy()[0].State != zap.Written {
		t.Fatal(err)
	}
	record := sink.snapshot()[0]
	matched := false
	for _, field := range record.Fields {
		if field.Key == "fathomry.call" {
			matched = field.Value.StringValue() == value.NativeCorrelation().Call
		}
	}
	if !matched {
		t.Fatal("retained native correlation lost")
	}
	if !record.Time.Equal(stamp) || record.PC != 0 || record.Caller.Defined || record.Name != "business" {
		t.Fatal("original ingress")
	}
	if err := view.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A retained-family lifecycle result is independent of event evidence.
	delivery, err := deps.Evidence.NextReleased(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Ack()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err := owner.Client().Log(canceled, zapcore.ErrorLevel, "not sent"); receipt != nil || err == nil {
		t.Fatal("canceled direct accepted")
	}
	if len(sink.snapshot()) != 1 {
		t.Fatal("canceled direct wrote")
	}
}

func TestSafeSlogChronologyTimePCAndFrameworkErrors(t *testing.T) {
	sink := &recorder{}
	owner, deps := loggerOwner(t, zap.Settings{Name: "gateway", Version: 1, Structured: true, Caller: ptr(true)}, sink)
	handler, err := owner.Client().Slog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close(context.Background())
	logger := slog.New(handler).With("before", "root").WithGroup("outer").With("middle", 7).WithGroup("inner")
	canceled, cancel := context.WithCancel(context.WithValue(context.Background(), struct{}{}, "association"))
	cancel()
	logger.InfoContext(canceled, "original", "leaf", true)
	delivery, err := deps.Evidence.NextReleased(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Ack()
	records := sink.snapshot()
	if len(records) != 1 || !strings.HasSuffix(records[0].Caller.Function, "TestSafeSlogChronologyTimePCAndFrameworkErrors") {
		t.Fatal("slog caller/cancel", len(records))
	}
	fields := map[string]logging.Value{}
	for _, field := range records[0].Fields {
		fields[field.Key] = field.Value
	}
	if fields["before"].StringValue() != "root" {
		t.Fatal("earlier field moved")
	}
	outer := fields["outer"].FieldsCopy()
	if len(outer) != 2 || outer[0].Key != "middle" || outer[1].Key != "inner" || outer[1].Value.FieldsCopy()[0].Key != "leaf" {
		t.Fatal("group chronology")
	}
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "logging_zap", BaseLocale: "en", Resources: zap.Resources(), Directory: "resources", Definitions: zap.Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	var def failure.Definition
	for _, item := range zap.Definitions() {
		if item.Code == zap.ErrWrite {
			def = item
		}
	}
	original, _ := failure.New(def, failure.Location{Operation: "business"}, errors.New("PRIVATE_CAUSE"))
	emitted := framework.NewErrorLog(slog.New(handler), presenter).Emit(context.Background(), original)
	if !emitted.Recognized {
		t.Fatal("framework presentation")
	}
	delivery, err = deps.Evidence.NextReleased(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Ack()
	records = sink.snapshot()
	if len(records) != 2 {
		t.Fatal("framework not emitted")
	}
	for _, field := range records[1].Fields {
		if field.Key == "error" {
			for _, part := range field.Value.FieldsCopy() {
				if strings.Contains(part.Value.StringValue(), "PRIVATE_CAUSE") {
					t.Fatal("cause leaked")
				}
			}
		}
	}
}

func itoa(value int) string { return fmt.Sprint(value) }
