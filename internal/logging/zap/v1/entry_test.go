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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"go.uber.org/zap/zapcore"
)

//go:noinline
func forwardEntry(logger *Logger, ctx context.Context, input Entry) (*invocation.Receipt[Result], error) {
	return logger.LogEntry(ctx, fault.Correlation{Call: "forwarded"}, input)
}

func TestEntryPreservesOriginalTimePCNameAndAssociation(t *testing.T) {
	var metadata []Entry
	type contextKey struct{}
	sink := &recordingSink{onWrite: func(ctx context.Context) {
		input, ok := EntryMetadata(ctx)
		if !ok || ctx.Value(contextKey{}) != "association" {
			t.Fatal("structured entry lost ingress metadata or caller association")
		}
		metadata = append(metadata, input)
	}}
	fixture := bindFixture(t, OptionsV1{Name: "ingress", Caller: true}, sink, 3)
	logger, err := fixture.logger.Named("prefix")
	if err != nil {
		t.Fatal(err)
	}
	var program [1]uintptr
	runtime.Callers(1, program[:])
	frames := runtime.CallersFrames(program[:])
	expected, _ := frames.Next()
	ctx := context.WithValue(context.Background(), contextKey{}, "association")
	input := Entry{PC: program[0], Level: zapcore.InfoLevel, Message: "original", Name: "component"}
	receipt, err := forwardEntry(logger, ctx, input)
	if result := resultOf(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	first := sink.records[0].entry
	if !first.Time.IsZero() || first.LoggerName != "prefix.component" || first.Message != "original" ||
		!first.Caller.Defined || first.Caller.PC != expected.PC || first.Caller.File != expected.File || first.Caller.Line != expected.Line || first.Caller.Function != expected.Function {
		t.Fatal("wrapper replaced original metadata or caller frame", first.Caller)
	}
	if metadata[0].PC != program[0] || !metadata[0].Time.IsZero() || metadata[0].Name != "prefix.component" {
		t.Fatal("ingress return-PC/time was confused with adjusted native caller data")
	}
	encoded, err := zapcore.NewJSONEncoder(encoderConfig()).EncodeEntry(first, sink.records[0].fields)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	err = json.Unmarshal(encoded.Bytes(), &record)
	encoded.Free()
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := record["ts"]; exists {
		t.Fatal("zero event time became a fabricated observation time")
	}
	input.Time = time.Date(1960, 2, 3, 4, 5, 6, 7, time.FixedZone("input", 9*60*60))
	input.PC = 0
	receipt, err = forwardEntry(logger, ctx, input)
	if result := resultOf(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	second := sink.records[1].entry
	if !second.Time.Equal(input.Time) || second.Time.Location() != time.UTC || second.Caller.Defined || second.Caller.PC != 0 || metadata[1].PC != 0 || metadata[1].Time != input.Time {
		t.Fatal("explicit time or absent PC was not preserved")
	}
	receipt, err = logger.Log(ctx, fault.Correlation{Call: "direct"}, zapcore.InfoLevel, "direct")
	if result := resultOf(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	third := sink.records[2].entry
	if third.Time.IsZero() || !strings.HasSuffix(third.Caller.Function, ".TestEntryPreservesOriginalTimePCNameAndAssociation") || strings.Contains(third.Caller.Function, "forwardEntry") {
		t.Fatal("legacy Log captured its implementation wrapper instead of business caller", third.Caller)
	}
	if _, present := EntryMetadata(nil); present {
		t.Fatal("nil context manufactured ingress metadata")
	}
}

func TestEntryRefusalsAndPerCallNamesDoNotConsumeDerivations(t *testing.T) {
	sink := &recordingSink{}
	fixture := bindFixture(t, OptionsV1{Name: "ingress"}, sink, 1)
	for _, input := range []Entry{
		{Level: zapcore.DPanicLevel}, {Level: zapcore.FatalLevel},
		{Message: "\xff"}, {Message: strings.Repeat("x", MaxMessageBytes+1)},
		{Name: "invalid name"}, {Name: strings.Repeat("n", 129)},
	} {
		if receipt, err := fixture.logger.LogEntry(context.Background(), fault.Correlation{Call: "refused"}, input); receipt != nil || err == nil {
			t.Fatal("invalid explicit entry was admitted")
		}
	}
	canceled, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("caller-cancellation")
	cancel(cause)
	if receipt, err := fixture.logger.LogEntry(canceled, fault.Correlation{Call: "canceled"}, Entry{Message: "not slog"}); receipt != nil || !errors.Is(err, cause) {
		t.Fatal("direct explicit ingress ignored caller cancellation", err)
	}
	if fixture.inbox.Usage().Outstanding != 0 || sink.count() != 0 {
		t.Fatal("refused ingress produced effects or accepted evidence")
	}
	for range MaxDerivedViews + 1 {
		receipt, err := fixture.logger.LogEntry(context.Background(), fault.Correlation{Call: "entry"}, Entry{Name: "per-call", Level: zapcore.InfoLevel})
		if result := resultOf(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, fixture.inbox)
	}
	if fixture.logger.owner.derivations != 0 || fixture.logger.owner.derivationBytes != 0 {
		t.Fatal("explicit per-call name consumed retained-view allowance")
	}
	for _, entry := range sink.records {
		if entry.entry.LoggerName != "per-call" || !entry.entry.Time.IsZero() {
			t.Fatal("per-call explicit metadata changed")
		}
	}
	if _, err := fixture.logger.Named("retained"); err != nil {
		t.Fatal("per-call metadata exhausted the retained facade allowance", err)
	}
}

func TestClosedBinaryArrayUsesBase64InActualFile(t *testing.T) {
	options := fileOptions(t, false)
	fixture := bindFixture(t, options, nil, 1)
	if result := logResult(t, fixture.logger, "binary-array", Field("binary", Value{Kind: "array", Array: []Value{{Kind: "bytes", Bytes: []byte{0, 255, 128}}}})); result.Err() != nil {
		t.Fatal(result.Err())
	}
	records := readJSON(t, options.Outputs[0].Directory+"/current.log")
	if len(records) != 1 || len(records[0]["binary"].([]any)) != 1 || !bytes.Equal([]byte(records[0]["binary"].([]any)[0].(string)), []byte("AP+A")) {
		t.Fatal("actual local record corrupted binary array encoding")
	}
}
