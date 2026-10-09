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
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
)

//go:noinline
func forwardEntry(logger *Logger, ctx context.Context, entry Entry) (*invocation.Receipt[Result], error) {
	return logger.LogEntry(ctx, correlation("forwarded"), entry)
}

func TestEntryPreservesOriginalTimePCAndAssociation(t *testing.T) {
	collector := &recordCollector{}
	fixture := bindFixture(t, OptionsV1{Name: "entry", Caller: true, Sinks: []SinkV1{{Name: "record", Records: collector}}}, 1)
	var program [1]uintptr
	runtime.Callers(1, program[:])
	expected, _ := runtime.CallersFrames(program[:]).Next()
	ctx := context.WithValue(context.Background(), contextKey{}, "association")
	input := Entry{PC: program[0], Level: Info, Message: "original"}
	receipt, err := forwardEntry(fixture.logger, ctx, input)
	if result := observed(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	first := collector.records[0]
	caller := first.Caller()
	if !first.Time().IsZero() || first.PC() != program[0] || !caller.Defined || caller.PC != expected.PC || caller.File != expected.File || caller.Line != expected.Line || caller.Function != expected.Function || collector.contexts[0] != "association" {
		t.Fatal("wrapper replaced original ingress metadata or caller association", caller)
	}
	wire := decodeRecords(t, first.JSONCopy())[0]
	if _, present := wire["time"]; present {
		t.Fatal("absent timestamp became fabricated observation time")
	}
	if wire["caller"].(map[string]any)["function"] != expected.Function {
		t.Fatal("actual encoded caller did not name original business frame")
	}
	drain(t, fixture.inbox)
	input.Time = time.Date(1960, 2, 3, 4, 5, 6, 7, time.FixedZone("input", 9*60*60))
	input.PC = 0
	receipt, err = forwardEntry(fixture.logger, ctx, input)
	if result := observed(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	second := collector.records[1]
	if !second.Time().Equal(input.Time) || second.Time().Location() != time.UTC || second.PC() != 0 || second.Caller().Defined {
		t.Fatal("explicit time or absent PC changed")
	}
	drain(t, fixture.inbox)
	_, _, directLine, _ := runtime.Caller(0)
	receipt, err = fixture.logger.Log(ctx, correlation("direct"), Info, "business")
	if result := observed(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	third := collector.records[2]
	if third.Time().IsZero() || third.PC() == 0 || third.Caller().Line != directLine+1 || !strings.HasSuffix(third.Caller().Function, ".TestEntryPreservesOriginalTimePCAndAssociation") {
		t.Fatal("Log captured a new implementation wrapper rather than its business caller", third.Caller())
	}
	drain(t, fixture.inbox)
}

func TestEntryRefusalsAndWithSnapshot(t *testing.T) {
	collector := &recordCollector{}
	fixture := bindFixture(t, OptionsV1{Name: "entry", Sinks: []SinkV1{{Name: "record", Records: collector}}}, 1)
	for _, input := range []Entry{{Level: "invalid"}, {Level: Info, Message: "\xff"}, {Level: Info, Time: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}} {
		if receipt, err := fixture.logger.LogEntry(context.Background(), correlation("invalid"), input); receipt != nil || err == nil {
			t.Fatal("invalid explicit ingress was admitted", err)
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("entry-cancellation")
	cancel(cause)
	if receipt, err := fixture.logger.LogEntry(ctx, correlation("canceled"), Entry{Level: Info}); receipt != nil || !errors.Is(err, cause) {
		t.Fatal("direct ingress ignored caller cancellation", err)
	}
	if fixture.inbox.Usage().Outstanding != 0 || len(collector.records) != 0 {
		t.Fatal("refused ingress reached evidence/output")
	}
	data := []byte{0, 255}
	attrs := []slog.Attr{slog.Any("binary", Value{Kind: "bytes", Bytes: data})}
	view, err := fixture.logger.With(attrs...)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 77
	attrs[0] = slog.Any("changed", nil)
	var program [1]uintptr
	runtime.Callers(1, program[:])
	receipt, err := view.LogEntry(context.Background(), correlation("derived"), Entry{Level: Fatal, PC: program[0]}, slog.Bool("event", true))
	if result := observed(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	record := collector.records[0]
	values, err := AttributesValues(record.AttributesCopy(), 16<<10)
	if err != nil || len(values) != 2 || values[0].Key != "binary" || values[0].Value.Bytes[0] != 0 || values[1].Key != "event" || record.Level() != Fatal || record.Caller().Defined || record.PC() != program[0] {
		t.Fatal("derived snapshot, severity-only Fatal or caller-disabled ingress changed", err)
	}
	drain(t, fixture.inbox)
	if next, err := view.With(slog.Any("binary", nil)); next != nil || !errors.Is(err, ErrInput) {
		t.Fatal("derived/event duplicate attributes were not refused", err)
	}
	if receipt, err := view.Log(context.Background(), correlation("duplicate"), Info, "", slog.Any("binary", nil)); receipt != nil || !errors.Is(err, ErrInput) {
		t.Fatal("event duplicate reached native output", err)
	}
	if fixture.logger.owner.derivations != 1 || len(fixture.logger.attributes) != 0 {
		t.Fatal("failed derivation consumed allowance or mutated parent")
	}
}
