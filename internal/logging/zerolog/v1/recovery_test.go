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
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type managedSequence struct {
	outcomes             []RecordOutcome
	calls, syncs, closes int
	panicValue           any
}

type nilManagedError struct{}

func (*nilManagedError) Error() string { panic("typed nil error formatted") }

func (writer *managedSequence) WriteManagedRecord(context.Context, Record) RecordOutcome {
	writer.calls++
	if writer.panicValue != nil {
		panic(writer.panicValue)
	}
	if writer.calls <= len(writer.outcomes) {
		return writer.outcomes[writer.calls-1]
	}
	return RecordOutcome{State: RecordAccepted}
}
func (writer *managedSequence) Sync() error  { writer.syncs++; return nil }
func (writer *managedSequence) Close() error { writer.closes++; return nil }

func managedFixture(t *testing.T, writer ManagedRecordWriter, local io.Writer) *fixture {
	t.Helper()
	options := OptionsV1{Name: "managed", Sinks: []SinkV1{{Name: "remote", Kind: "managed-record"}}}
	bindings := BindingsV1{ManagedRecords: map[string]ManagedRecordWriter{"remote": writer}}
	if local != nil {
		options.Sinks = append(options.Sinks, SinkV1{Name: "local", Kind: "writer"})
		bindings.Writers = map[string]io.Writer{"local": local}
	}
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := prepared.Select(bindings)
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, prepared.Metadata().Limits)
	assembly, err := resource.Assemble(context.Background(), context.Background(), "managed", selection)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](8, 8*prepared.Metadata().EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	logger, err := Bind(assembly, selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	value := &fixture{logger: logger, assembly: assembly, selected: selection, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
		drain(t, inbox)
	})
	return value
}

func TestManagedRejectionOnlyFailsItsIndependentRecord(t *testing.T) {
	rejected, stopped := errors.New("record-refused"), errors.New("destination-closed")
	writer := &managedSequence{outcomes: []RecordOutcome{{State: RecordRejected, Err: rejected}, {State: RecordAccepted}, {State: RecordStopped, Err: stopped}, {State: RecordAccepted}}}
	var local bytes.Buffer
	fixture := managedFixture(t, writer, &local)
	first := emit(t, fixture, "first", Info, "first")
	sinks := first.Outcome.Value.SinksCopy()
	if !errors.Is(first.Err(), rejected) || !sinks[0].Attempted || sinks[0].Accepted || sinks[0].Stopped || sinks[0].BytesKnown || !sinks[1].Accepted {
		t.Fatal("record-local refusal changed output health or sibling evidence")
	}
	second := emit(t, fixture, "second", Info, "second")
	if second.Err() != nil || !second.Outcome.Value.SinksCopy()[0].Accepted || writer.calls != 2 {
		t.Fatal("next independent event did not recover", second.Err())
	}
	third := emit(t, fixture, "third", Info, "third")
	if !errors.Is(third.Err(), stopped) || !third.Outcome.Value.SinksCopy()[0].Stopped {
		t.Fatal("terminal outcome did not stop output")
	}
	fourth := emit(t, fixture, "fourth", Info, "fourth")
	if writer.calls != 3 || fourth.Outcome.Value.SinksCopy()[0].Attempted || !fourth.Outcome.Value.SinksCopy()[0].Stopped || !errors.Is(fourth.Err(), stopped) {
		t.Fatal("stopped destination was retried or original failure lost")
	}
	if len(decodeRecords(t, local.Bytes())) != 4 {
		t.Fatal("managed failure suppressed healthy local sibling")
	}
	if !errors.Is(first.Err(), rejected) || first.Outcome.Value.SinksCopy()[0].Stopped {
		t.Fatal("later success changed historical refusal")
	}
	if receipt, err := fixture.logger.Sync(context.Background(), correlation("sync")); receipt != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatal("borrowed managed sink became owned maintenance", err)
	}
	if receipt, err := fixture.logger.Rotate(context.Background(), correlation("rotate")); receipt != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatal("borrowed managed sink became owned rotation", err)
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if writer.syncs != 0 || writer.closes != 0 {
		t.Fatal("borrowed output was maintained or closed")
	}
}

func TestManagedMalformedAndPanicOutcomesStopConservatively(t *testing.T) {
	cause := errors.New("private-managed-cause")
	for _, test := range []struct {
		name       string
		value      RecordOutcome
		panicValue any
	}{
		{"empty", RecordOutcome{}, nil},
		{"accepted-error", RecordOutcome{State: RecordAccepted, Err: cause}, nil},
		{"rejected-nil", RecordOutcome{State: RecordRejected}, nil},
		{"stopped-nil", RecordOutcome{State: RecordStopped}, nil},
		{"rejected-typed-nil", RecordOutcome{State: RecordRejected, Err: (*nilManagedError)(nil)}, nil},
		{"unknown", RecordOutcome{State: "unknown", Err: cause}, nil},
		{"panic", RecordOutcome{}, "private-panic-canary"},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := &managedSequence{outcomes: []RecordOutcome{test.value}, panicValue: test.panicValue}
			fixture := managedFixture(t, writer, nil)
			first := emit(t, fixture, "bad", Info, "first")
			report := first.Outcome.Value.SinksCopy()[0]
			if first.Err() == nil || !report.Stopped || report.Accepted || !report.Attempted || report.BytesKnown {
				t.Fatal("malformed result manufactured acceptance or recovery")
			}
			second := emit(t, fixture, "later", Info, "later")
			if writer.calls != 1 || second.Outcome.Value.SinksCopy()[0].Attempted || !second.Outcome.Value.SinksCopy()[0].Stopped {
				t.Fatal("malformed destination reentered")
			}
			conformance.Private(t, first.Err(), "private-managed-cause", "private-panic-canary")
		})
	}
}

type recoverableLegacyRecord struct{ calls int }

func (writer *recoverableLegacyRecord) WriteRecord(context.Context, Record) error {
	writer.calls++
	if writer.calls == 1 {
		return errors.New("legacy-first-failure")
	}
	return nil
}

type recoverableLegacyBytes struct{ calls int }

func (writer *recoverableLegacyBytes) Write(data []byte) (int, error) {
	writer.calls++
	if writer.calls == 1 {
		return len(data), errors.New("legacy-byte-failure")
	}
	return len(data), nil
}

func TestLegacyOutputsRetainAnyErrorFailureStop(t *testing.T) {
	record, writer := &recoverableLegacyRecord{}, &recoverableLegacyBytes{}
	fixture := bindFixture(t, OptionsV1{Name: "legacy", Sinks: []SinkV1{{Name: "record", Records: record}, {Name: "writer", Writer: writer}}}, 4)
	first := emit(t, fixture, "first", Info, "first")
	second := emit(t, fixture, "second", Info, "second")
	if first.Err() == nil || second.Err() == nil || record.calls != 1 || writer.calls != 1 {
		t.Fatal("legacy borrowed failure-stop was relaxed")
	}
	for _, report := range second.Outcome.Value.SinksCopy() {
		if !report.Stopped || report.Attempted || report.Accepted {
			t.Fatal("legacy stopped facts changed")
		}
	}
}

func TestOwnedFileRetainsConservativeFailureStop(t *testing.T) {
	options := fileOptions(t, false, 2)
	var healthy bytes.Buffer
	options.Sinks = append(options.Sinks, SinkV1{Name: "healthy", Writer: &healthy})
	fixture := bindFixture(t, options, 4)
	fixture.allowCloseError = true
	if err := fixture.logger.owner.sinks[0].file.file.Close(); err != nil {
		t.Fatal(err)
	}
	first := emit(t, fixture, "broken-file", Info, "first")
	firstSinks := first.Outcome.Value.SinksCopy()
	if first.Err() == nil || !firstSinks[0].Attempted || firstSinks[0].Accepted || !firstSinks[0].Stopped || !firstSinks[1].Accepted {
		t.Fatal("actual file failure did not stop only its output")
	}
	next := emit(t, fixture, "after-file-failure", Info, "next")
	if next.Err() == nil || next.Outcome.Value.SinksCopy()[0].Attempted || !next.Outcome.Value.SinksCopy()[0].Stopped || !next.Outcome.Value.SinksCopy()[1].Accepted {
		t.Fatal("file output silently recovered after an actual write failure")
	}
	if err := fixture.assembly.Close(context.Background()); err == nil {
		t.Fatal("file cleanup hid the recovery responsibility")
	}
	if _, err := os.Stat(filepath.Join(options.Sinks[0].File.Directory, lockName)); err != nil {
		t.Fatal("failed file lost its recovery marker", err)
	}
	if len(decodeRecords(t, healthy.Bytes())) != 2 {
		t.Fatal("file failure suppressed healthy output")
	}
	drain(t, fixture.inbox)
}
