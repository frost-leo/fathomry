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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type preparedWriter struct {
	bytes.Buffer
	syncs, closes int
}

func (writer *preparedWriter) Sync() error  { writer.syncs++; return nil }
func (writer *preparedWriter) Close() error { writer.closes++; return nil }

type preparedRecords struct {
	calls, syncs, closes int
}

func (writer *preparedRecords) WriteRecord(context.Context, Record) error {
	writer.calls++
	return nil
}
func (writer *preparedRecords) Sync() error  { writer.syncs++; return nil }
func (writer *preparedRecords) Close() error { writer.closes++; return nil }
func (writer *preparedRecords) WriteManagedRecord(context.Context, Record) RecordOutcome {
	writer.calls++
	return RecordOutcome{State: RecordAccepted}
}

func TestPreparedExactResolvedSettingsBindingsAndBudgets(t *testing.T) {
	directory := privateDir(t)
	options := OptionsV1{Name: "prepared", MaxRecordBytes: 1024, QueuedCalls: 2, Sinks: []SinkV1{
		{Name: "writer", Kind: "writer"}, {Name: "record", Kind: "record"}, {Name: "managed", Kind: "managed-record"},
		{Name: "file", Kind: "file", File: &FileOptionsV1{Directory: directory, MaxBytes: 4096, Backups: 3, Compress: true}},
	}}
	layer := resource.Layer{Kind: resource.Local, Content: []byte("max_record_bytes: 2048\nqueued_calls: 0\nmin_level: trace\ncaller: true\n")}
	prepared, err := PrepareV1(options, layer)
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	if metadata.Limits.Active != 1 || metadata.Limits.Queued != 0 || metadata.Limits.QueuedBytes != 0 ||
		metadata.WorkBytes != 24*2048+2<<20 || metadata.Limits.Bytes != metadata.WorkBytes || metadata.EvidenceBytes != 64<<10 ||
		metadata.Sinks != 4 || metadata.Files != 1 || metadata.Writers != 1 || metadata.Records != 1 || metadata.ManagedRecords != 1 ||
		metadata.MaxRecordBytes != 2048 || metadata.DerivationBytes != MaxDerivedBytes || metadata.MaxDerivedViews != MaxDerivedViews ||
		metadata.SourceBytes <= metadata.DerivationBytes || metadata.PolicyBytes <= 0 || metadata.ViewBytes < 2048 {
		t.Fatalf("final native envelopes missing: %+v", metadata)
	}
	archive := int64(2*4096 + 64<<10)
	if metadata.FileBytes != 4096+3*archive || metadata.MaintenanceFileBytes != 4096+5*archive {
		t.Fatal("file transition envelope differs from native compression cap")
	}
	if entries, err := os.ReadDir(directory); err != nil || len(entries) != 0 {
		t.Fatal("preparation acquired file ownership", err)
	}
	options.Sinks[3].File.Directory = "mutated"
	layer.Content[0] = 'X'
	copy := prepared.Options()
	if copy.MinLevel != Trace || !copy.Caller || copy.QueuedCalls != 0 || copy.Timeout != 5*time.Second || copy.Sinks[0].MinLevel != Trace {
		t.Fatal("effective options lost defaults/overlays")
	}
	copy.Sinks[3].File.Directory = "mutated-again"
	if prepared.Options().Sinks[3].File.Directory != directory || prepared.Options().Sinks[0].Writer != nil || prepared.Options().Sinks[1].Records != nil {
		t.Fatal("options snapshot aliases data or exposes live authority")
	}
	writer, records, managed := &preparedWriter{}, &preparedRecords{}, &preparedRecords{}
	bindings := BindingsV1{Writers: map[string]io.Writer{"writer": writer}, Records: map[string]RecordWriter{"record": records}, ManagedRecords: map[string]ManagedRecordWriter{"managed": managed}}
	selected, err := prepared.Select(bindings)
	if err != nil {
		t.Fatal(err)
	}
	bindings.Writers["writer"], bindings.Records["record"], bindings.ManagedRecords["managed"] = io.Discard, nil, nil
	selected = resource.WithLimits(selected, metadata.Limits)
	assembly, err := resource.Assemble(context.Background(), context.Background(), "prepared", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = assembly.Close(context.Background()) })
	inbox, err := invocation.NewInbox[Result](4, 4*metadata.EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	logger, err := Bind(assembly, selected, inbox, nil)
	if err != nil || logger.owner.prepared.state != prepared.state || logger.policy != prepared.state || !logger.Enabled(Trace) || logger.Enabled(Level("unknown")) {
		t.Fatal("native construction did not use frozen data", err)
	}
	receipt, err := logger.Log(context.Background(), correlation("prepared"), Trace, "frozen")
	result := observed(t, receipt, err)
	if result.Err() != nil || writer.Len() == 0 || records.calls != 1 || managed.calls != 1 {
		t.Fatal("selected binding maps changed after selection", result.Err())
	}
	for _, sink := range result.Outcome.Value.SinksCopy() {
		if !sink.Accepted || !sink.Attempted {
			t.Fatal("required physical output not entered")
		}
	}
	for _, operation := range []func(context.Context, fault.Correlation) (*invocation.Receipt[Result], error){logger.Sync, logger.Rotate} {
		receipt, err := operation(context.Background(), correlation("maintenance"))
		value := observed(t, receipt, err)
		if value.Err() != nil {
			t.Fatal(value.Err())
		}
		for index, sink := range value.Outcome.Value.SinksCopy() {
			if index != 3 && (sink.Attempted || sink.Accepted || sink.Synced || sink.Rotated) {
				t.Fatal("borrowed output was reported as owned-file maintenance")
			}
		}
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if writer.syncs != 0 || writer.closes != 0 || records.syncs != 0 || records.closes != 0 || managed.syncs != 0 || managed.closes != 0 {
		t.Fatal("borrowed destination ownership was seized")
	}
	drain(t, inbox)
}

func TestPreparedBindingsRejectBeforeFileEffects(t *testing.T) {
	directory := privateDir(t)
	prepared, err := PrepareV1(OptionsV1{Name: "bindings", Sinks: []SinkV1{
		{Name: "writer", Kind: "writer"}, {Name: "record", Kind: "record"}, {Name: "managed", Kind: "managed-record"},
		{Name: "file", Kind: "file", File: &FileOptionsV1{Directory: directory}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing", "unused", "writer-nil", "record-nil", "managed-nil", "wrong-kind", "file-binding"} {
		t.Run(name, func(t *testing.T) {
			bindings := BindingsV1{Writers: map[string]io.Writer{"writer": io.Discard}, Records: map[string]RecordWriter{"record": &preparedRecords{}}, ManagedRecords: map[string]ManagedRecordWriter{"managed": &preparedRecords{}}}
			switch name {
			case "missing":
				delete(bindings.Writers, "writer")
			case "unused":
				bindings.Writers["unused"] = io.Discard
			case "writer-nil":
				var value *bytes.Buffer
				bindings.Writers["writer"] = value
			case "record-nil":
				var value *preparedRecords
				bindings.Records["record"] = value
			case "managed-nil":
				var value *preparedRecords
				bindings.ManagedRecords["managed"] = value
			case "wrong-kind":
				bindings.ManagedRecords["record"] = &preparedRecords{}
			case "file-binding":
				bindings.Writers["file"] = io.Discard
			}
			if _, err := prepared.Select(bindings); !errors.Is(err, ErrInput) {
				t.Fatal("invalid dependency selection accepted", err)
			}
			if entries, err := os.ReadDir(directory); err != nil || len(entries) != 0 {
				t.Fatal("invalid bindings reached file construction", err)
			}
		})
	}
	if _, err := PrepareV1(OptionsV1{Name: "live", Sinks: []SinkV1{{Name: "writer", Writer: io.Discard}}}); !errors.Is(err, ErrInput) {
		t.Fatal("inert preparation accepted a live handle", err)
	}
	if _, err := (Prepared{}).Select(BindingsV1{}); err == nil || (Prepared{}).PhysicalEquivalent(prepared) || (Prepared{}).Metadata() != (Metadata{}) {
		t.Fatal("zero preparation constructed defaults")
	}
}

func TestLegacySelectPreservesRemovedAndOriginalBindingRules(t *testing.T) {
	var kept bytes.Buffer
	removed := &preparedWriter{}
	directory := privateDir(t)
	options := OptionsV1{Name: "legacy", Sinks: []SinkV1{{Name: "keep", Writer: &kept}, {Name: "drop", Writer: removed}, {Name: "file", File: &FileOptionsV1{Directory: directory}}}}
	fixture := bindFixture(t, options, 2, resource.Layer{Kind: resource.Local, Content: []byte("sinks: [{name: keep, kind: writer, min_level: trace}]\n")})
	if result := emit(t, fixture, "legacy", Info, "retained"); result.Err() != nil || len(result.Outcome.Value.SinksCopy()) != 1 || kept.Len() == 0 || removed.Len() != 0 {
		t.Fatal("legacy removed-binding behavior changed", result.Err())
	}
	if entries, err := os.ReadDir(directory); err != nil || len(entries) != 0 {
		t.Fatal("removed file target was constructed", err)
	}
	encoded, err := json.Marshal(map[string]any{"sinks": []any{map[string]any{"name": "keep", "kind": "file", "min_level": "trace", "file": map[string]any{"directory": directory, "max_bytes": 32768, "backups": 1, "compress": false}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Select(options, resource.Layer{Kind: resource.Local, Content: encoded}); !errors.Is(err, resource.ErrConfiguration) || !errors.Is(err, ErrInput) {
		t.Fatal("legacy binding refusal lost its configuration/underlying identity", err)
	}
	drain(t, fixture.inbox)
}

func TestPolicyViewsShareLevelsAndAllNativeAllowances(t *testing.T) {
	for _, queued := range []int{0, 1} {
		t.Run(map[int]string{0: "no-waiter", 1: "one-waiter"}[queued], func(t *testing.T) {
			blocked := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
			options := fileOptions(t, false, 3)
			options.QueuedCalls, options.MinLevel = queued, Error
			options.Sinks = append(options.Sinks, SinkV1{Name: "blocked", Writer: blocked, MinLevel: Error})
			fixture := bindFixture(t, options, 8)
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(blocked.release) }) })
			updated := fixture.logger.owner.prepared.Options()
			updated.MinLevel, updated.Sinks[1].MinLevel = Trace, Trace
			prepared, err := PrepareV1(updated)
			if err != nil {
				t.Fatal(err)
			}
			view, err := fixture.logger.WithPolicy(prepared)
			if err != nil || view.owner != fixture.logger.owner || view.access != fixture.logger.access || view.inbox != fixture.logger.inbox || view.observer != fixture.logger.observer {
				t.Fatal("policy minted a native owner or allowance", err)
			}
			physical := fixture.logger.PolicyDescription()
			if !view.Enabled(Trace) || fixture.logger.Enabled(Trace) || view.PolicyDescription().Revision == physical.Revision || view.access.Info().Configuration.Revision != physical.Revision {
				t.Fatal("logical and physical policies conflated")
			}
			type completion struct {
				receipt *invocation.Receipt[Result]
				err     error
			}
			first := make(chan completion, 1)
			go func() {
				receipt, err := fixture.logger.Log(context.Background(), correlation("held"), Error, "held")
				first <- completion{receipt, err}
			}()
			<-blocked.entered
			var pending chan completion
			if queued == 1 {
				pending = make(chan completion, 1)
				go func() {
					receipt, err := view.Rotate(context.Background(), correlation("queued-rotate"))
					pending <- completion{receipt, err}
				}()
				deadline := time.Now().Add(3 * time.Second)
				for fixture.assembly.Snapshot().Sources[0].Usage.Queued != 1 {
					if time.Now().After(deadline) {
						t.Fatal("physical queue not reached")
					}
					time.Sleep(time.Millisecond)
				}
			}
			if receipt, err := view.Log(context.Background(), correlation("refused"), Trace, "refused"); receipt != nil || !errors.Is(err, resource.ErrCapacity) {
				t.Fatal("new policy acquired another write allowance", err)
			}
			if receipt, err := fixture.logger.Sync(context.Background(), correlation("refused-sync")); receipt != nil || !errors.Is(err, resource.ErrCapacity) {
				t.Fatal("old policy acquired another maintenance allowance", err)
			}
			if queued == 0 {
				if receipt, err := view.Rotate(context.Background(), correlation("refused-rotate")); receipt != nil || !errors.Is(err, resource.ErrCapacity) {
					t.Fatal("rotation bypassed source allowance", err)
				}
			}
			releaseOnce.Do(func() { close(blocked.release) })
			result := <-first
			if value := observed(t, result.receipt, result.err); value.Err() != nil {
				t.Fatal(value.Err())
			}
			if pending != nil {
				result = <-pending
				if value := observed(t, result.receipt, result.err); value.Err() != nil || !value.Outcome.Value.SinksCopy()[0].Rotated {
					t.Fatal("shared queued Rotate did not resume", value.Err())
				}
			}
			receipt, err := view.Log(context.Background(), correlation("new-trace"), Trace, "new-trace")
			if value := observed(t, receipt, err); value.Err() != nil || !value.Outcome.Value.SinksCopy()[0].Accepted || !value.Outcome.Value.SinksCopy()[1].Accepted {
				t.Fatal("lowered view did not preserve both target policies", value.Err())
			}
			if value := emit(t, fixture, "old-trace", Trace, "old-trace"); value.Err() != nil || !value.Outcome.Value.SinksCopy()[0].Filtered || !value.Outcome.Value.SinksCopy()[1].Filtered {
				t.Fatal("old source policy changed")
			}
			drain(t, fixture.inbox)
		})
	}
}

func TestPolicyEquivalenceAndWorstSeverityWidth(t *testing.T) {
	options := OptionsV1{Name: "equivalent", MinLevel: Info, MaxRecordBytes: 1024, Timeout: time.Second, Sinks: []SinkV1{
		{Name: "file", Kind: "file", MinLevel: Info, File: &FileOptionsV1{Directory: privateDir(t), MaxBytes: 4096, Backups: 3}},
		{Name: "other", Kind: "writer", MinLevel: Warn},
	}}
	first, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	changed := first.Options()
	changed.MinLevel, changed.Sinks[0].MinLevel, changed.Sinks[1].MinLevel = Panic, Debug, Fatal
	second, err := PrepareV1(changed)
	if err != nil || !first.PhysicalEquivalent(second) || first.Metadata() != second.Metadata() {
		t.Fatal("admitted severity changes outgrew original policy reservation", err)
	}
	for name, change := range map[string]func(*OptionsV1){
		"name": func(value *OptionsV1) { value.Name = "changed" }, "record": func(value *OptionsV1) { value.MaxRecordBytes = 2048 },
		"queue": func(value *OptionsV1) { value.QueuedCalls = 1 }, "timeout": func(value *OptionsV1) { value.Timeout += time.Second },
		"caller": func(value *OptionsV1) { value.Caller = true }, "sink-name": func(value *OptionsV1) { value.Sinks[1].Name = "changed" },
		"kind":       func(value *OptionsV1) { value.Sinks[1].Kind = "managed-record" },
		"directory":  func(value *OptionsV1) { value.Sinks[0].File.Directory = privateDir(t) },
		"file-bytes": func(value *OptionsV1) { value.Sinks[0].File.MaxBytes = 8192 },
		"backups":    func(value *OptionsV1) { value.Sinks[0].File.Backups = 4 },
		"gzip":       func(value *OptionsV1) { value.Sinks[0].File.Compress = true },
		"order":      func(value *OptionsV1) { value.Sinks[0], value.Sinks[1] = value.Sinks[1], value.Sinks[0] },
		"count":      func(value *OptionsV1) { value.Sinks = value.Sinks[:1] },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := first.Options()
			change(&candidate)
			prepared, err := PrepareV1(candidate)
			if err != nil {
				t.Fatal("non-level control was not independently valid", err)
			}
			if first.PhysicalEquivalent(prepared) || prepared.PhysicalEquivalent(first) {
				t.Fatal("non-level change admitted as physical equivalence")
			}
		})
	}
}

func TestPolicyBorrowKeepsLockNameUntilActualRelease(t *testing.T) {
	options := fileOptions(t, false, 2)
	fixture := bindFixture(t, options, 4)
	alias := resource.Borrow("alias", fixture.assembly, fixture.selected)
	borrower, err := resource.Assemble(context.Background(), context.Background(), "borrower", alias)
	if err != nil {
		t.Fatal(err)
	}
	defer borrower.Close(context.Background())
	logger, err := Bind(borrower, alias, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	updated := fixture.logger.owner.prepared.Options()
	updated.MinLevel = Trace
	prepared, err := PrepareV1(updated)
	if err != nil {
		t.Fatal(err)
	}
	view, err := logger.WithPolicy(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("owner discarded live borrowed policy", err)
	}
	marker := filepath.Join(options.Sinks[0].File.Directory, lockName)
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("borrowed view lost original ownership marker", err)
	}
	selection, err := prepared.Select(BindingsV1{})
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, prepared.Metadata().Limits)
	failed, err := resource.Assemble(context.Background(), context.Background(), "competing", selection)
	if err == nil || failed == nil || failed.Snapshot().Sources[0].Pending {
		t.Fatal("competing owner acquired or leaked the original marker", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("failed candidate removed another owner's marker", err)
	}
	receipt, err := view.Log(context.Background(), correlation("borrowed"), Trace, "still-owned")
	if value := observed(t, receipt, err); value.Err() != nil || !value.Outcome.Value.SinksCopy()[0].Accepted {
		t.Fatal("borrowed policy lost native authority", value.Err())
	}
	if err := borrower.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("clean physical release retained lock name", err)
	}
	reopened, err := resource.Assemble(context.Background(), context.Background(), "reopened", selection)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	drain(t, fixture.inbox)
}

func TestPolicyViewsShareCumulativeDerivedBudget(t *testing.T) {
	fixture := bindFixture(t, OptionsV1{Name: "views", Sinks: []SinkV1{{Name: "out", Writer: io.Discard}}}, 1)
	prepared, err := PrepareV1(fixture.logger.owner.prepared.Options())
	if err != nil {
		t.Fatal(err)
	}
	for range MaxDerivedViews + 1 {
		if _, err := fixture.logger.WithPolicy(prepared); err != nil {
			t.Fatal("logical updates consumed cumulative attribute quota", err)
		}
	}
	view, err := fixture.logger.WithPolicy(prepared)
	if err != nil {
		t.Fatal(err)
	}
	for index := range MaxDerivedViews {
		logger := fixture.logger
		if index%2 == 1 {
			logger = view
		}
		if _, err := logger.With(slog.Int("value", index)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := view.With(slog.Bool("overflow", true)); !errors.Is(err, ErrLimit) {
		t.Fatal("policy minted a fresh attribute-derivation allowance", err)
	}
}
