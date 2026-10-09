//go:build linux

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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.uber.org/zap/zapcore"
)

func TestPreparedFileBudgetsBeforeEffects(t *testing.T) {
	options := fileOptions(t, true)
	options.QueuedCalls = 2
	prepared, err := PrepareV1(options, false, resource.Layer{Kind: resource.Local, Content: []byte("queued_calls: 0")})
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	maximum, backups := options.Outputs[0].MaxFileBytes, int64(options.Outputs[0].MaxBackups)
	compressed := maximum + maximum/100 + 64<<10
	if metadata.Files != 1 || metadata.Outputs != 1 || metadata.Structured || metadata.FileBytes != backups*compressed+maximum ||
		metadata.MaintenanceFileBytes != (backups+1)*compressed+maximum || metadata.Limits.Queued != 0 {
		t.Fatal("frozen file or queue envelope missing")
	}
	if entries, err := os.ReadDir(options.Outputs[0].Directory); err != nil || len(entries) != 0 {
		t.Fatal("inert preparation opened a file", err)
	}
	copy := prepared.Options()
	copy.Outputs[0].Directory = "changed"
	copy.Outputs[0].Level = "debug"
	if prepared.Options().Outputs[0].Directory != options.Outputs[0].Directory || prepared.Options().Outputs[0].Level != "info" {
		t.Fatal("effective output snapshot is aliased")
	}
	selected, err := prepared.Select(nil)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, metadata.Limits)
	assembly, err := resourceAssembly(selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	inbox, err := newResultInbox()
	if err != nil {
		t.Fatal(err)
	}
	logger, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal("exact final file policy did not bind", err)
	}
	if logger.owner.prepared.state != prepared.state || logger.owner.settings.QueuedCalls != 0 || logger.owner.branches[0].file == nil {
		t.Fatal("construction did not use exact preparation")
	}
}

func TestPolicyEquivalenceRejectsEveryNonLevelChange(t *testing.T) {
	options := OptionsV1{Name: "physical", Timeout: time.Second, MaxEntryBytes: 1024, Outputs: []OutputV1{
		{Name: "first", Kind: "file", Directory: privateDirectory(t), MaxFileBytes: 4096, MaxBackups: 3},
		{Name: "second", Kind: "file", Directory: privateDirectory(t), MaxFileBytes: 4096, MaxBackups: 3},
	}}
	prepared, err := PrepareV1(options, true)
	if err != nil {
		t.Fatal(err)
	}
	fixture := bindFixture(t, options, &recordingSink{}, 4)
	tests := map[string]func(*OptionsV1){
		"name":        func(value *OptionsV1) { value.Name = "different" },
		"queue":       func(value *OptionsV1) { value.QueuedCalls = 1 },
		"timeout":     func(value *OptionsV1) { value.Timeout += time.Second },
		"entry":       func(value *OptionsV1) { value.MaxEntryBytes = 2048 },
		"caller":      func(value *OptionsV1) { value.Caller = true },
		"output-name": func(value *OptionsV1) { value.Outputs[0].Name = "different" },
		"output-kind": func(value *OptionsV1) { value.Outputs[0] = OutputV1{Name: "first", Kind: "stderr"} },
		"directory":   func(value *OptionsV1) { value.Outputs[0].Directory = privateDirectory(t) },
		"file-bytes":  func(value *OptionsV1) { value.Outputs[0].MaxFileBytes = 8192 },
		"backups":     func(value *OptionsV1) { value.Outputs[0].MaxBackups = 4 },
		"gzip":        func(value *OptionsV1) { value.Outputs[0].Compress = true },
		"order":       func(value *OptionsV1) { value.Outputs[0], value.Outputs[1] = value.Outputs[1], value.Outputs[0] },
		"count":       func(value *OptionsV1) { value.Outputs = value.Outputs[:1] },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			changed := prepared.Options()
			change(&changed)
			candidate, err := PrepareV1(changed, true)
			if err != nil {
				t.Fatal("test did not reach valid changed profile", err)
			}
			if prepared.PhysicalEquivalent(candidate) || candidate.PhysicalEquivalent(prepared) {
				t.Fatal("non-level change presented as physical equivalence")
			}
			if view, err := fixture.logger.WithPolicy(candidate); view != nil || !errors.Is(err, ErrUnsupported) {
				t.Fatal("non-level view adopted", err)
			}
		})
	}
	withoutSink, err := PrepareV1(prepared.Options(), false)
	if err != nil || prepared.PhysicalEquivalent(withoutSink) {
		t.Fatal("structured dependency ownership change ignored", err)
	}
	for _, output := range options.Outputs {
		data, err := os.ReadFile(filepath.Join(output.Directory, "current.log"))
		if err != nil || len(data) != 0 {
			t.Fatal("failed policy candidate wrote to physical output", err)
		}
	}
}

func TestPolicyViewsLowerNativeThresholdWithoutReplacingOwner(t *testing.T) {
	options := fileOptions(t, false)
	options.Outputs[0].Level, options.ExtensionLevel = "error", "error"
	sink := &recordingSink{}
	fixture := bindFixture(t, options, sink, 8)
	physical := fixture.logger.PolicyDescription()
	options.Outputs[0].Level, options.ExtensionLevel = "debug", "warn"
	prepared, err := PrepareV1(options, true)
	if err != nil {
		t.Fatal(err)
	}
	view, err := fixture.logger.WithPolicy(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if view.owner != fixture.logger.owner || view.access != fixture.logger.access ||
		view.owner.branches[0].Core.Enabled(zapcore.InfoLevel) || !view.Enabled(zapcore.InfoLevel) || fixture.logger.Enabled(zapcore.InfoLevel) {
		t.Fatal("native core was replaced or frozen filtering changed")
	}
	if view.PolicyDescription().Revision == physical.Revision || view.access.Info().Configuration.Revision != physical.Revision ||
		fixture.logger.PolicyDescription().Revision != physical.Revision {
		t.Fatal("physical and logical revision attribution conflated")
	}
	for _, test := range []struct {
		logger *Logger
		name   string
		level  zapcore.Level
		local  SinkState
		remote SinkState
	}{
		{fixture.logger, "old-filter", zapcore.InfoLevel, Filtered, Filtered},
		{view, "new-local", zapcore.InfoLevel, Written, Filtered},
		{fixture.logger, "old-error", zapcore.ErrorLevel, Written, Written},
	} {
		receipt, err := test.logger.Log(context.Background(), fault.Correlation{Call: test.name}, test.level, test.name)
		result := resultOf(t, receipt, err)
		states := result.Outcome.Value.SinksCopy()
		if result.Err() != nil || len(states) != 2 || states[0].State != test.local || states[1].State != test.remote ||
			result.Source.Configuration.Revision != physical.Revision {
			t.Fatal("view changed physical attribution or per-output filtering", result.Err())
		}
	}
	rows := readJSON(t, filepath.Join(options.Outputs[0].Directory, "current.log"))
	if len(rows) != 2 || rows[0]["msg"] != "new-local" || rows[1]["msg"] != "old-error" || sink.count() != 1 {
		t.Fatal("independent JSON output did not preserve frozen old/new levels")
	}
}

func TestPolicyBorrowRetainsPhysicalFileLock(t *testing.T) {
	options := fileOptions(t, false)
	fixture := bindFixture(t, options, nil, 8)
	alias := resource.Borrow("borrowed", fixture.assembly, fixture.selection)
	borrower, err := resource.Assemble(context.Background(), context.Background(), "borrowed", alias)
	if err != nil {
		t.Fatal(err)
	}
	defer borrower.Close(context.Background())
	logger, err := Bind(borrower, alias, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	options.Outputs[0].Level = "debug"
	prepared, err := PrepareV1(options, false)
	if err != nil {
		t.Fatal(err)
	}
	view, err := logger.WithPolicy(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("physical file released before borrowed policy ended", err)
	}
	selection, err := prepared.Select(nil)
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, prepared.Metadata().Limits)
	failed, err := resource.Assemble(context.Background(), context.Background(), "competing", selection)
	if err == nil || failed == nil || failed.Snapshot().Sources[0].Pending {
		t.Fatal("original directory lock lost or failed candidate leaked", err)
	}
	receipt, err := view.Log(context.Background(), fault.Correlation{Call: "borrowed"}, zapcore.DebugLevel, "still-open")
	if result := resultOf(t, receipt, err); result.Err() != nil {
		t.Fatal("retained policy lost native write authority", result.Err())
	}
	if err := borrower.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopened, err := resource.Assemble(context.Background(), context.Background(), "reopened", selection)
	if err != nil {
		t.Fatal("completed physical release retained the lock", err)
	}
	if err := reopened.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyViewRetainsPhysicalFileFailure(t *testing.T) {
	options := fileOptions(t, false)
	fixture := bindFixture(t, options, nil, 4)
	fixture.allowCloseError = true
	file := fixture.logger.owner.branches[0].file
	if err := file.active.Close(); err != nil {
		t.Fatal(err)
	}
	result := logResult(t, fixture.logger, "failed")
	if !errors.Is(result.Err(), os.ErrClosed) || file.failed == nil {
		t.Fatal("physical write failure not retained")
	}
	options.Outputs[0].Level = "debug"
	prepared, err := PrepareV1(options, false)
	if err != nil {
		t.Fatal(err)
	}
	view, err := fixture.logger.WithPolicy(prepared)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := view.Sync(context.Background(), fault.Correlation{Call: "sync"})
	if next := resultOf(t, receipt, err); !errors.Is(next.Err(), os.ErrClosed) || view.owner.branches[0].file != file || !errors.Is(result.Err(), os.ErrClosed) {
		t.Fatal("new policy erased physical failure or historical evidence")
	}
}
