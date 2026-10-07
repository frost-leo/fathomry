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

package duckdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	sdk "github.com/duckdb/duckdb-go/v2"
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

func TestErrorTranslation(t *testing.T) {
	nativeCause := &sdk.Error{Type: sdk.ErrorTypeConstraint, Msg: "private-canary"}
	for _, entry := range []struct {
		kind fault.Kind
		code failure.Code
	}{
		{native.ErrInput, ErrInput}, {native.ErrUnsupported, ErrUnsupported},
		{native.ErrNative, ErrNative}, {native.ErrLimit, ErrLimit},
		{native.ErrState, ErrState}, {native.ErrCleanup, ErrCleanup},
		{source.ErrCapacity, ErrLimit}, {invocation.ErrEvidence, ErrLimit},
		{invocation.ErrInvalid, ErrInput}, {invocation.ErrState, ErrState},
	} {
		original := entry.kind.New(fault.Context{}, nativeCause)
		translated := translate(invocation.ErrFailed.New(fault.Context{}, original), "operation")
		core, ok := failure.Inspect(translated)
		if !ok || core.Diagnostic().Definition.Code != entry.code || !errors.Is(translated, original) {
			t.Fatal("native identity changed", entry.kind)
		}
		var found *sdk.Error
		if !errors.As(translated, &found) || found != nativeCause {
			t.Fatal("native typed cause was replaced")
		}
	}
	input := native.ErrInput.New(fault.Context{}, nativeCause)
	operation := native.ErrNative.New(fault.Context{}, input)
	cleanup := native.ErrCleanup.New(fault.Context{}, operation)
	for _, entry := range []struct {
		err  error
		code failure.Code
	}{
		{operation, ErrNative}, {cleanup, ErrCleanup},
		{invocation.ErrCleanup.New(fault.Context{}, operation), ErrCleanup},
		{source.ErrCleanup.New(fault.Context{}, operation), ErrCleanup},
		{source.ErrAssembly.New(fault.Context{}, operation, cleanup), ErrNative},
		{fmt.Errorf("wrapper: %w", cleanup), ErrCleanup},
		{errors.Join(cleanup, operation), ErrCleanup},
		{source.ErrAdmission.New(fault.Context{}, source.ErrCapacity), ErrLimit},
		{native.ErrCleanup.New(fault.Context{}, source.ErrCapacity), ErrCleanup},
		{source.ErrConfiguration.New(fault.Context{}, native.ErrUnsupported), ErrUnsupported},
	} {
		translated := translate(entry.err, "operation")
		core, ok := failure.Inspect(translated)
		if !ok || core.Diagnostic().Definition.Code != entry.code {
			t.Fatalf("nearest classification replaced: got %v, want %v", translated, entry.code)
		}
		if !errors.Is(translated, entry.err) {
			t.Fatal("cause graph detached")
		}
		if errors.Is(entry.err, source.ErrCapacity) && !errors.Is(translated, ErrLimit) {
			t.Fatal("outer classification erased nested capacity")
		}
	}
	bounded := native.ErrNative.New(fault.Context{}, native.ErrLimit.New(fault.Context{}, nativeCause))
	translated := translate(invocation.ErrFailed.New(fault.Context{}, bounded), "query")
	core, ok := failure.Inspect(translated)
	if !ok || core.Diagnostic().Definition.Code != ErrNative || !errors.Is(translated, ErrLimit) || !errors.Is(core.Unwrap()[0], bounded) {
		t.Fatal("nested identities replaced the original primary/cause graph")
	}
	repeated := translate(errors.Join(bounded, bounded, bounded), "query")
	if core, ok := failure.Inspect(repeated); !ok || len(core.Unwrap()) != 2 {
		t.Fatal("nested semantic identities were not deduplicated")
	}
	cycle := new(diagnosticCycle)
	if translated := translate(cycle, "query"); translated == nil || cycle.visits != 128 {
		t.Fatal("classification traversal is not bounded")
	}
	direct := fail(ErrNative, "query", nativeCause)
	if translate(direct, "operation") != direct || translate(nil, "operation") != nil {
		t.Fatal("existing semantic occurrence changed")
	}
	for _, value := range []error{direct, translated, cleanup} {
		if text := fmt.Sprintf("%v %+v %#v", value, value, value); strings.Contains(text, "private-canary") {
			t.Fatal("implicit native error disclosure")
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("private-cancellation-cause")
	cancel(cause)
	canceled := translate(native.ErrNative.New(fault.Context{}, ctx.Err(), context.Cause(ctx)), "query")
	if !errors.Is(canceled, context.Canceled) || !errors.Is(canceled, cause) || !errors.Is(canceled, ErrNative) {
		t.Fatal("cancellation lost its classification or cause")
	}
}

func TestConfigurationIdentityFailureRemainsInputAcrossEntryPoints(t *testing.T) {
	for _, operation := range []string{"validate", "recommend", "open"} {
		invalid := source.ErrConfiguration.New(fault.Context{}, errors.New("invalid-private-name"))
		translated := translate(invalid, operation)
		if !errors.Is(translated, ErrInput) || !errors.Is(translated, invalid) || errors.Is(translated, ErrNative) {
			t.Fatalf("%s changed a pure configuration failure into a native operation failure", operation)
		}
	}
	if _, err := Recommend(Settings{Name: "Invalid"}); !errors.Is(err, ErrInput) {
		t.Fatal("invalid source identity lost its input classification", err)
	}
}

type diagnosticCycle struct{ visits int }

func (*diagnosticCycle) Error() string { return "cycle" }

func (value *diagnosticCycle) Unwrap() error {
	value.visits++
	return value
}

func TestDiagnostics(t *testing.T) {
	values := []any{
		Settings{Name: "private-canary", Path: "/private-canary/database"},
		Request{SQL: "private-canary", Args: []any{"private-canary"}},
		Column{Name: "private-canary", Type: "private-canary"},
		Step{Rows: [][]any{{"private-canary"}}},
		Progress{Steps: []Step{{Rows: [][]any{{"private-canary"}}}}},
		Decimal{Width: 10, Scale: 2, Value: big.NewInt(123456789)},
		UUID{1, 2, 3, 4}, Interval{Days: 123456789},
		BuildInfo{SDKs: []ModuleInfo{{Path: sqlengine.Fact{Value: "private-canary"}}}},
		ModuleInfo{Path: sqlengine.Fact{Value: "private-canary"}},
	}
	for _, value := range values {
		for _, format := range []string{"%v", "%+v", "%#v", "%q", "%s", "%d"} {
			text := fmt.Sprintf(format, value)
			if strings.Contains(text, "private-canary") || strings.Contains(text, "123456789") {
				t.Fatal("implicit runtime value disclosure")
			}
		}
		for _, jsonOutput := range []bool{false, true} {
			var output bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&output, nil)
			if jsonOutput {
				handler = slog.NewJSONHandler(&output, nil)
			}
			slog.New(handler).Info("value", "value", value)
			if text := output.String(); strings.Contains(text, "private-canary") || strings.Contains(text, "123456789") || strings.Contains(text, "LogValue panicked") {
				t.Fatal("implicit structured disclosure or invalid log value")
			}
		}
	}
	runtime := []any{new(Owner), new(Handle), new(Client), new(Result), new(Request), new(Column), new(Step), new(Progress), new(Reader), new(ReadProgress), new(BuildInfo), new(ModuleInfo), new(Decimal), new(UUID), new(Interval)}
	for _, value := range runtime {
		if _, err := json.Marshal(value); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime serialized", err)
		}
		if err := json.Unmarshal([]byte("{}"), value); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime reconstructed", err)
		}
	}
	nils := []any{(*Owner)(nil), (*Handle)(nil), (*Client)(nil), (*Result)(nil), (*Request)(nil), (*Column)(nil), (*Step)(nil), (*Progress)(nil), (*Reader)(nil), (*ReadProgress)(nil), (*BuildInfo)(nil), (*ModuleInfo)(nil)}
	for _, value := range nils {
		if got := slog.AnyValue(value).Resolve(); got.Kind() != slog.KindString || got.String() != "duckdb[restricted]" {
			t.Fatal("nil runtime emitted panic diagnostic")
		}
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "PANIC") {
			t.Fatal("nil runtime format panic")
		}
	}
}

func TestSettingsLogRedaction(t *testing.T) {
	sensitive := Settings{Name: "name-canary", Path: "/path-canary/database"}
	var normalizedAbsent any
	for _, value := range []any{sensitive, &sensitive, Settings{}, new(Settings), normalizedAbsent} {
		for _, jsonOutput := range []bool{false, true} {
			var output bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&output, nil)
			if jsonOutput {
				handler = slog.NewJSONHandler(&output, nil)
			}
			slog.New(handler).Info("settings", "settings", value)
			if text := output.String(); strings.Contains(text, "canary") || strings.Contains(text, "LogValue panicked") {
				t.Fatal("settings logging leaked a value or emitted a panic diagnostic")
			}
			if jsonOutput {
				var record map[string]any
				if err := json.Unmarshal(output.Bytes(), &record); err != nil {
					t.Fatal(err)
				}
				actual, present := record["settings"]
				if !present || value == nil && actual != nil || value != nil && actual != "duckdb[restricted]" {
					t.Fatal("JSON logging changed redaction or absence")
				}
			}
		}
	}
	encoded, err := json.Marshal(sensitive)
	if err != nil || !strings.Contains(string(encoded), "path-canary") {
		t.Fatal("explicit configuration serialization was refused")
	}
}

func TestCatalog(t *testing.T) {
	definitions := Definitions()
	catalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "database_duckdb", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: definitions})
	if err != nil {
		t.Fatal(err)
	}
	for index, definition := range definitions {
		if definition.Code != failure.Code(0xA0820001+index) || definition.Code.Domain() != failure.DomainDatabase || definition.Component != "database_duckdb" {
			t.Fatal("stable database identity changed")
		}
		stored, found, err := catalog.Lookup(definition.Code)
		if err != nil || !found || stored != definition {
			t.Fatal("offline identity mismatch")
		}
		english, found, err := messages.Explain(definition.Code, "en")
		if err != nil || !found || english.Message.Text != definition.Message {
			t.Fatal("baseline definition mismatch")
		}
		chinese, found, err := messages.Explain(definition.Code, "zh-CN")
		if err != nil || !found || chinese.Message.Locale != "zh-CN" || chinese.Message.Text == english.Message.Text {
			t.Fatal("localized identity missing")
		}
	}
	coverage, err := messages.Coverage("zh-CN")
	if err != nil || len(coverage) != 1 || len(coverage[0].Missing) != 0 {
		t.Fatal("incomplete translation coverage")
	}
	definitions[0].Message = "changed"
	if Definitions()[0].Message == "changed" {
		t.Fatal("definition slice aliases")
	}
}

func TestResultErrorPhases(t *testing.T) {
	primaryCause, cleanupCause := errors.New("primary-canary"), errors.New("cleanup-canary")
	primary, cleanup := fail(ErrNative, "query", primaryCause), fail(ErrCleanup, "cleanup", cleanupCause)
	combined := combineResultErrors("query", primary, cleanup)
	core, ok := failure.Inspect(combined)
	if !ok || core.Diagnostic().Definition.Code != ErrNative || !errors.Is(combined, primaryCause) || !errors.Is(combined, cleanupCause) {
		t.Fatal("phase aggregation lost native identity or causes")
	}
	causes := core.Unwrap()
	if len(causes) != 2 || causes[0] != primary || causes[1] != cleanup {
		t.Fatal("phase aggregation replaced original occurrences")
	}
	if combineResultErrors("query", primary, nil) != primary || combineResultErrors("query", nil, cleanup) != cleanup || combineResultErrors("query", nil, nil) != nil {
		t.Fatal("sole occurrence changed")
	}
	var waitDefinition failure.Definition
	for _, definition := range adapters.Definitions() {
		if definition.Code == adapters.ErrWait {
			waitDefinition = definition
		}
	}
	details := adapters.Details{Sequence: 17, Pending: true}
	wait, err := failure.NewDetailed(waitDefinition, failure.Location{Operation: "wait"}, details, func(value adapters.Details) adapters.Details { return value }, context.DeadlineExceeded)
	if err != nil {
		t.Fatal(err)
	}
	combined = combineResultErrors("query", wait, cleanup)
	typed, ok := combined.(*failure.Detailed[adapters.Details])
	if !ok || !errors.Is(combined, adapters.ErrWait) || !errors.Is(combined, context.DeadlineExceeded) || !errors.Is(combined, cleanupCause) {
		t.Fatal("phase aggregation lost shared wait detail contract")
	}
	if got, present := typed.Details(); !present || got != details {
		t.Fatal("phase aggregation changed pending responsibility")
	}
}
