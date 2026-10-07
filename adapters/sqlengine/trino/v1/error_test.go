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

package trino

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
	sdk "github.com/trinodb/trino-go-client/trino"
)

func TestErrorTranslation(t *testing.T) {
	remote := &sdk.ErrTrino{Message: "private-canary", ErrorName: "GENERIC_INTERNAL_ERROR", ErrorType: "INTERNAL_ERROR"}
	queryFailed := &sdk.ErrQueryFailed{StatusCode: 503, Reason: remote}
	for _, entry := range []struct {
		kind fault.Kind
		code failure.Code
	}{
		{native.ErrInput, ErrInput}, {native.ErrUnsupported, ErrUnsupported},
		{native.ErrAuthority, ErrAuthority}, {native.ErrLimit, ErrLimit},
		{native.ErrProtocol, ErrProtocol}, {native.ErrOperation, ErrOperation},
		{native.ErrCleanup, ErrCleanup}, {source.ErrCapacity, ErrLimit},
		{invocation.ErrInvalid, ErrInput}, {invocation.ErrState, ErrState},
		{invocation.ErrEvidence, ErrLimit}, {invocation.ErrAttempts, ErrLimit},
		{invocation.ErrCleanup, ErrCleanup}, {source.ErrIncomplete, ErrCleanup},
	} {
		original := entry.kind.New(fault.Context{}, queryFailed, context.Canceled)
		translated := translate(invocation.ErrFailed.New(fault.Context{}, original), "operation")
		core, ok := failure.Inspect(translated)
		if !ok || core.Diagnostic().Definition.Code != entry.code || !errors.Is(translated, original) || !errors.Is(translated, context.Canceled) {
			t.Fatal("native identity changed", entry.kind)
		}
		var foundRemote *sdk.ErrTrino
		if !errors.As(translated, &foundRemote) || foundRemote != remote {
			t.Fatal("server error was replaced")
		}
		var foundQuery *sdk.ErrQueryFailed
		if !errors.As(translated, &foundQuery) || foundQuery != queryFailed {
			t.Fatal("query failure was replaced")
		}
	}
	input := native.ErrInput.New(fault.Context{}, remote)
	operation := native.ErrOperation.New(fault.Context{}, input)
	cleanup := native.ErrCleanup.New(fault.Context{}, operation)
	for _, entry := range []struct {
		err  error
		code failure.Code
	}{
		{operation, ErrOperation}, {cleanup, ErrCleanup},
		{invocation.ErrCleanup.New(fault.Context{}, operation), ErrCleanup},
		{source.ErrCleanup.New(fault.Context{}, operation), ErrCleanup},
		{source.ErrAssembly.New(fault.Context{}, operation, cleanup), ErrOperation},
		{fmt.Errorf("private-canary: %w", cleanup), ErrCleanup},
		{errors.Join(cleanup, operation), ErrCleanup},
		{source.ErrAdmission.New(fault.Context{}, source.ErrCapacity), ErrLimit},
		{native.ErrCleanup.New(fault.Context{}, source.ErrCapacity), ErrCleanup},
		{source.ErrConfiguration.New(fault.Context{}, native.ErrUnsupported), ErrUnsupported},
		{source.ErrConfiguration.New(fault.Context{}, errors.New("private-canary")), ErrInput},
		{source.ErrAssembly.New(fault.Context{}, source.ErrConfiguration), ErrInput},
		{source.ErrAssembly.New(fault.Context{}, context.DeadlineExceeded), ErrConnect},
		{source.ErrInitialization.New(fault.Context{}, context.Canceled), ErrConnect},
	} {
		translated := translate(entry.err, "operation")
		core, ok := failure.Inspect(translated)
		if !ok || core.Diagnostic().Definition.Code != entry.code {
			t.Fatalf("nearest classification replaced: got %v, want %v", translated, entry.code)
		}
		if !errors.Is(translated, entry.err) {
			t.Fatal("original cause graph detached")
		}
		if errors.Is(entry.err, source.ErrCapacity) && !errors.Is(translated, ErrLimit) {
			t.Fatal("outer classification erased nested capacity")
		}
	}
	bounded := native.ErrOperation.New(fault.Context{}, native.ErrLimit.New(fault.Context{}, queryFailed))
	translated := translate(invocation.ErrFailed.New(fault.Context{}, bounded), "operation")
	core, ok := failure.Inspect(translated)
	if !ok || core.Diagnostic().Definition.Code != ErrOperation || !errors.Is(translated, ErrLimit) || !errors.Is(core.Unwrap()[0], bounded) {
		t.Fatal("classification replaced native graph or nested limit")
	}
	repeated := translate(errors.Join(bounded, bounded, bounded), "operation")
	if core, ok := failure.Inspect(repeated); !ok || len(core.Unwrap()) != 2 {
		t.Fatal("nested semantic identities were not deduplicated")
	}
	cycle := new(diagnosticCycle)
	if translate(cycle, "operation") == nil || cycle.visits != 128 {
		t.Fatal("classification traversal is not bounded")
	}
	direct := fail(ErrOperation, "operation", queryFailed)
	if translate(direct, "close") != direct || translate(nil, "operation") != nil {
		t.Fatal("existing semantic occurrence changed")
	}
	joined := translate(errors.Join(bounded, context.DeadlineExceeded, cleanup), "operation")
	var foundRemote *sdk.ErrTrino
	var foundQuery *sdk.ErrQueryFailed
	if !errors.As(joined, &foundRemote) || foundRemote != remote || !errors.As(joined, &foundQuery) || foundQuery != queryFailed ||
		!errors.Is(joined, context.DeadlineExceeded) || !errors.Is(joined, ErrCleanup) {
		t.Fatal("joined native/cancellation/cleanup identity was erased")
	}
	for _, value := range []error{direct, joined, translate(fmt.Errorf("private-canary: %w", queryFailed), "query")} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "private-canary") ||
			strings.Contains(slog.AnyValue(value).Resolve().String(), "private-canary") {
			t.Fatal("implicit native error disclosure")
		}
	}
}

type diagnosticCycle struct{ visits int }

func (*diagnosticCycle) Error() string { return "cycle" }

func (value *diagnosticCycle) Unwrap() error {
	value.visits++
	return value
}

func TestErrorFallbacks(t *testing.T) {
	for _, entry := range []struct {
		operation string
		code      failure.Code
	}{
		{"query", ErrOperation}, {"read", ErrOperation},
		{"validate", ErrInput}, {"recommend", ErrInput}, {"configuration", ErrInput},
		{"open", ErrConnect}, {"readiness", ErrConnect},
		{"cleanup", ErrCleanup}, {"close", ErrCleanup},
	} {
		original := errors.New("private-canary")
		translated := translate(original, entry.operation)
		core, ok := failure.Inspect(translated)
		if !ok || core.Diagnostic().Definition.Code != entry.code || !errors.Is(translated, original) {
			t.Fatal("fallback classification erased cause", entry.operation)
		}
	}
}

func TestErrorPhaseCombination(t *testing.T) {
	primaryCause, cleanupCause := errors.New("primary-canary"), errors.New("cleanup-canary")
	primary, cleanup := fail(ErrOperation, "query", primaryCause), fail(ErrCleanup, "cleanup", cleanupCause)
	if combineResultErrors("query", primary, nil) != primary || combineResultErrors("query", nil, cleanup) != cleanup ||
		combineResultErrors("query", nil, nil) != nil {
		t.Fatal("sole phase occurrence changed")
	}
	combined := combineResultErrors("read", primary, cleanup)
	core, ok := failure.Inspect(combined)
	if !ok || core.Diagnostic().Definition.Code != ErrOperation || core.Diagnostic().Location.Operation != "read" ||
		len(core.Unwrap()) != 2 || core.Unwrap()[0] != primary || core.Unwrap()[1] != cleanup ||
		!errors.Is(combined, primaryCause) || !errors.Is(combined, cleanupCause) {
		t.Fatal("independent phase identity lost")
	}
	var waitDefinition failure.Definition
	for _, definition := range adapters.Definitions() {
		if definition.Code == adapters.ErrWait {
			waitDefinition = definition
		}
	}
	details := adapters.Details{Sequence: 19, Pending: true}
	wait, err := failure.NewDetailed(waitDefinition, failure.Location{Operation: "wait"}, details, func(value adapters.Details) adapters.Details { return value }, context.Canceled)
	if err != nil {
		t.Fatal(err)
	}
	waited := combineResultErrors("read", wait, combined)
	detailed, ok := waited.(*failure.Detailed[adapters.Details])
	if !ok {
		t.Fatal("typed wait occurrence was replaced")
	}
	retained, present := detailed.Details()
	if !present || retained != details || !errors.Is(waited, adapters.ErrWait) || !errors.Is(waited, context.Canceled) ||
		!errors.Is(waited, primary) || !errors.Is(waited, cleanup) {
		t.Fatal("wait detail or independent phase evidence changed")
	}
}

func TestRuntimeDiagnostics(t *testing.T) {
	sensitive := []any{
		Settings{Password: "private-canary", BearerToken: "private-canary", Endpoint: "private-canary", RootCAPEM: "private-canary"},
		Statement{SQL: "private-canary", Args: []any{"private-canary"}},
		BatchInsert{Table: "private-canary", Columns: []string{"private-canary"}, Rows: [][]any{{"private-canary"}}},
		Column{Name: "private-canary", Type: "private-canary", Signature: []byte("private-canary")},
		Numeric("private-canary"),
	}
	for _, value := range sensitive {
		for _, format := range []string{"%v", "%+v", "%#v", "%q", "%s", "%d"} {
			if strings.Contains(fmt.Sprintf(format, value), "private-canary") {
				t.Fatal("implicit value disclosure")
			}
		}
		if strings.Contains(slog.AnyValue(value).Resolve().String(), "private-canary") {
			t.Fatal("implicit structured disclosure")
		}
	}
	runtime := []any{new(Dependencies), new(Owner), new(Handle), new(Client), new(Result), new(Statement), new(BatchInsert), new(Column), new(Reader), new(ReadProgress), new(BuildInfo), new(ModuleInfo), new(Numeric)}
	for _, value := range runtime {
		if _, err := json.Marshal(value); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime serialized", err)
		}
		if err := json.Unmarshal([]byte("{}"), value); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime reconstructed", err)
		}
	}
	nils := []any{(*Dependencies)(nil), (*Owner)(nil), (*Handle)(nil), (*Client)(nil), (*Result)(nil), (*Statement)(nil), (*BatchInsert)(nil), (*Column)(nil), (*Reader)(nil), (*ReadProgress)(nil), (*BuildInfo)(nil), (*ModuleInfo)(nil)}
	for _, value := range nils {
		if got := slog.AnyValue(value).Resolve(); got.Kind() != slog.KindString || got.String() != "trino[restricted]" {
			t.Fatal("nil runtime emitted a panic diagnostic")
		}
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "PANIC") {
			t.Fatal("nil runtime formatting panicked")
		}
	}
	if string(Numeric("12345678901234567890.123456789")) != "12345678901234567890.123456789" {
		t.Fatal("explicit Numeric inspection is lossy")
	}
}

func TestSettingsDiagnosticAndSerializationBoundary(t *testing.T) {
	sensitive := Settings{Password: "password-canary", BearerToken: "token-canary", Endpoint: "endpoint-canary", RootCAPEM: "trust-canary"}
	encoded, err := json.Marshal(sensitive)
	if err != nil || !bytes.Contains(encoded, []byte("password-canary")) {
		t.Fatal("intentional configuration serialization refused")
	}
	var decoded Settings
	if json.Unmarshal(encoded, &decoded) != nil || decoded.Password != sensitive.Password || decoded.BearerToken != sensitive.BearerToken {
		t.Fatal("configuration reconstruction failed")
	}
	for _, sample := range []any{sensitive, &sensitive, Settings{}, new(Settings), nil} {
		for _, jsonOutput := range []bool{false, true} {
			var output bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&output, nil)
			if jsonOutput {
				handler = slog.NewJSONHandler(&output, nil)
			}
			slog.New(handler).Info("settings", "settings", sample)
			if strings.Contains(output.String(), "canary") || strings.Contains(output.String(), "LogValue panicked") {
				t.Fatal("settings logging leaked or panicked")
			}
			if jsonOutput {
				var record map[string]any
				if json.Unmarshal(output.Bytes(), &record) != nil {
					t.Fatal("malformed structured diagnostic")
				}
				actual, present := record["settings"]
				if !present || sample == nil && actual != nil || sample != nil && actual != "trino[restricted]" {
					t.Fatal("settings redaction changed")
				}
			}
		}
	}
}

func TestCatalog(t *testing.T) {
	definitions := Definitions()
	catalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "database_trino", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: definitions})
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 10 {
		t.Fatal("public error taxonomy changed")
	}
	for index, definition := range definitions {
		if uint32(definition.Code) != uint32(0xA0830001+index) || definition.Code.Domain() != failure.DomainDatabase ||
			definition.Module != "fathomry" || definition.Component != "database_trino" {
			t.Fatal("public allocation changed")
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
		t.Fatal("translation coverage incomplete")
	}
	definitions[0].Message = "changed"
	if Definitions()[0].Message == "changed" {
		t.Fatal("definition slice aliases")
	}
}
