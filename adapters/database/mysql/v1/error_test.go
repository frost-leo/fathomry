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

package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/database/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	native "github.com/frost-leo/fathomry/internal/database/mysql/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

func TestErrorTranslation(t *testing.T) {
	remote := &sdk.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "private-canary"}
	for _, entry := range []struct {
		kind fault.Kind
		code failure.Code
	}{
		{native.ErrInput, ErrInput}, {native.ErrUnsupported, ErrUnsupported},
		{native.ErrConnect, ErrConnect}, {native.ErrQuery, ErrQuery}, {native.ErrLimit, ErrLimit},
		{source.ErrCapacity, ErrLimit},
		{native.ErrState, ErrState}, {native.ErrCleanup, ErrCleanup}, {native.ErrProtocol, ErrProtocol},
	} {
		original := entry.kind.New(fault.Context{}, remote)
		translated := translate(invocation.ErrFailed.New(fault.Context{}, original), "operation")
		core, ok := failure.Inspect(translated)
		if !ok || core.Diagnostic().Definition.Code != entry.code || !errors.Is(translated, original) {
			t.Fatal("native identity changed", entry.kind)
		}
		if found, ok := InspectError(translated); !ok || found != remote {
			t.Fatal("server error was replaced")
		}
	}
	input := native.ErrInput.New(fault.Context{}, remote)
	query := native.ErrQuery.New(fault.Context{}, input)
	cleanup := native.ErrCleanup.New(fault.Context{}, query)
	for _, entry := range []struct {
		err  error
		code failure.Code
	}{
		{query, ErrQuery}, {cleanup, ErrCleanup},
		{invocation.ErrCleanup.New(fault.Context{}, query), ErrCleanup},
		{source.ErrCleanup.New(fault.Context{}, query), ErrCleanup},
		{source.ErrAssembly.New(fault.Context{}, native.ErrConnect.New(fault.Context{}, input), cleanup), ErrConnect},
		{fmt.Errorf("wrapper: %w", cleanup), ErrCleanup},
		{errors.Join(cleanup, query), ErrCleanup},
		{source.ErrAdmission.New(fault.Context{}, source.ErrCapacity), ErrLimit},
		{native.ErrCleanup.New(fault.Context{}, source.ErrCapacity), ErrCleanup},
		{source.ErrConfiguration.New(fault.Context{}, native.ErrUnsupported), ErrUnsupported},
	} {
		translated := translate(entry.err, "operation")
		core, ok := failure.Inspect(translated)
		if !ok || core.Diagnostic().Definition.Code != entry.code {
			t.Fatalf("outer classification replaced: got %v, want %v", translated, entry.code)
		}
		if !errors.Is(translated, entry.err) {
			t.Fatal("cause graph detached")
		}
		if errors.Is(entry.err, source.ErrCapacity) && !errors.Is(translated, ErrLimit) {
			t.Fatal("outer classification erased nested source capacity")
		}
	}
	direct := fail(ErrQuery, "query", remote)
	bounded := native.ErrQuery.New(fault.Context{}, native.ErrLimit.New(fault.Context{}, remote))
	translated := translate(invocation.ErrFailed.New(fault.Context{}, bounded), "query")
	core, ok := failure.Inspect(translated)
	if !ok || core.Diagnostic().Definition.Code != ErrQuery || !errors.Is(translated, ErrLimit) {
		t.Fatal("outer query erased nested limit identity")
	}
	if first := core.Unwrap()[0]; !errors.Is(first, bounded) {
		t.Fatal("original native graph is not the first cause")
	}
	if found, ok := InspectError(translated); !ok || found != remote {
		t.Fatal("nested identity mapping replaced native server cause")
	}
	repeated := translate(errors.Join(bounded, bounded, bounded), "query")
	if core, ok := failure.Inspect(repeated); !ok || len(core.Unwrap()) != 2 {
		t.Fatal("nested semantic identities were not deduplicated")
	}
	cycle := new(diagnosticCycle)
	if translated := translate(cycle, "query"); translated == nil || cycle.visits != 128 {
		t.Fatal("classification traversal is not bounded")
	}
	if translate(direct, "operation") != direct || translate(nil, "operation") != nil {
		t.Fatal("existing semantic occurrence changed")
	}
	if _, ok := InspectError(nil); ok {
		t.Fatal("nil became server evidence")
	}
	if text := fmt.Sprintf("%v %+v %#v", direct, direct, direct); strings.Contains(text, "private-canary") {
		t.Fatal("implicit native error disclosure")
	}
}

type diagnosticCycle struct{ visits int }

func (*diagnosticCycle) Error() string { return "cycle" }

func (value *diagnosticCycle) Unwrap() error {
	value.visits++
	return value
}

func TestDiagnostics(t *testing.T) {
	sensitive := []any{
		Settings{Password: "private-canary", Database: "private-canary"},
		Result{source: database.Info{Name: "private-canary"}, attribution: database.Attribution{ID: "private-canary"}},
		Column{Name: "private-canary"},
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
	runtime := []any{new(Owner), new(Handle), new(Client), new(Result), new(Row), new(Column), new(Transaction), new(Statement)}
	for _, value := range runtime {
		if _, err := json.Marshal(value); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime serialized", err)
		}
		if err := json.Unmarshal([]byte("{}"), value); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime reconstructed", err)
		}
	}
	nils := []any{(*Owner)(nil), (*Handle)(nil), (*Client)(nil), (*Result)(nil), (*Row)(nil), (*Column)(nil), (*Transaction)(nil), (*Statement)(nil)}
	for _, value := range nils {
		if got := slog.AnyValue(value).Resolve(); got.Kind() != slog.KindString || got.String() != "mysql[restricted]" {
			t.Fatal("nil runtime emitted panic diagnostic")
		}
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "PANIC") {
			t.Fatal("nil runtime format panic")
		}
	}
}

func TestSettingsLogRedaction(t *testing.T) {
	sensitive := Settings{Database: "database-canary", User: "user-canary", Password: "password-canary", RootCAPEM: "trust-canary"}
	var optional *Settings
	var normalized any
	if optional != nil {
		normalized = optional
	}
	for _, sample := range []struct {
		name   string
		value  any
		absent bool
	}{
		{"value", sensitive, false},
		{"pointer", &sensitive, false},
		{"zero_value", Settings{}, false},
		{"zero_pointer", new(Settings), false},
		{"normalized_absent", normalized, true},
	} {
		for _, jsonOutput := range []bool{false, true} {
			name := "text"
			if jsonOutput {
				name = "json"
			}
			t.Run(sample.name+"/"+name, func(t *testing.T) {
				var output bytes.Buffer
				var handler slog.Handler = slog.NewTextHandler(&output, nil)
				if jsonOutput {
					handler = slog.NewJSONHandler(&output, nil)
				}
				slog.New(handler).Info("settings", "settings", sample.value)
				rendered := output.String()
				if strings.Contains(rendered, "canary") || strings.Contains(rendered, "LogValue panicked") {
					t.Fatal("settings logging leaked a value or emitted a panic diagnostic")
				}
				if jsonOutput {
					var record map[string]any
					if err := json.Unmarshal(output.Bytes(), &record); err != nil {
						t.Fatal(err)
					}
					actual, present := record["settings"]
					if !present || sample.absent && actual != nil || !sample.absent && actual != "mysql[restricted]" {
						t.Fatal("JSON handler changed settings redaction or absence")
					}
				} else {
					expected := "settings=mysql[restricted]"
					if sample.absent {
						expected = "settings=<nil>"
					}
					if !strings.Contains(rendered, expected) {
						t.Fatal("text handler changed settings redaction or absence")
					}
				}
			})
		}
	}
}

func TestCatalog(t *testing.T) {
	definitions := Definitions()
	errorsCatalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "database_mysql", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: definitions})
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range definitions {
		stored, found, err := errorsCatalog.Lookup(definition.Code)
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

func requireLocalizedResultError(t testing.TB, original error, code failure.Code) {
	t.Helper()
	core, ok := failure.Inspect(original)
	if !ok || core.Diagnostic().Definition.Code != code {
		t.Fatal("direct result hid its semantic occurrence")
	}
	catalog, err := i18n.Prepare(
		i18n.Component{Module: "fathomry", Name: "database_mysql", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()},
		i18n.Component{Module: "fathomry", Name: "operation", BaseLocale: "en", Resources: adapters.Resources(), Directory: "resources", Definitions: adapters.Definitions()},
	)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithLocale("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	presented, ok := presenter.Present(original).(*i18n.Presented)
	if !ok || presented.Issue() != nil || presented.Info().Message.Locale != "zh-CN" || !errors.Is(presented, original) || !errors.Is(presented, code) {
		t.Fatal("direct result did not compose its prepared localization")
	}
}

func TestDirectResultErrorPresentation(t *testing.T) {
	peer := newPeer(t, false, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	_, operationErr := owner.Client().Query(context.Background(), "")
	requireLocalizedResultError(t, operationErr, ErrInput)
	delivery := receive(t, inbox)
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := receipt.Snapshot()
	if snapshot.Primary() != operationErr || snapshot.Cleanup() != nil {
		t.Fatal("sole primary occurrence was replaced")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestResultErrorPhasesAndWait(t *testing.T) {
	runtime, err := adapters.New(context.Background(), adapters.Options{MaxActive: 1, MaxWorkBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](adapters.EvidenceOptions{Capacity: 1, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := bind(Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	primaryCause, cleanupCause := errors.New("primary-cause"), errors.New("cleanup-cause")
	primary, cleanup := fail(ErrQuery, "query", primaryCause), fail(ErrCleanup, "cleanup", cleanupCause)
	var guard adapters.Guard
	t.Cleanup(func() {
		_ = guard.Release()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		ack(t, inbox)
	})
	receipt, err := endpoint.Run(context.Background(), request("query", "", 1, 1), func(call *adapters.Call[Result]) {
		var err error
		guard, err = call.Hold()
		if err != nil {
			t.Fatal(err)
		}
		if err := call.Resolve(adapters.Outcome[Result]{Primary: primary, Cleanup: cleanup}); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := receipt.Snapshot()
	outcome := outcomeError(snapshot)
	requireLocalizedResultError(t, outcome, ErrQuery)
	core, _ := failure.Inspect(outcome)
	causes := core.Unwrap()
	if len(causes) != 2 || causes[0] != primary || causes[1] != cleanup || !errors.Is(outcome, primaryCause) || !errors.Is(outcome, cleanupCause) {
		t.Fatal("phase aggregation replaced original causes")
	}
	wait, cancel := context.WithCancelCause(context.Background())
	waitCause := errors.New("wait-cause")
	cancel(waitCause)
	_, err = (&retainedResult{receipt: receipt}).result(wait)
	requireLocalizedResultError(t, err, adapters.ErrWait)
	if !errors.Is(err, waitCause) || !errors.Is(err, primary) || !errors.Is(err, cleanup) {
		t.Fatal("wait aggregation erased independent outcome")
	}
	detailed, ok := err.(*failure.Detailed[adapters.Details])
	if !ok {
		t.Fatal("wait occurrence lost its declared detail contract")
	}
	details, present := detailed.Details()
	if !present || !details.Pending || details.Sequence != snapshot.Info().Sequence {
		t.Fatal("wait occurrence rewrote pending responsibility")
	}
	after, _ := receipt.Snapshot()
	if after.Primary() != primary || after.Cleanup() != cleanup || after.Info().Released {
		t.Fatal("direct error aggregation changed independent phase evidence")
	}
}
