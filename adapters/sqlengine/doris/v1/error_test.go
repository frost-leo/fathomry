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

package doris

import (
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
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	sdk "github.com/go-sql-driver/mysql"
)

func TestErrorIdentityClassificationAndCatalog(t *testing.T) {
	remote := &sdk.MySQLError{Number: 1077, Message: "private-canary"}
	for _, entry := range []struct {
		kind fault.Kind
		code failure.Code
	}{
		{native.ErrInput, ErrInput},
		{native.ErrUnsupported, ErrUnsupported},
		{native.ErrLimit, ErrLimit},
		{native.ErrProtocol, ErrProtocol},
		{native.ErrTransport, ErrTransport},
		{native.ErrSQL, ErrSQL},
		{native.ErrLoad, ErrLoad},
		{native.ErrUncertain, ErrUncertain},
		{native.ErrRowQuality, ErrRowQuality},
		{native.ErrDuplicate, ErrDuplicate},
		{native.ErrCleanup, ErrCleanup},
		{native.ErrState, ErrState},
		{invocation.ErrEvidence, adapters.ErrEvidence}, {source.ErrCapacity, resource.ErrLimit},
		{source.ErrConfiguration, resource.ErrSelection}, {source.ErrCleanup, resource.ErrCleanup},
		{invocation.ErrBudget, adapters.ErrRequest},
	} {
		original := entry.kind.New(fault.Context{}, remote)
		value := translate(invocation.ErrFailed.New(fault.Context{}, original), "query")
		core, ok := failure.Inspect(value)
		if !ok || core.Diagnostic().Definition.Code != entry.code || !errors.Is(value, original) {
			t.Fatal("classification", entry.kind, value)
		}
		if found, ok := InspectError(value); !ok || found != remote {
			t.Fatal("native error detached")
		}
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "private-canary") {
			t.Fatal("native error leaked")
		}
	}
	original := native.ErrSQL.New(fault.Context{}, native.ErrLimit, remote)
	value := translate(original, "query")
	if !errors.Is(value, ErrSQL) || !errors.Is(value, ErrLimit) {
		t.Fatal("nested identities erased")
	}
	if core, ok := failure.Inspect(value); !ok || core.Unwrap()[0] != original {
		t.Fatal("original not first cause")
	}
	catalog, err := failure.Prepare(Definitions()...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "database_doris", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range Definitions() {
		if definition.Code.Facility() != 0x084 || definition.Code.Domain() != failure.DomainDatabase {
			t.Fatal("wrong semantic domain")
		}
		got, found, err := catalog.Lookup(definition.Code)
		if err != nil || !found || got != definition {
			t.Fatal("catalog")
		}
		for _, locale := range []string{"en", "zh-CN"} {
			explanation, found, err := messages.Explain(definition.Code, locale)
			if err != nil || !found || explanation.Message.Locale != locale || locale == "en" && explanation.Message.Text != definition.Message {
				t.Fatal("localization")
			}
		}
	}
	coverage, err := messages.Coverage("zh-CN")
	if err != nil || len(coverage) != 1 || len(coverage[0].Missing) != 0 {
		t.Fatal("coverage")
	}
}

func TestRuntimePrivacyAndCapabilitySurface(t *testing.T) {
	peer := newSQLPeer(t, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	value := (reader{t}).checked(owner.Client().Query(testContext(t), "SELECT exact"))
	ack(t, inbox)
	for _, sample := range []any{owner, owner.Handle(), owner.Client(), value, value.RowsCopy()[0], value.ColumnsCopy()[0],
		Batch{Table: "private-canary", Label: "private-canary", JSON: []byte("private-canary")}, LoadEvidence{Label: "private-canary"}, new(Cursor)} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			if strings.Contains(fmt.Sprintf(format, sample), "private-canary") {
				t.Fatal("runtime leak")
			}
		}
		if _, err := json.Marshal(sample); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime serialized", err)
		}
	}
	for _, target := range []any{new(Owner), new(Handle), new(Client), new(Result), new(Row), new(Column), new(Batch), new(LoadEvidence), new(Cursor)} {
		if err := json.Unmarshal([]byte("{}"), target); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime reconstructed")
		}
	}
	for _, value := range []any{(*Owner)(nil), (*Handle)(nil), (*Client)(nil), (*Result)(nil), (*Cursor)(nil)} {
		if got := slog.AnyValue(value).Resolve(); got.Kind() != slog.KindString || got.String() != "doris[restricted]" {
			t.Fatal("nil formatting")
		}
	}
	settings := Settings{Password: "private-canary", RootCAPEM: "private-canary"}
	for _, value := range []any{settings, &settings} {
		if strings.Contains(fmt.Sprintf("%+v", value), "private-canary") || slog.AnyValue(value).Resolve().String() != "doris[restricted]" {
			t.Fatal("settings logging")
		}
	}
	conformance.Facade(t, owner.Client(), "Query", "Exec", "QueryCursor", "StreamLoad", "InspectLabel", "Profile", "WithID", "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
}

func TestCancellationCauseGraphExceedsDirectCauseBudget(t *testing.T) {
	peer := newSQLPeer(t, false)
	owner, _, _ := testOwner(t, peer.options(), 0)
	lifetime, cancel := context.WithCancelCause(testContext(t))
	cursor, receipt, err := owner.Client().QueryCursor(testContext(t), lifetime, "SELECT page-stall")
	if err != nil || cursor == nil {
		t.Fatal("cursor setup", err)
	}
	marker := errors.New("cancellation-marker")
	causes := []error{marker}
	for _, definitions := range [][]failure.Definition{Definitions(), adapters.Definitions(), resource.Definitions()} {
		for _, definition := range definitions {
			causes = append(causes, definition.Code)
		}
	}
	cancel(errors.Join(causes...))
	result, err := receipt.WaitReleased(testContext(t))
	if err != nil || !errors.Is(result.Err(), marker) || !errors.Is(result.Err(), context.Canceled) {
		t.Fatal("cancellation graph discarded", err, result.Err())
	}
	for _, cause := range causes {
		if !errors.Is(result.Err(), cause) {
			t.Fatal("mapped identity lost", cause)
		}
	}
	unknown := failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0xffff
	translated := translate(errors.Join(unknown, marker), "operation")
	if !errors.Is(translated, marker) || !errors.Is(translated, unknown) || errors.Is(translated, failure.ErrDefinition) {
		t.Fatal("unassigned public identity discarded original graph")
	}
}
