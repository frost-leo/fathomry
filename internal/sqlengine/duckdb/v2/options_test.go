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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOptionsPreparationAndProfile(t *testing.T) {
	for _, options := range []OptionsV1{
		{Name: "gh40", Path: "relative.db"}, {Name: "gh40", Path: "/tmp/value?threads=99"}, {Name: "gh40", Path: "https://invalid.example/gh40"},
		{Name: "gh40", Connections: 9}, {Name: "gh40", Threads: -1}, {Name: "gh40", MemoryBytes: 1},
		{Name: "gh40", Timeout: -1}, {Name: "gh40", ResultBytes: 1}, {Name: "gh40", MaxRows: -1}, {Name: "gh40", QueuedCalls: 65},
	} {
		if _, err := Select(options); err == nil {
			t.Fatal("invalid provider options accepted")
		}
	}
	for _, content := range []string{`{"unknown":true}`, `{"threads":"two"}`, `{"threads":1,"threads":2}`, `{"path":null}`} {
		if _, err := Select(OptionsV1{Name: "gh40"}, resource.Layer{Kind: resource.Local, Content: []byte(content)}); err == nil {
			t.Fatal("invalid overlay accepted")
		}
	}
	fixture := openFixture(t, OptionsV1{})
	profile := fixture.database.Profile()
	if profile.Native.Value != "v1.5.5" || profile.Native.Kind != compatibility.Observed || profile.ServiceVersion.Kind != compatibility.NotApplicable {
		t.Fatal("native version axes changed")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/duckdb/duckdb-go/v2", "github.com/duckdb/duckdb-go-bindings", "github.com/duckdb/duckdb-go-bindings/lib/linux-amd64", "github.com/apache/arrow-go/v18"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, fixture.database.access, profile, []compatibility.Requirement{{Guarantee: "native-batch", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Require(compatibility.Policy{}) == nil {
		t.Fatal("absent evidence certified compatibility")
	}
	if !strings.Contains(ProviderID, "duckdb") {
		t.Fatal("provider identity")
	}
}

func TestInputAndEvidenceRejectBeforeNativeWork(t *testing.T) {
	fixture := openFixture(t, OptionsV1{InputBytes: 1024})
	fixture.exec(t, "CREATE TABLE gh40_rows (id BIGINT)")
	for _, request := range []Request{
		{Mode: Mode(99)}, {Mode: Execute, SQL: ""}, {Mode: Query, SQL: "SELECT 1", Table: "unexpected"},
		{Mode: Append, Table: "gh40_rows", Columns: []string{"id", "id"}},
		{Mode: Append, Table: "gh40_rows", Rows: [][]any{{strings.Repeat("x", 1025)}}},
		{Mode: Execute, SQL: "INSERT INTO gh40_rows VALUES (?)", Args: []any{map[string]any{}}},
	} {
		receipt, err := fixture.database.Run(deadline(t), deadline(t), fixture.id(), request)
		if err == nil || receipt != nil || fixture.inbox.Usage().Outstanding != 0 {
			t.Fatal("invalid input accepted or evidence capacity leaked")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, contexts := range [][2]context.Context{{ctx, deadline(t)}, {deadline(t), ctx}} {
		receipt, err := fixture.database.Run(contexts[0], contexts[1], fixture.id(), Request{Mode: Execute, SQL: "INSERT INTO gh40_rows VALUES (1)"})
		if err == nil || receipt != nil {
			t.Fatal("pre-canceled request entered SDK")
		}
	}
	tiny, _ := invocation.NewInbox[Result](1, 1)
	db, err := Bind(fixture.assembly, fixture.selection, tiny, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := db.Run(deadline(t), deadline(t), fixture.id(), Request{Mode: Execute, SQL: "INSERT INTO gh40_rows VALUES (1)"})
	if !errors.Is(err, invocation.ErrEvidence) || receipt != nil {
		t.Fatal("evidence bytes did not reject")
	}
	if rows := fixture.rows(t, "SELECT count(*) FROM gh40_rows"); rows[0][0] != int64(0) {
		t.Fatal("rejected input had effects")
	}
}

func TestAdmissionCancellation(t *testing.T) {
	fixture := openFixture(t, OptionsV1{QueuedCalls: 1})
	lease, err := fixture.database.access.Acquire(deadline(t), defaults(OptionsV1{}).reservation())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	receipt, err := fixture.database.Run(ctx, deadline(t), fault.Correlation{Call: "gh40-wait"}, Request{Mode: Query, SQL: "SELECT 1"})
	if !errors.Is(err, context.DeadlineExceeded) || receipt != nil {
		t.Fatal("admission wait not canceled")
	}
	if usage := fixture.inbox.Usage(); usage.Outstanding != 0 || usage.ReservedBytes != 0 {
		t.Fatal("canceled admission retained evidence reservation")
	}
}
