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
	"database/sql/driver"
	"testing"

	bindings "github.com/duckdb/duckdb-go-bindings"
	sdk "github.com/duckdb/duckdb-go/v2"
)

// These controls use only generated in-memory inputs; deliberate native errors
// here contain no caller SQL, private paths or credentials.
func TestNativeProfileSettings(t *testing.T) {
	connector, err := sdk.NewConnector(defaults(OptionsV1{}).dsn(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	native, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	conn := native.(*sdk.Conn)
	for _, query := range []string{"SET enable_external_access=false", "SET lock_configuration=true"} {
		if _, err := conn.ExecContext(context.Background(), query, nil); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := conn.QueryContext(context.Background(), "SELECT current_setting('enable_external_access'),current_setting('autoload_known_extensions'),current_setting('autoinstall_known_extensions'),current_setting('lock_configuration'),current_setting('temp_directory')", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := make([]driver.Value, 5)
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	if values[0] != false || values[1] != false || values[2] != false || values[3] != true || values[4] != "" {
		t.Fatal("native settings mismatch")
	}
}

func TestNativePrepareExpandedAlter(t *testing.T) {
	connector, err := sdk.NewConnector(defaults(OptionsV1{}).dsn(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	native, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	conn := native.(*sdk.Conn)
	if _, err := conn.ExecContext(context.Background(), "CREATE TABLE gh40_alter (id BIGINT)", nil); err != nil {
		t.Fatal(err)
	}
	statement, err := conn.Prepare("ALTER TABLE gh40_alter ADD COLUMN flag BOOLEAN DEFAULT TRUE")
	if err == nil {
		statement.Close()
		t.Fatal("expanded ALTER no longer rejected: requalify native single-statement path")
	}
	if _, err := conn.ExecContext(context.Background(), "ALTER TABLE gh40_alter ADD COLUMN flag BOOLEAN DEFAULT TRUE", nil); err != nil {
		t.Fatal(err)
	}
}

func TestNativeConfigurationOrderControl(t *testing.T) {
	var config bindings.Config
	if bindings.CreateConfig(&config) != bindings.StateSuccess {
		t.Fatal("create config")
	}
	defer bindings.DestroyConfig(&config)
	if bindings.SetConfig(config, "enable_external_access", "false") != bindings.StateSuccess {
		t.Fatal("disable external access")
	}
	if bindings.SetConfig(config, "temp_directory", "") != bindings.StateError {
		t.Fatal("order-dependent rejection changed")
	}
	var control bindings.Config
	if bindings.CreateConfig(&control) != bindings.StateSuccess {
		t.Fatal("create control config")
	}
	defer bindings.DestroyConfig(&control)
	if bindings.SetConfig(control, "temp_directory", "") != bindings.StateSuccess || bindings.SetConfig(control, "enable_external_access", "false") != bindings.StateSuccess {
		t.Fatal("ordered positive control failed")
	}
}

func TestNativeHandleLifecycle(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	_, tracking := bindings.GetAllocationCount(bindings.AllocationCounterDatabase)
	fixture.exec(t, "CREATE TABLE gh40_handles (id BIGINT PRIMARY KEY, value VARCHAR)")
	requireOK(t, fixture.run(t, Request{Mode: Append, Table: "gh40_handles", Rows: [][]any{{1, "first"}, {2, "second"}}}))
	fixture.rows(t, "SELECT * FROM gh40_handles ORDER BY id")
	if result := fixture.run(t, Request{Mode: Append, Table: "gh40_handles", Rows: [][]any{{3, "buffered"}, {1, "duplicate"}}}); result.Err() == nil {
		t.Fatal("failure control succeeded")
	}
	if result := fixture.run(t, Request{Mode: Query, SQL: "SELECT MAP([from_hex('00'),from_hex('01')],[1,2])"}); result.Err() == nil {
		t.Fatal("decoder rejection control succeeded")
	}
	if err := fixture.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if !tracking {
		t.Log("native lifecycle actions ran; allocation accounting NOT checked (requires upstream -tags=debug_bindings)")
		return
	}
	for _, counter := range []string{
		bindings.AllocationCounterDatabase, bindings.AllocationCounterConnection, bindings.AllocationCounterConfig,
		bindings.AllocationCounterAppender, bindings.AllocationCounterDataChunk, bindings.AllocationCounterLogicalType,
		bindings.AllocationCounterExtractedStatements, bindings.AllocationCounterPendingResult,
		bindings.AllocationCounterPreparedStatement, bindings.AllocationCounterResult, bindings.AllocationCounterValue,
		bindings.AllocationCounterClientContext, bindings.AllocationCounterErrorData,
	} {
		if count, _ := bindings.GetAllocationCount(counter); count != 0 {
			t.Fatalf("native handle counter %s=%d", counter, count)
		}
	}
	t.Logf("upstream debug_bindings tracking active=%t; counters are not an RSS allocator", tracking)
}
