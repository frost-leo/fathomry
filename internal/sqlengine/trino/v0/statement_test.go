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
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	native "github.com/trinodb/trino-go-client/trino"
)

func TestStatementFamiliesAndArgumentBoundaries(t *testing.T) {
	s := defaults(OptionsV1{Writes: true, Maintenance: true})
	for _, sql := range []string{
		"SELECT 1", "-- comment\nWITH batch AS (SELECT 1) SELECT * FROM batch",
		"INSERT INTO t VALUES (?)", "INSERT INTO t SELECT * FROM s", "UPDATE t SET x = 2",
		"DELETE FROM t WHERE x = 1", "MERGE INTO t USING s ON t.x=s.x WHEN MATCHED THEN DELETE",
		"TRUNCATE TABLE t", "CREATE TABLE t AS SELECT * FROM s", "CREATE SCHEMA example",
		"ALTER TABLE t ADD COLUMN x BIGINT", "DROP TABLE t", "COMMENT ON TABLE t IS 'text'",
		"ALTER TABLE t EXECUTE optimize", "CALL catalog.system.expire_snapshots()", "ANALYZE t",
		"SELECT ';' /* ; */; -- tail", "SELECT \"semi;colon\" FROM t",
	} {
		if _, err := prepare(Statement{SQL: sql}, s, false); err != nil {
			t.Fatalf("approved family rejected: %s", sql)
		}
	}
	for _, sql := range []string{"START TRANSACTION", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT x",
		"SET SESSION retry_policy='TASK'", "USE x", "PREPARE x FROM SELECT 1", "EXECUTE x",
		"GRANT ALL ON t TO x", "SELECT 1; INSERT INTO t VALUES (1)", "SELECT 1 /*",
		"SELECT 'open", "SELECT (1", "EXPLAIN ANALYZE DELETE FROM t", "WITH x AS (SELECT 1) DELETE FROM t"} {
		if _, err := prepare(Statement{SQL: sql}, s, true); err == nil {
			t.Fatalf("unsafe query admitted: %s", sql)
		}
	}
	s.Maintenance = false
	if _, err := prepare(Statement{SQL: "ALTER TABLE t EXECUTE remove_orphan_files"}, s, false); !errors.Is(err, ErrAuthority) {
		t.Fatal("maintenance gate bypassed")
	}
	cycle := []any{nil}
	cycle[0] = cycle
	for _, arg := range []any{sql.Named("accessToken", "synthetic-secret"), map[string]string{"x": "y"}, float64(1),
		native.Numeric("NaN"), native.Numeric("0x10"), native.Numeric("1_0"), uint64(1) << 63, cycle, []any(nil)} {
		if _, err := prepare(Statement{SQL: "SELECT ?", Args: []any{arg}}, s, true); err == nil {
			t.Fatal("unsupported argument accepted")
		}
	}
	for _, arg := range []any{nil, true, int64(9223372036854775807), native.Numeric("12345.6700"), "a'b",
		[]byte{0, 255}, []any{int64(1), nil}, time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)} {
		if _, err := prepare(Statement{SQL: "SELECT ?", Args: []any{arg}}, s, true); err != nil {
			t.Fatal(err)
		}
	}
}

func FuzzSingleStatementGate(f *testing.F) {
	for _, seed := range []string{"SELECT 1", "/* /* */ DELETE FROM data -- */ SELECT 1", "EXPLAIN --\rANALYZE DELETE FROM data", "SELECT 'semi;colon'", "WITH batch AS (SELECT 1) SELECT * FROM batch"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		if len(sql) > 4096 {
			t.Skip()
		}
		read, err := prepare(Statement{SQL: sql}, defaults(OptionsV1{MaxSQLBytes: 4096}), true)
		if err == nil && !read {
			t.Fatal("read-only gate admitted a mutation family")
		}
	})
}
func TestNativeMultiRowInsertAndBounds(t *testing.T) {
	var posts atomic.Int64
	_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.Header.Get("X-Trino-Query-Data-Encoding") != "" ||
			r.Header.Get("X-Trino-Prepared-Statement") != "" ||
			!strings.Contains(r.Header.Get("X-Trino-Session"), "retry_policy=NONE") ||
			!strings.Contains(string(body), "\"test_catalog\".\"gh41_run\".\"data\"") ||
			!strings.Contains(string(body), "a''b") || !strings.Contains(string(body), "X'00ff'") {
			t.Error("native immediate submission or isolation changed")
		}
		reply(t, w, map[string]any{"id": "insert", "updateCount": 2})
	})
	f := bindFixture(t, o, 4)
	receipt, err := f.client.Insert(deadline(t), deadline(t), correlation("insert"), BatchInsert{
		Table: "data", Columns: []string{"id", "value", "binary"}, Rows: [][]any{{int64(1), "a'b", []byte{0, 255}}, {int64(2), nil, []byte{}}}})
	value := success(t, settle(t, receipt, err))
	count, known := value.UpdateCount()
	if count != 2 || !known || value.Effect() != Acknowledged || value.Submissions() != 1 || value.Rows() != 0 {
		t.Fatal("aggregate evidence changed")
	}
	for i, batch := range []BatchInsert{
		{Table: "data", Columns: []string{"id"}, Rows: [][]any{}},
		{Table: "data", Columns: []string{"id"}, Rows: [][]any{{1, 2}}},
		{Table: "data; DROP", Columns: []string{"id"}, Rows: [][]any{{1}}},
	} {
		receipt, err := f.client.Insert(deadline(t), deadline(t), correlation("bad-"+string(rune('a'+i))), batch)
		bad := settle(t, receipt, err)
		if bad.Err() == nil || bad.Outcome.Value.Effect() != NotSubmitted {
			t.Fatal("invalid batch submitted")
		}
	}
	if posts.Load() != 1 {
		t.Fatal("batch split, retry or invalid submission")
	}
}
func TestPagedQueryRetainsInitialPageAndExactValues(t *testing.T) {
	var serverURL string
	_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
		columns := []any{column("integer", "bigint"), column("decimal", "decimal", long(38), long(10)),
			column("time", "timestamp with time zone", long(12)), column("binary", "varbinary"),
			column("nested", "array", typed("bigint"))}
		switch r.Method {
		case http.MethodPost:
			reply(t, w, map[string]any{"id": "query", "nextUri": serverURL + "/v1/statement/executing/query/slug/1", "columns": columns,
				"data": [][]any{{json.Number("9223372036854775807"), "1234567890123456789012345678.1234567890", "2026-09-14 01:02:03.123456789012 +08:00", "AP8=", []any{json.Number("9223372036854775807"), nil}}}})
		case http.MethodGet:
			reply(t, w, map[string]any{"id": "query", "columns": columns, "data": [][]any{{nil, nil, nil, nil, []any{}}}})
		default:
			t.Error("unexpected cancellation after terminal query")
		}
	})
	serverURL = o.Endpoint
	f := bindFixture(t, o, 1)
	receipt, err := f.client.Query(deadline(t), deadline(t), correlation("query"), Statement{SQL: "SELECT * FROM data ORDER BY integer"})
	value := success(t, settle(t, receipt, err))
	decoder := json.NewDecoder(strings.NewReader(string(value.DataCopy())))
	decoder.UseNumber()
	var rows [][]any
	if err := decoder.Decode(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][0] != json.Number("9223372036854775807") ||
		rows[0][1] != "1234567890123456789012345678.1234567890" ||
		rows[0][2] != "2026-09-14 01:02:03.123456789012 +08:00" || rows[0][3] != "AP8=" ||
		rows[1][0] != nil || value.Rows() != 2 || value.Pages() != 2 || value.Effect() != ReadOnly {
		t.Fatal("direct result fidelity/completion lost")
	}
	copied := value.DataCopy()
	copied[0] = 0
	columns := value.ColumnsCopy()
	columns[0].Name = "changed"
	columns[0].Signature[0] = 0
	if value.DataCopy()[0] != '[' || value.ColumnsCopy()[0].Name != "integer" || value.ColumnsCopy()[0].Signature[0] != '{' {
		t.Fatal("result aliases mutable copies")
	}
	if _, known := value.UpdateCount(); known {
		t.Fatal("absent update count became zero")
	}
}
func TestSaturatedEvidenceRejectsBeforeHTTP(t *testing.T) {
	var posts atomic.Int64
	_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		reply(t, w, map[string]any{"id": "query"})
	})
	f := bindFixture(t, o, 1)
	receipt, err := f.client.Query(deadline(t), deadline(t), correlation("first"), Statement{SQL: "SELECT 1"})
	success(t, settle(t, receipt, err))
	receipt, err = f.client.Query(deadline(t), deadline(t), correlation("second"), Statement{SQL: "SELECT 1"})
	if err == nil || receipt != nil || posts.Load() != 1 {
		t.Fatal("required evidence capacity bypassed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err = f.client.Query(ctx, deadline(t), correlation("cancelled"), Statement{SQL: "SELECT 1"}); err == nil || receipt != nil || posts.Load() != 1 {
		t.Fatal("pre-cancelled request submitted")
	}
}
