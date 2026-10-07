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

package trino_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestPublicInitialProgressAndExactRows(t *testing.T) {
	columns := []any{wireColumn("integer", "bigint", "bigint"), wireColumn("decimal", "decimal(38,10)", "decimal", longArgument(38), longArgument(10)),
		wireColumn("time", "timestamp(12) with time zone", "timestamp with time zone", longArgument(12)), wireColumn("binary", "varbinary", "varbinary"),
		wireColumn("nested", "array(bigint)", "array", typeArgument("bigint"))}
	var peer *wirePeer
	peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
		switch {
		case request.Method == http.MethodPost:
			writePage(t, writer, map[string]any{"id": "query_canary", "columns": columns, "nextUri": peer.next("query_canary", 1),
				"data": [][]any{{json.Number("9223372036854775807"), "1234567890123456789012345678.1234567890", "2026-10-06 01:02:03.123456789012 +08:00", "AP8=", []any{json.Number("9223372036854775807"), nil}}}})
		case strings.HasSuffix(request.URL.Path, "/1"):
			writePage(t, writer, map[string]any{"id": "query_canary", "nextUri": peer.next("query_canary", 2)})
		case request.Method == http.MethodGet:
			writePage(t, writer, map[string]any{"id": "query_canary", "columns": columns, "data": [][]any{{nil, nil, nil, nil, []any{}}}})
		default:
			t.Error("completed query performed unexpected cleanup")
		}
	})
	owner, inbox, _ := openPublic(t, peer.settings(), 0)
	client, err := owner.Client().WithID("opaque/attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	value, err := client.Query(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT payload_canary"})
	if err != nil || !value.Terminal() || !value.Succeeded() || !value.Complete() || value.Effect() != trino.ReadOnly {
		t.Fatal("valid exact query did not complete", err)
	}
	want := `[[9223372036854775807,"1234567890123456789012345678.1234567890","2026-10-06 01:02:03.123456789012 +08:00","AP8=",[9223372036854775807,null]],[null,null,null,null,[]]]`
	if string(value.DataCopy()) != want || value.Rows() != 2 || value.Pages() != 3 || value.Submissions() != 1 || value.WireBytes() == 0 || value.QueryID() != "query_canary" {
		t.Fatal("initial/progress/final evidence or precise JSON tokens changed")
	}
	if value.Attribution().ID != "opaque/attempt-1" || value.Source().Name != "fixture" || value.Source().Revision == "" || value.Attempts().Observed != 3 || value.Attempts().Exact {
		t.Fatal("source, attribution or observed wire attempts lost")
	}
	if _, known := value.UpdateCount(); known || value.CancellationAttempted() || value.CancellationAcknowledged() {
		t.Fatal("unobserved aggregate or cancellation facts invented")
	}
	data := value.DataCopy()
	data[0] = 'x'
	metadata := value.ColumnsCopy()
	metadata[0].Name = "changed"
	metadata[0].Signature[0] = 'x'
	if string(value.DataCopy()) != want || value.ColumnsCopy()[0].Name != "integer" || value.ColumnsCopy()[0].Signature[0] != '{' {
		t.Fatal("public result aliases detached copies")
	}
	snapshot := acknowledge(t, inbox)
	evidence, present := snapshot.ValueCopy()
	if !present || snapshot.Err() != nil || string(evidence.DataCopy()) != want || evidence.Attribution().ID != value.Attribution().ID || evidence.QueryID() != value.QueryID() {
		t.Fatal("public custody does not contain the returned exact outcome")
	}
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("result", "value", value, "metadata", value.ColumnsCopy()[0])
	for _, item := range []any{value, value.ColumnsCopy()[0], owner, owner.Handle(), client, trino.Statement{SQL: "sql_canary"}, trino.BatchInsert{Table: "table_canary"}} {
		if raw, err := json.Marshal(item); err == nil {
			t.Fatalf("runtime value serialized: %T (%d bytes)", item, len(raw))
		}
		for _, formatted := range []string{fmt.Sprintf("%v", item), fmt.Sprintf("%+v", item), fmt.Sprintf("%#v", item), logged.String()} {
			if strings.Contains(formatted, "canary") || strings.Contains(formatted, "1234567890123456789012345678") || strings.Contains(formatted, peer.server.URL) {
				t.Fatalf("default runtime presentation disclosed payload: %T", item)
			}
		}
	}
}

func TestPublicStatementFamiliesAndSingleBatch(t *testing.T) {
	peer := newWirePeer(t, func(writer http.ResponseWriter, _ *http.Request, _ []byte) {
		writePage(t, writer, map[string]any{"id": "statement", "updateCount": 0})
	})
	owner, inbox, _ := openPublic(t, peer.settings(), 0)
	ctx := boundedContext(t)
	for _, sql := range []string{"SELECT 1", "VALUES (1)", "TABLE fixture", "SHOW TABLES", "DESCRIBE fixture", "EXPLAIN SELECT 1", "WITH batch AS (SELECT 1) SELECT * FROM batch"} {
		value, err := owner.Client().Query(ctx, ctx, trino.Statement{SQL: sql})
		if err != nil || !value.Complete() || value.Rows() != 0 || string(value.DataCopy()) != "[]" || value.Effect() != trino.ReadOnly {
			t.Fatal("admitted read family or successful empty query lost", sql, err)
		}
		acknowledge(t, inbox)
	}
	for _, sql := range []string{"CREATE TABLE fixture AS SELECT 1", "ALTER TABLE fixture ADD COLUMN x BIGINT", "DROP TABLE fixture", "COMMENT ON TABLE fixture IS 'comment'", "INSERT INTO fixture SELECT * FROM other", "UPDATE fixture SET x=1", "DELETE FROM fixture", "MERGE INTO fixture USING other ON fixture.x=other.x WHEN MATCHED THEN DELETE", "TRUNCATE TABLE fixture", "CALL catalog.system.expire_snapshots()", "ANALYZE fixture", "REFRESH MATERIALIZED VIEW fixture", "ALTER TABLE fixture EXECUTE optimize"} {
		before := peer.posts.Load()
		value, err := owner.Client().Execute(ctx, ctx, trino.Statement{SQL: sql})
		count, present := value.UpdateCount()
		if err != nil || !value.Complete() || value.Effect() != trino.Acknowledged || !present || count != 0 || value.Rows() != 0 || peer.posts.Load() != before+1 {
			t.Fatal("supported Execute family/aggregate zero lost or replayed", sql, err)
		}
		acknowledge(t, inbox)
	}
	batch := trino.BatchInsert{Table: "select", Columns: []string{"key", "payload", "binary", "precise"}, Rows: [][]any{
		{int64(1), "a'b", []byte{0, 255}, trino.Numeric("12345678901234567890.123400")}, {int64(2), nil, []byte{}, trino.Numeric("0")}}}
	before := peer.posts.Load()
	value, err := owner.Client().Insert(ctx, ctx, batch)
	if err != nil || !value.Complete() || value.Submissions() != 1 || peer.posts.Load() != before+1 {
		t.Fatal("physical batch split or rejected", err)
	}
	statements := peer.statements()
	wire := statements[len(statements)-1]
	for _, token := range []string{`"test_catalog"."test_schema"."select"`, `"key","payload","binary","precise"`, "a''b", "X'00ff'", "12345678901234567890.123400"} {
		if !strings.Contains(wire, token) {
			t.Fatal("native physical batch encoding lost an exact input")
		}
	}
	acknowledge(t, inbox)
}

type forbiddenValuer struct{ calls *int }

func (value forbiddenValuer) Value() (driver.Value, error) {
	*value.calls++
	return "must-not-run", nil
}

func TestPublicInputBoundariesAndAuthority(t *testing.T) {
	peer := newWirePeer(t, func(writer http.ResponseWriter, _ *http.Request, _ []byte) {
		writePage(t, writer, map[string]any{"id": "input"})
	})
	owner, inbox, _ := openPublic(t, peer.settings(), 32)
	ctx := boundedContext(t)
	calls := 0
	cycle := []any{nil}
	cycle[0] = cycle
	for _, argument := range []any{float64(1), float32(1), uint8(1), uint64(1) << 63, map[string]any{"x": 1}, new(int), forbiddenValuer{&calls}, trino.Numeric("NaN"), trino.Numeric("1_0"), trino.Numeric("0x10"), []any(nil), cycle} {
		before := peer.posts.Load()
		value, err := owner.Client().Query(ctx, ctx, trino.Statement{SQL: "SELECT ?", Args: []any{argument}})
		if err == nil || value.Complete() || value.Submissions() != 0 || value.Effect() != trino.NotSubmitted || peer.posts.Load() != before {
			t.Fatalf("unsupported argument submitted: %T", argument)
		}
	}
	if calls != 0 {
		t.Fatal("native Valuer escape invoked application code")
	}
	for _, argument := range []any{nil, true, int8(-128), int16(-32768), int32(-2147483648), int64(-9223372036854775808), int(1), uint(1), uint16(1), uint32(1), uint64(1), "exact", []byte{0, 255}, []any{int64(1), nil}, []int64{}, trino.Numeric("-123456789.001e+2"), time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)} {
		value, err := owner.Client().Query(ctx, ctx, trino.Statement{SQL: "SELECT ?", Args: []any{argument}})
		if err != nil || !value.Complete() {
			t.Fatalf("admitted parameter rejected: %T: %v", argument, err)
		}
		acknowledge(t, inbox)
	}
	for _, sql := range []string{"START TRANSACTION", "COMMIT", "SET SESSION retry_policy='TASK'", "USE other", "PREPARE x FROM SELECT 1", "SELECT 1; DELETE FROM fixture", "EXPLAIN ANALYZE DELETE FROM fixture", "DELETE FROM fixture"} {
		before := peer.posts.Load()
		value, err := owner.Client().Query(ctx, ctx, trino.Statement{SQL: sql})
		if err == nil || value.Complete() || value.Effect() != trino.NotSubmitted || peer.posts.Load() != before {
			t.Fatal("unsafe read/managed session admitted")
		}
	}
	for _, batch := range []trino.BatchInsert{{Table: "fixture", Columns: []string{"id"}}, {Table: "fixture", Columns: []string{"id"}, Rows: [][]any{{1, 2}}}, {Table: "fixture", Columns: []string{"id", "ID"}, Rows: [][]any{{1, 2}}}, {Table: "fixture; DROP", Columns: []string{"id"}, Rows: [][]any{{1}}}} {
		before := peer.posts.Load()
		value, err := owner.Client().Insert(ctx, ctx, batch)
		if err == nil || value.Submissions() != 0 || peer.posts.Load() != before {
			t.Fatal("invalid batch bypassed rectangular/identifier validation")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := peer.posts.Load()
	_, err := owner.Client().Query(ctx, boundedContext(t), trino.Statement{SQL: "SELECT 1"})
	if !errors.Is(err, context.Canceled) || peer.posts.Load() != before {
		t.Fatal("pre-canceled call lost cancellation or dispatched")
	}
}

func TestPublicEvidenceBackpressureAndFailedSink(t *testing.T) {
	peer := newWirePeer(t, func(writer http.ResponseWriter, _ *http.Request, _ []byte) {
		writePage(t, writer, map[string]any{"id": "custody"})
	})
	owner, inbox, runtime := openPublic(t, peer.settings(), 2)
	ctx := boundedContext(t)
	if _, err := owner.Client().Execute(ctx, ctx, trino.Statement{SQL: "INSERT INTO fixture VALUES (1)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Client().Execute(ctx, ctx, trino.Statement{SQL: "INSERT INTO fixture VALUES (2)"}); !errors.Is(err, adapters.ErrEvidence) || peer.posts.Load() != 1 {
		t.Fatal("full public custody allowed another native mutation", err)
	}
	source, err := inbox.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Ack(); !errors.Is(err, adapters.ErrPending) {
		t.Fatal("live owner evidence acknowledged")
	}
	sinkFailure := errors.New("test sink refusal")
	if err := inbox.DeliverOne(ctx, func(context.Context, adapters.Snapshot[trino.Result]) error { return sinkFailure }); !errors.Is(err, sinkFailure) {
		t.Fatal("failed sink cause was erased", err)
	}
	if state, _ := inbox.Inspect(); state.Outstanding != 2 || peer.posts.Load() != 1 {
		t.Fatal("failed sink discarded evidence or replayed SQL")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := inbox.NextReleased(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled evidence waiter consumed evidence")
	}
	snapshot := acknowledge(t, inbox)
	result, present := snapshot.ValueCopy()
	if !present || snapshot.Err() != nil || result.Effect() != trino.Acknowledged || result.Submissions() != 1 {
		t.Fatal("ignored direct return or failed sink erased mutation facts")
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source cleanup required new evidence admission", err)
	}
	if err := source.Ack(); err != nil {
		t.Fatal(err)
	}
	if state, _ := inbox.Inspect(); state.Outstanding != 0 {
		t.Fatal("evidence acknowledgement left custody behind")
	}
	if state, _ := runtime.Inspect(); state.Active != 0 || state.WorkBytes != 0 {
		t.Fatal("public source/call work remained charged after actual release")
	}
}

func TestConcurrentPublicRootsKeepDistinctCustody(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	defer unblock()
	peer := newWirePeer(t, func(writer http.ResponseWriter, _ *http.Request, body []byte) {
		entered <- struct{}{}
		<-release
		name := strings.TrimPrefix(string(body), "SELECT ")
		writePage(t, writer, map[string]any{"id": name, "columns": []any{wireColumn("value", "varchar", "varchar", longArgument(2147483647))}, "data": [][]string{{name}}})
	})
	owner, inbox, runtime := openPublic(t, peer.settings(), 0)
	type outcome struct {
		value trino.Result
		err   error
	}
	completed := make(chan outcome, 2)
	ctx := boundedContext(t)
	for _, name := range []string{"left", "right"} {
		client, err := owner.Client().WithID(name)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			value, err := client.Query(ctx, ctx, trino.Statement{SQL: "SELECT " + name})
			completed <- outcome{value, err}
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("independent root calls could not overlap")
		}
	}
	if usage, _ := runtime.Inspect(); usage.Active != 3 {
		t.Fatal("source plus two actual root operations not owned")
	}
	if _, err := owner.Client().Query(ctx, ctx, trino.Statement{SQL: "SELECT third"}); !errors.Is(err, adapters.ErrLimit) || peer.posts.Load() != 2 {
		t.Fatal("saturated work admission dispatched another query", err)
	}
	unblock()
	seen := make(map[string]bool)
	for range 2 {
		result := <-completed
		if result.err != nil || !result.value.Complete() || result.value.QueryID() != result.value.Attribution().ID {
			t.Fatal("concurrent returns exchanged native result correspondence", result.err)
		}
		seen[result.value.QueryID()] = true
	}
	if !seen["left"] || !seen["right"] {
		t.Fatal("concurrent result missing")
	}
	for range 2 {
		snapshot := acknowledge(t, inbox)
		value, present := snapshot.ValueCopy()
		if !present || snapshot.Err() != nil || !seen[value.QueryID()] || value.QueryID() != value.Attribution().ID || string(value.DataCopy()) != `[["`+value.QueryID()+`"]]` {
			t.Fatal("concurrent required evidence exchanged native correspondence")
		}
		delete(seen, value.QueryID())
	}
	if len(seen) != 0 {
		t.Fatal("concurrent evidence was duplicated")
	}
}
