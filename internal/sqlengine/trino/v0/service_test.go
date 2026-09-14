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
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/google/uuid"
	native "github.com/trinodb/trino-go-client/trino"
	"go.yaml.in/yaml/v3"
)

type serviceConfig struct {
	Trino struct {
		Host     string   `yaml:"direct_host"`
		Port     int      `yaml:"direct_port"`
		Auth     string   `yaml:"auth"`
		User     string   `yaml:"username_hint"`
		Catalogs []string `yaml:"catalogs"`
		Schema   string   `yaml:"iceberg_default_schema"`
	} `yaml:"trino"`
	Iceberg struct {
		Catalog string `yaml:"trino_catalog"`
	} `yaml:"iceberg"`
}

func serviceOptions(t *testing.T) OptionsV1 {
	t.Helper()
	path := os.Getenv("FATHOMRY_TRINO_SERVICE_CONFIG")
	if path == "" {
		t.Skip("requires an explicitly supplied isolated Trino service configuration")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("service configuration could not be opened")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() > 1<<20 {
		t.Fatal("service configuration exceeds the fixture bound")
	}
	var config serviceConfig
	if yaml.NewDecoder(io.LimitReader(file, 1<<20)).Decode(&config) != nil {
		t.Fatal("service configuration could not be decoded")
	}
	if config.Trino.Auth != "none" || config.Trino.Port < 1 || config.Trino.Port > 65535 ||
		!identifier(config.Iceberg.Catalog) || !identifier(config.Trino.Schema) {
		t.Fatal("unqualified service fixture profile")
	}
	found := false
	for _, catalog := range config.Trino.Catalogs {
		found = found || catalog == config.Iceberg.Catalog
	}
	if !found {
		t.Fatal("selected Iceberg catalog is not declared")
	}
	return OptionsV1{Name: "trino-service", Endpoint: "http://" + net.JoinHostPort(config.Trino.Host, strconv.Itoa(config.Trino.Port)),
		User: config.Trino.User, Plaintext: true, Catalog: config.Iceberg.Catalog, Schema: config.Trino.Schema,
		MaxActive: 1, Timeout: 2 * time.Minute, CleanupTimeout: 10 * time.Second, MaxPageBytes: 2 << 20, MaxResultBytes: 16 << 20}
}
func serviceContext(t testing.TB, limit time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	t.Cleanup(cancel)
	return ctx
}
func bindService(t *testing.T, o OptionsV1) fixture {
	t.Helper()
	selected, err := Select(o)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(o))
	assembly, err := resource.Assemble(serviceContext(t, 2*time.Minute), serviceContext(t, 10*time.Second), "gh41-service", selected)
	if assembly != nil {
		t.Cleanup(func() {
			if err := assembly.Close(serviceContext(t, 10*time.Second)); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](1, defaults(o).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if inbox.Usage().Outstanding != 0 {
			t.Error("service evidence was not explicitly received")
		}
	})
	return fixture{client: client, assembly: assembly, selected: selected, inbox: inbox, options: o}
}
func serviceReceipt(t testing.TB, f fixture, receipt *invocation.Receipt[Result], err error) (invocation.Result[Result], error) {
	t.Helper()
	if err != nil {
		return invocation.Result[Result]{}, err
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || !result.Released {
		return result, errors.New("service fixture retained local work")
	}
	delivery, err := f.inbox.Next(serviceContext(t, 10*time.Second))
	if err != nil {
		return result, err
	}
	if delivery.Receipt() != nil {
		delivered, _ := delivery.Receipt().Result()
		if delivered.Context.Correlation != result.Context.Correlation {
			return result, errors.New("service evidence attribution changed")
		}
	}
	return result, delivery.Release()
}
func serviceInvoke(t testing.TB, f fixture, step string, query bool, statement Statement) (invocation.Result[Result], error) {
	t.Helper()
	work := serviceContext(t, 2*time.Minute)
	cleanup := serviceContext(t, 3*time.Minute)
	var receipt *invocation.Receipt[Result]
	var err error
	if query {
		receipt, err = f.client.Query(work, cleanup, correlation(step), statement)
	} else {
		receipt, err = f.client.Execute(work, cleanup, correlation(step), statement)
	}
	return serviceReceipt(t, f, receipt, err)
}
func requireService(t testing.TB, f fixture, step string, query bool, statement Statement) Result {
	t.Helper()
	result, err := serviceInvoke(t, f, step, query, statement)
	if err != nil {
		t.Fatal(step, err)
	}
	if result.Err() != nil {
		var cause *native.ErrTrino
		if errors.As(result.Err(), &cause) && queryID(cause.ErrorName) {
			t.Logf("%s native error=%s code=%d", step, cause.ErrorName, cause.ErrorCode)
		}
		t.Fatal(step, result.Err())
	}
	return success(t, result)
}
func serviceRows(t testing.TB, value Result) [][]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(value.DataCopy()))
	decoder.UseNumber()
	var rows [][]any
	if err := decoder.Decode(&rows); err != nil {
		t.Fatal("invalid service result data")
	}
	return rows
}
func requireServiceProfile(t testing.TB, f fixture) {
	t.Helper()
	if f.client.Profile().ServiceVersion.Value != "482" {
		t.Fatal("service revision differs from the qualified 482 fixture")
	}
	value := requireService(t, f, "connector-profile", true, Statement{
		SQL: "SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = ?", Args: []any{f.options.Catalog}})
	rows := serviceRows(t, value)
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "iceberg" {
		t.Fatal("selected catalog is not the Iceberg connector")
	}
	requireService(t, f, "schema-profile", true, Statement{SQL: "SHOW SCHEMAS FROM " + quote(f.options.Catalog)})
	t.Log("observed Trino 482; Iceberg connector; direct JSON; explicitly configured unauthenticated HTTP")
}
func TestTrinoServiceProfile(t *testing.T) {
	f := bindService(t, serviceOptions(t))
	requireServiceProfile(t, f)
}

func TestTrinoDirectService(t *testing.T) {
	f := bindService(t, serviceOptions(t))
	if f.client.Profile().ServiceVersion.Value != "482" {
		t.Fatal("unqualified coordinator revision")
	}
	typed := requireService(t, f, "direct-types", true, Statement{
		SQL:  "SELECT CAST(? AS BIGINT), CAST(? AS DECIMAL(38,10)), CAST(? AS TIMESTAMP(12) WITH TIME ZONE), CAST(? AS VARCHAR), CAST(? AS VARBINARY), CAST(? AS ARRAY(BIGINT)), CAST(ROW('label', BIGINT '9223372036854775807') AS ROW(label VARCHAR, counter BIGINT)), MAP(ARRAY['key'], ARRAY[BIGINT '9223372036854775807'])",
		Args: []any{int64(9223372036854775807), "1234567890123456789012345678.1234567890", "2026-09-14 01:02:03.123456789012 +08:00", nil, []byte{0, 255}, []any{int64(9223372036854775807), nil}},
	})
	rows := serviceRows(t, typed)
	if len(rows) != 1 || len(rows[0]) != 8 {
		t.Fatal("direct type result arity mismatch")
	}
	row := rows[0]
	if row[0] != json.Number("9223372036854775807") || row[1] != "1234567890123456789012345678.1234567890" ||
		row[2] != "2026-09-14 01:02:03.123456789012 +08:00" || row[3] != nil || row[4] != "AP8=" {
		t.Fatal("direct scalar fidelity mismatch")
	}
	array, arrayOK := row[5].([]any)
	record, recordOK := row[6].([]any)
	mapping, mapOK := row[7].(map[string]any)
	if !arrayOK || len(array) != 2 || array[0] != row[0] || array[1] != nil || !recordOK || len(record) != 2 ||
		record[0] != "label" || record[1] != row[0] || !mapOK || len(mapping) != 1 || mapping["key"] != row[0] {
		t.Fatal("direct nested fidelity mismatch")
	}
	paged := requireService(t, f, "direct-pages", true, Statement{
		SQL: "SELECT id, rpad('page', 2048, 'page') FROM UNNEST(sequence(BIGINT '1', BIGINT '2056')) AS t(id) ORDER BY id",
	})
	pages := serviceRows(t, paged)
	if len(pages) != 2056 || len(paged.DataCopy()) <= f.options.MaxPageBytes || paged.Pages() < 3 {
		t.Fatal("direct multi-page consumption not established")
	}
	for i, row := range pages {
		if len(row) != 2 || row[0] != json.Number(strconv.Itoa(i+1)) || row[1] != strings.Repeat("page", 512) {
			t.Fatal("direct ordered page values changed")
		}
	}
	rejected, err := serviceInvoke(t, f, "direct-error", true, Statement{SQL: "SELECT CAST('not-an-integer' AS BIGINT)"})
	var cause *native.ErrTrino
	if err != nil || !errors.As(rejected.Err(), &cause) || cause.ErrorName != "INVALID_CAST_ARGUMENT" ||
		!rejected.Outcome.Value.Terminal() || rejected.Outcome.Value.Succeeded() || rejected.Outcome.Value.Submissions() != 1 {
		t.Fatal("direct terminal native error evidence changed")
	}
	t.Logf("Trino 482 direct scalar/nested types and %d exact ordered rows across %d protocol pages passed; no table/catalog writes", len(pages), paged.Pages())
}

func TestTrinoIcebergV2Service(t *testing.T) {
	o := serviceOptions(t)
	if os.Getenv("FATHOMRY_TRINO_SERVICE_WRITES") != "1" {
		t.Skip("requires isolated gh41 namespace/table write and cleanup authorization")
	}
	o.Writes = true
	base := bindService(t, o)
	requireServiceProfile(t, base)
	namespace := "gh41_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	qualified := quote(o.Catalog) + "." + quote(namespace)
	preexisting := requireService(t, base, "namespace-preflight", true, Statement{
		SQL: "SELECT schema_name FROM " + quote(o.Catalog) + ".information_schema.schemata WHERE schema_name = ?", Args: []any{namespace}})
	if preexisting.Rows() != 0 {
		t.Fatal("generated namespace already exists")
	}
	t.Logf("test-owned namespace: %s", namespace)
	t.Cleanup(func() {
		for _, table := range []string{"copied", "batch"} {
			result, err := serviceInvoke(t, base, "cleanup-"+table, false, Statement{SQL: "DROP TABLE IF EXISTS " + qualified + "." + quote(table)})
			if err != nil || result.Err() != nil {
				t.Error("test-owned table cleanup failed", err, result.Err())
			}
		}
		tables, err := serviceInvoke(t, base, "cleanup-table-check", true, Statement{
			SQL: "SELECT table_name FROM " + quote(o.Catalog) + ".information_schema.tables WHERE table_schema = ?", Args: []any{namespace}})
		if err != nil || tables.Err() != nil || tables.Outcome.Value.Rows() != 0 {
			t.Error("namespace table absence not confirmed")
			return
		}
		result, err := serviceInvoke(t, base, "cleanup-namespace", false, Statement{SQL: "DROP SCHEMA IF EXISTS " + qualified})
		if err != nil || result.Err() != nil {
			t.Error("test-owned namespace cleanup failed", err, result.Err())
			return
		}
		check, err := serviceInvoke(t, base, "cleanup-namespace-check", true, Statement{
			SQL: "SELECT schema_name FROM " + quote(o.Catalog) + ".information_schema.schemata WHERE schema_name = ?", Args: []any{namespace}})
		if err != nil || check.Err() != nil || check.Outcome.Value.Rows() != 0 {
			t.Error("namespace absence not confirmed")
			return
		}
		t.Log("independent SQL checks confirmed test-owned table and namespace absence; no physical object-purge claim")
	})
	requireService(t, base, "create-namespace", false, Statement{SQL: "CREATE SCHEMA " + qualified})
	o.Schema = namespace
	f := bindService(t, o)
	table := qualified + "." + quote("batch")
	copyTable := qualified + "." + quote("copied")
	requireService(t, f, "create-table", false, Statement{SQL: "CREATE TABLE " + table +
		" (id BIGINT, amount DECIMAL(38,10), created TIMESTAMP(6) WITH TIME ZONE, note VARCHAR, payload VARBINARY, nested ARRAY(BIGINT)) WITH (format = 'PARQUET', format_version = 2)"})
	show := requireService(t, f, "verify-format", true, Statement{SQL: "SHOW CREATE TABLE " + table})
	if !bytes.Contains(show.DataCopy(), []byte("format_version = 2")) {
		t.Fatal("Iceberg format version 2 was not observed")
	}
	const count = 257
	when := time.Date(2026, 9, 14, 1, 2, 3, 123456000, time.UTC)
	batch := BatchInsert{Table: "batch", Columns: []string{"id", "amount", "created", "note", "payload", "nested"}}
	for i := 0; i < count; i++ {
		var note any = fmt.Sprintf("row-%d", i)
		if i%7 == 0 {
			note = nil
		}
		batch.Rows = append(batch.Rows, []any{int64(i), native.Numeric("1234567890123456789012345678.1234567890"), when, note, []byte{0, 255}, []any{int64(i), nil}})
	}
	receipt, err := f.client.Insert(serviceContext(t, 2*time.Minute), serviceContext(t, 3*time.Minute), correlation("batch-insert"), batch)
	written, err := serviceReceipt(t, f, receipt, err)
	if err != nil {
		t.Fatal(err)
	}
	value := success(t, written)
	if affected, known := value.UpdateCount(); !known || affected != count || value.Effect() != Acknowledged {
		t.Fatal("batch acknowledgement/count mismatch")
	}
	read := requireService(t, f, "exact-read", true, Statement{SQL: "SELECT id, amount, created, note, payload, nested FROM " + table + " ORDER BY id"})
	rows := serviceRows(t, read)
	if len(rows) != count {
		t.Fatal("batch read count mismatch")
	}
	for i, row := range rows {
		var note any = fmt.Sprintf("row-%d", i)
		if i%7 == 0 {
			note = nil
		}
		if len(row) != 6 || row[0] != json.Number(strconv.Itoa(i)) || row[1] != "1234567890123456789012345678.1234567890" ||
			row[2] != "2026-09-14 01:02:03.123456 UTC" || row[3] != note || row[4] != "AP8=" {
			t.Fatal("scalar service round trip mismatch")
		}
		nested, ok := row[5].([]any)
		if !ok || len(nested) != 2 || nested[0] != json.Number(strconv.Itoa(i)) || nested[1] != nil {
			t.Fatal("nested service round trip mismatch")
		}
	}
	snapshots := requireService(t, f, "snapshot-read", true, Statement{SQL: "SELECT snapshot_id FROM " + qualified + "." + quote("batch$snapshots") + " ORDER BY committed_at"})
	snapshotRows := serviceRows(t, snapshots)
	if len(snapshotRows) == 0 {
		t.Fatal("snapshot metadata absent")
	}
	snapshot, ok := snapshotRows[len(snapshotRows)-1][0].(json.Number)
	if !ok {
		t.Fatal("snapshot ID lost integer fidelity")
	}
	paged := requireService(t, f, "multi-page-read", true, Statement{SQL: "SELECT id, rpad('page', 2048, 'page') FROM " + table + " CROSS JOIN UNNEST(sequence(1, 8)) AS u(n) ORDER BY id, n"})
	if paged.Rows() != count*8 || len(paged.DataCopy()) <= o.MaxPageBytes || paged.Pages() < 3 {
		t.Fatal("real multi-page result was not established")
	}
	t.Logf("finite query retained %d rows over %d protocol pages", paged.Rows(), paged.Pages())
	requireService(t, f, "update", false, Statement{SQL: "UPDATE " + table + " SET note = 'updated' WHERE id % 2 = 0"})
	requireService(t, f, "delete", false, Statement{SQL: "DELETE FROM " + table + " WHERE id % 3 = 0"})
	requireService(t, f, "merge", false, Statement{SQL: "MERGE INTO " + table + " t USING (VALUES (BIGINT '1', 'merged'), (BIGINT '1001', 'new')) AS s(id,note) ON t.id = s.id WHEN MATCHED THEN UPDATE SET note = s.note WHEN NOT MATCHED THEN INSERT (id,note) VALUES (s.id,s.note)"})
	requireService(t, f, "schema-add", false, Statement{SQL: "ALTER TABLE " + table + " ADD COLUMN extra BIGINT"})
	requireService(t, f, "schema-rename", false, Statement{SQL: "ALTER TABLE " + table + " RENAME COLUMN extra TO renamed"})
	requireService(t, f, "table-comment", false, Statement{SQL: "COMMENT ON TABLE " + table + " IS 'Issue 41 isolated fixture'"})
	columns := requireService(t, f, "evolution-columns", true, Statement{SQL: "SELECT column_name, data_type FROM " + quote(o.Catalog) + ".information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position", Args: []any{namespace, "batch"}})
	expectedColumns := []string{"id", "amount", "created", "note", "payload", "nested", "renamed"}
	columnRows := serviceRows(t, columns)
	if len(columnRows) != len(expectedColumns) {
		t.Fatal("evolved column count mismatch")
	}
	for i, column := range columnRows {
		if len(column) != 2 || column[0] != expectedColumns[i] || i == len(expectedColumns)-1 && column[1] != "bigint" {
			t.Fatal("column addition/rename/type was not observed")
		}
	}
	backfill := requireService(t, f, "evolution-backfill", true, Statement{SQL: "SELECT count(*) FROM " + table + " WHERE renamed IS NOT NULL"})
	if serviceRows(t, backfill)[0][0] != json.Number("0") {
		t.Fatal("evolved nullable column backfill changed")
	}
	comment := requireService(t, f, "evolution-comment", true, Statement{SQL: "SHOW CREATE TABLE " + table})
	commentRows := serviceRows(t, comment)
	if len(commentRows) != 1 || len(commentRows[0]) != 1 {
		t.Fatal("table comment metadata absent")
	}
	ddl, ok := commentRows[0][0].(string)
	if !ok || !strings.Contains(ddl, "COMMENT 'Issue 41 isolated fixture'") {
		t.Fatal("table comment was not observed")
	}
	requireService(t, f, "ctas", false, Statement{SQL: "CREATE TABLE " + copyTable + " WITH (format = 'PARQUET', format_version = 2) AS SELECT id, note, CAST(ROW(note, id) AS ROW(label VARCHAR, counter BIGINT)) AS record, MAP(ARRAY['key'], ARRAY[id]) AS mapping FROM " + table})
	mutated := requireService(t, f, "mutation-readback", true, Statement{SQL: "SELECT id, note, record, mapping FROM " + copyTable + " ORDER BY id"})
	expected := make(map[int64]any)
	for i := 0; i < count; i++ {
		if i%3 == 0 {
			continue
		}
		var note any = fmt.Sprintf("row-%d", i)
		if i%7 == 0 {
			note = nil
		}
		if i%2 == 0 {
			note = "updated"
		}
		expected[int64(i)] = note
	}
	expected[1] = "merged"
	expected[1001] = "new"
	mutationRows := serviceRows(t, mutated)
	if !matchesMutationRows(mutationRows, expected) {
		t.Fatal("exact ordered mutation/ROW/MAP state mismatch")
	}
	historical := requireService(t, f, "time-travel", true, Statement{SQL: "SELECT count(*) FROM " + table + " FOR VERSION AS OF " + string(snapshot)})
	if serviceRows(t, historical)[0][0] != json.Number(strconv.Itoa(count)) {
		t.Fatal("snapshot read changed after mutations")
	}
	rejected, err := serviceInvoke(t, f, "invalid-write", false, Statement{SQL: "INSERT INTO " + table + " (id) VALUES (CAST('not-an-integer' AS BIGINT))"})
	if err != nil || rejected.Err() == nil || rejected.Outcome.Value.Submissions() != 1 || rejected.Outcome.Value.Effect() != Unknown {
		t.Fatal("failed write ambiguity/retry evidence mismatch")
	}
	unchanged := requireService(t, f, "reconcile-invalid-write", true, Statement{SQL: "SELECT count(*) FROM " + table})
	if serviceRows(t, unchanged)[0][0] != json.Number(strconv.Itoa(len(expected))) {
		t.Fatal("failed-write readback mismatch")
	}
	requireService(t, f, "insert-select", false, Statement{SQL: "INSERT INTO " + table + " (id,note) SELECT id + 2000, note FROM " + copyTable})
	doubled := requireService(t, f, "set-write-readback", true, Statement{SQL: "SELECT count(*) FROM " + table})
	if serviceRows(t, doubled)[0][0] != json.Number(strconv.Itoa(2*len(expected))) {
		t.Fatal("server-side batch state mismatch")
	}
	for step, target := range map[string]string{"truncate-batch": table, "truncate-copy": copyTable} {
		requireService(t, f, step, false, Statement{SQL: "TRUNCATE TABLE " + target})
		empty := requireService(t, f, step+"-readback", true, Statement{SQL: "SELECT count(*) FROM " + target})
		if serviceRows(t, empty)[0][0] != json.Number("0") {
			t.Fatal("truncate was not observed")
		}
	}
	t.Log("format-2 batch INSERT/query, CTAS, INSERT SELECT, UPDATE, DELETE, MERGE, TRUNCATE, evolution and time travel passed")
}

func matchesMutationRows(rows [][]any, expected map[int64]any) bool {
	if len(rows) != len(expected) {
		return false
	}
	seen := make(map[int64]bool, len(rows))
	var previous int64
	for index, row := range rows {
		if len(row) != 4 {
			return false
		}
		number, ok := row[0].(json.Number)
		if !ok {
			return false
		}
		id, err := number.Int64()
		if err != nil || seen[id] || index > 0 && id <= previous {
			return false
		}
		note, exists := expected[id]
		if !exists || row[1] != note {
			return false
		}
		record, recordOK := row[2].([]any)
		mapping, mapOK := row[3].(map[string]any)
		if !recordOK || len(record) != 2 || record[0] != note || record[1] != number ||
			!mapOK || len(mapping) != 1 || mapping["key"] != number {
			return false
		}
		seen[id], previous = true, id
	}
	return true
}

func TestServiceMutationOracleRejectsIncompleteState(t *testing.T) {
	row := func(id string, note any) []any {
		number := json.Number(id)
		return []any{number, note, []any{note, number}, map[string]any{"key": number}}
	}
	want := map[int64]any{1: "merged", 2: nil}
	if !matchesMutationRows([][]any{row("1", "merged"), row("2", nil)}, want) {
		t.Fatal("exact positive control rejected")
	}
	for _, rows := range [][][]any{
		{row("1", "merged"), row("1", "merged")},
		{row("2", nil), row("1", "merged")},
		{row("1", "merged")},
		{row("1", "merged"), row("3", nil)},
		{row("1", "wrong"), row("2", nil)},
		{row("1", "merged"), {json.Number("2"), nil, nil, nil}},
	} {
		if matchesMutationRows(rows, want) {
			t.Fatal("corrupted service state passed the acceptance oracle")
		}
	}
}
