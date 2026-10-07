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
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/google/uuid"
	"go.yaml.in/yaml/v3"
)

type publicServiceConfig struct {
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

func publicServiceIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_') {
			return false
		}
	}
	return true
}

func publicServiceSettings(t *testing.T) trino.Settings {
	t.Helper()
	path := os.Getenv("FATHOMRY_TRINO_SERVICE_CONFIG")
	if path == "" || os.Getenv("FATHOMRY_TRINO_PUBLIC_WRITES") != "1" {
		t.Skip("requires explicit isolated Trino configuration and public table-write authorization")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("explicit service configuration unavailable")
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() < 1 || stat.Size() > 1<<20 {
		t.Fatal("service configuration violates fixture bounds")
	}
	var config publicServiceConfig
	decoder := yaml.NewDecoder(io.LimitReader(file, (1<<20)+1))
	if decoder.Decode(&config) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
		t.Fatal("service configuration cannot be decoded")
	}
	if config.Trino.Auth != "none" || config.Trino.Port < 1 || config.Trino.Port > 65535 ||
		!publicServiceIdentifier(config.Iceberg.Catalog) || !publicServiceIdentifier(config.Trino.Schema) {
		t.Fatal("service configuration is outside the explicitly qualified profile")
	}
	declared := false
	for _, catalog := range config.Trino.Catalogs {
		declared = declared || catalog == config.Iceberg.Catalog
	}
	if !declared {
		t.Fatal("configured Iceberg catalog is not explicitly declared")
	}
	value := trino.Settings{Name: "gh110-public-service",
		Endpoint: "http://" + net.JoinHostPort(config.Trino.Host, strconv.Itoa(config.Trino.Port)),
		User:     config.Trino.User, Plaintext: true, Catalog: config.Iceberg.Catalog, Schema: config.Trino.Schema,
		Writes: true, MaxActive: 1, MaxPageBytes: 2 << 20, MaxResultBytes: 16 << 20,
		Timeout: 2 * time.Minute, ReadTimeout: 2 * time.Minute, CleanupTimeout: 10 * time.Second}
	if trino.Validate(value) != nil {
		t.Fatal("service settings rejected by the public contract")
	}
	return value
}

type publicService struct {
	owner   *trino.Owner
	inbox   *adapters.Inbox[trino.Result]
	runtime *adapters.Runtime
	options trino.Settings
}

func openPublicService(t *testing.T, options trino.Settings) *publicService {
	t.Helper()
	policy, err := trino.Recommend(options)
	if err != nil {
		t.Fatal("service reservation preparation failed")
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal("service runtime preparation failed")
	}
	inbox, err := adapters.NewInbox[trino.Result](policy.Evidence)
	if err != nil {
		t.Fatal("service evidence preparation failed")
	}
	lifetime, cancel := context.WithCancel(context.Background())
	owner, openErr := trino.Open(lifetime, options, trino.Dependencies{Runtime: runtime, Evidence: inbox})
	fixture := &publicService{owner: owner, inbox: inbox, runtime: runtime, options: options}
	t.Cleanup(func() {
		defer cancel()
		ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if owner != nil {
			if owner.Close(ctx) != nil || !owner.ShutdownComplete() {
				t.Error("public service source cleanup remains unconfirmed")
			}
		}
		if runtime.Close(ctx) != nil {
			t.Error("public service runtime release remains unconfirmed")
		}
		for status, inspectErr := inbox.Inspect(); inspectErr == nil && status.Outstanding > 0; status, inspectErr = inbox.Inspect() {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error("public service evidence responsibility remains pending")
				return
			}
			if delivery.Ack() != nil {
				t.Error("public service evidence acknowledgement failed")
				return
			}
		}
	})
	if openErr != nil || owner == nil {
		t.Fatal("public service readiness failed")
	}
	return fixture
}

func (fixture *publicService) observe(value trino.Result, callErr error) (trino.Result, error) {
	status, err := fixture.inbox.Inspect()
	if err != nil || status.Outstanding != 2 {
		return value, errors.Join(callErr, errors.New("public service evidence reservation missing"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	delivery, err := fixture.inbox.NextReleased(ctx)
	if err != nil {
		return value, errors.Join(callErr, err)
	}
	receipt, receiptErr := delivery.Receipt()
	if receiptErr != nil {
		return value, errors.Join(callErr, receiptErr, delivery.Ack())
	}
	snapshot, snapshotErr := receipt.WaitReleased(ctx)
	retained, present := snapshot.ValueCopy()
	ackErr := delivery.Ack()
	if snapshotErr != nil || ackErr != nil || !snapshot.Info().Resolved || !snapshot.Info().Released ||
		(snapshot.Err() == nil) != (callErr == nil) || present != value.HasData() ||
		present && (retained.Attribution() != value.Attribution() || retained.Submissions() != value.Submissions() ||
			retained.Effect() != value.Effect() || retained.Complete() != value.Complete() || retained.QueryID() != value.QueryID()) {
		return value, errors.Join(callErr, errors.New("public service evidence custody changed"))
	}
	return value, callErr
}

func (fixture *publicService) invoke(query bool, sql string, args ...any) (trino.Result, error) {
	work, cancelWork := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelWork()
	cleanup, cancelCleanup := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelCleanup()
	statement := trino.Statement{SQL: sql, Args: args}
	if query {
		return fixture.observe(fixture.owner.Client().Query(work, cleanup, statement))
	}
	return fixture.observe(fixture.owner.Client().Execute(work, cleanup, statement))
}

func (fixture *publicService) require(t *testing.T, step string, query bool, sql string, args ...any) trino.Result {
	t.Helper()
	value, err := fixture.invoke(query, sql, args...)
	if err != nil || !value.HasData() || !value.Terminal() || !value.Succeeded() || !value.Complete() || value.Submissions() != 1 {
		t.Fatal(step + ": public service outcome incomplete")
	}
	if query && value.Effect() != trino.ReadOnly || !query && value.Effect() != trino.Acknowledged {
		t.Fatal(step + ": public service effect classification changed")
	}
	return value
}

func publicServiceRows(value trino.Result) ([][]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(value.DataCopy()))
	decoder.UseNumber()
	var rows [][]any
	if err := decoder.Decode(&rows); err != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
		return nil, errors.New("service JSON row representation is invalid")
	}
	return rows, nil
}

func requirePublicServiceRows(t *testing.T, value trino.Result) [][]any {
	t.Helper()
	rows, err := publicServiceRows(value)
	if err != nil || len(rows) != value.Rows() {
		t.Fatal("public service row count or JSON representation changed")
	}
	return rows
}

func publicServiceQuote(value string) string { return "\"" + value + "\"" }

func (fixture *publicService) table(name string) string {
	return publicServiceQuote(fixture.options.Catalog) + "." + publicServiceQuote(fixture.options.Schema) + "." + publicServiceQuote(name)
}

func (fixture *publicService) absence(name string) (bool, error) {
	value, err := fixture.invoke(true, "SELECT table_name FROM "+publicServiceQuote(fixture.options.Catalog)+
		".information_schema.tables WHERE table_schema = ? AND table_name = ?", fixture.options.Schema, name)
	if err != nil || !value.Complete() {
		return false, errors.New("fixture absence observation failed")
	}
	return value.Rows() == 0, nil
}

func serviceNotesMatch(rows [][]any, expected map[int64]any, nested bool) bool {
	if len(rows) != len(expected) {
		return false
	}
	seen := make(map[int64]bool, len(rows))
	var previous int64
	for index, row := range rows {
		arity := 2
		if nested {
			arity = 4
		}
		if len(row) != arity {
			return false
		}
		number, ok := row[0].(json.Number)
		if !ok {
			return false
		}
		id, err := number.Int64()
		note, exists := expected[id]
		if err != nil || !exists || seen[id] || index > 0 && id <= previous || !reflect.DeepEqual(row[1], note) {
			return false
		}
		if nested {
			record, recordOK := row[2].([]any)
			mapping, mapOK := row[3].(map[string]any)
			if !recordOK || len(record) != 2 || !reflect.DeepEqual(record[0], note) || record[1] != number ||
				!mapOK || len(mapping) != 1 || mapping["key"] != number {
				return false
			}
		}
		seen[id], previous = true, id
	}
	return true
}

func (fixture *publicService) requireNotes(t *testing.T, step, table string, expected map[int64]any, nested bool) {
	t.Helper()
	columns := "id, note"
	if nested {
		columns += ", record, mapping"
	}
	rows := requirePublicServiceRows(t, fixture.require(t, step, true, "SELECT "+columns+" FROM "+table+" ORDER BY id"))
	if !serviceNotesMatch(rows, expected, nested) {
		t.Fatal(step + ": independent exact ordered readback differs")
	}
}

// This test is opt-in and mutates only unpredictable table names whose absence
// was independently observed in the explicitly configured existing schema.
func TestTrinoPublicService(t *testing.T) {
	fixture := openPublicService(t, publicServiceSettings(t))
	profile, err := fixture.owner.Client().Profile(context.Background())
	if err != nil || profile.ServiceVersion.Kind != "observed" || profile.ServiceVersion.Value != "482" {
		t.Fatal("service coordinator is outside the qualified version")
	}
	connector := requirePublicServiceRows(t, fixture.require(t, "connector-profile", true,
		"SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = ?", fixture.options.Catalog))
	if !reflect.DeepEqual(connector, [][]any{{"iceberg"}}) {
		t.Fatal("configured catalog is outside the qualified connector")
	}
	schema := fixture.require(t, "schema-profile", true, "SELECT schema_name FROM "+publicServiceQuote(fixture.options.Catalog)+
		".information_schema.schemata WHERE schema_name = ?", fixture.options.Schema)
	if schema.Rows() != 1 {
		t.Fatal("explicitly configured schema is unavailable")
	}

	const decimal = "1234567890123456789012345678.1234567890"
	direct := requirePublicServiceRows(t, fixture.require(t, "direct-types", true,
		"SELECT CAST(? AS BIGINT), CAST(? AS DECIMAL(38,10)), CAST(? AS TIMESTAMP(6) WITH TIME ZONE), CAST(? AS VARBINARY), CAST(? AS ARRAY(BIGINT)), CAST(? AS VARCHAR)",
		int64(9223372036854775807), trino.Numeric(decimal), "2026-10-06 01:02:03.123456 +08:00", []byte{0, 255}, []any{int64(9223372036854775807), nil}, nil))
	wantDirect := [][]any{{json.Number("9223372036854775807"), decimal, "2026-10-06 01:02:03.123456 +08:00", "AP8=", []any{json.Number("9223372036854775807"), nil}, nil}}
	if !reflect.DeepEqual(direct, wantDirect) {
		t.Fatal("direct Numeric, temporal, binary, nested or null fidelity changed")
	}

	name := "gh110_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	copyName := name + "_copy"
	for _, candidate := range []string{name, copyName} {
		absent, err := fixture.absence(candidate)
		if err != nil || !absent {
			t.Fatal("generated fixture table is not independently absent")
		}
	}
	type creationState struct{ attempted, acknowledged bool }
	created := map[string]*creationState{name: {}, copyName: {}}
	t.Logf("test-owned table identifiers: %s, %s", name, copyName)
	t.Cleanup(func() {
		for _, candidate := range []string{copyName, name} {
			state := created[candidate]
			if !state.attempted {
				continue
			}
			value, err := fixture.invoke(false, "DROP TABLE IF EXISTS "+fixture.table(candidate))
			if err != nil || !value.Complete() || value.Effect() != trino.Acknowledged {
				t.Error("isolated public service table DROP is unconfirmed")
				continue
			}
			absent, err := fixture.absence(candidate)
			if err != nil || !absent {
				t.Error("isolated public service table absence is unconfirmed")
			} else {
				t.Logf("independent cleanup confirmed absent: %s", candidate)
			}
			if !state.acknowledged {
				t.Error("creation was unacknowledged; delayed remote creation cannot be excluded")
			}
		}
	})
	table, copied := fixture.table(name), fixture.table(copyName)
	created[name].attempted = true
	fixture.require(t, "create-table", false, "CREATE TABLE "+table+
		" (id BIGINT, amount DECIMAL(38,10), created TIMESTAMP(6) WITH TIME ZONE, note VARCHAR, payload VARBINARY, nested ARRAY(BIGINT)) WITH (format = 'PARQUET', format_version = 2)")
	created[name].acknowledged = true
	format := fixture.require(t, "format-profile", true, "SHOW CREATE TABLE "+table)
	if !bytes.Contains(format.DataCopy(), []byte("format_version = 2")) || !bytes.Contains(format.DataCopy(), []byte("PARQUET")) {
		t.Fatal("explicit table format is not observed")
	}

	const count = 257
	when := time.Date(2026, 10, 6, 1, 2, 3, 123456000, time.UTC)
	batch := trino.BatchInsert{Table: name, Columns: []string{"id", "amount", "created", "note", "payload", "nested"}}
	expected := make(map[int64]any, count+1)
	for index := 0; index < count; index++ {
		var note any = "row-" + strconv.Itoa(index)
		if index%7 == 0 {
			note = nil
		}
		batch.Rows = append(batch.Rows, []any{int64(index), trino.Numeric(decimal), when, note, []byte{0, 255}, []any{int64(index), nil}})
		expected[int64(index)] = note
	}
	work, cancelWork := context.WithTimeout(context.Background(), 2*time.Minute)
	cleanup, cancelCleanup := context.WithTimeout(context.Background(), 3*time.Minute)
	inserted, insertErr := fixture.observe(fixture.owner.Client().Insert(work, cleanup, batch))
	cancelWork()
	cancelCleanup()
	affected, known := inserted.UpdateCount()
	if insertErr != nil || !inserted.Complete() || inserted.Effect() != trino.Acknowledged || inserted.Submissions() != 1 || !known || affected != count {
		t.Fatal("single physical public Insert acknowledgement changed")
	}
	rows := requirePublicServiceRows(t, fixture.require(t, "typed-roundtrip", true, "SELECT id, amount, created, note, payload, nested FROM "+table+" ORDER BY id"))
	if len(rows) != count {
		t.Fatal("physical Insert readback row count changed")
	}
	for index, row := range rows {
		expectedRow := []any{json.Number(strconv.Itoa(index)), decimal, "2026-10-06 01:02:03.123456 UTC", expected[int64(index)], "AP8=", []any{json.Number(strconv.Itoa(index)), nil}}
		if !reflect.DeepEqual(row, expectedRow) {
			t.Fatal("persisted exact scalar/nested roundtrip changed")
		}
	}

	fixture.require(t, "update", false, "UPDATE "+table+" SET note = 'updated' WHERE id % 2 = 0")
	for id := range expected {
		if id%2 == 0 {
			expected[id] = "updated"
		}
	}
	fixture.requireNotes(t, "update-readback", table, expected, false)
	fixture.require(t, "delete", false, "DELETE FROM "+table+" WHERE id % 3 = 0")
	for id := range expected {
		if id%3 == 0 {
			delete(expected, id)
		}
	}
	fixture.requireNotes(t, "delete-readback", table, expected, false)
	fixture.require(t, "merge", false, "MERGE INTO "+table+
		" t USING (VALUES (BIGINT '1', 'merged'), (BIGINT '1001', 'new')) AS s(id,note) ON t.id = s.id WHEN MATCHED THEN UPDATE SET note = s.note WHEN NOT MATCHED THEN INSERT (id,note) VALUES (s.id,s.note)")
	expected[1], expected[1001] = "merged", "new"
	fixture.requireNotes(t, "merge-readback", table, expected, false)

	fixture.require(t, "column-add", false, "ALTER TABLE "+table+" ADD COLUMN extra BIGINT")
	fixture.require(t, "column-rename", false, "ALTER TABLE "+table+" RENAME COLUMN extra TO renamed")
	columns := requirePublicServiceRows(t, fixture.require(t, "evolution-readback", true, "SELECT column_name, data_type FROM "+
		publicServiceQuote(fixture.options.Catalog)+".information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position", fixture.options.Schema, name))
	columnNames := []string{"id", "amount", "created", "note", "payload", "nested", "renamed"}
	if len(columns) != len(columnNames) {
		t.Fatal("evolved schema column count changed")
	}
	for index, column := range columns {
		if len(column) != 2 || column[0] != columnNames[index] || index == len(columns)-1 && column[1] != "bigint" {
			t.Fatal("evolved schema names or added type changed")
		}
	}
	backfill := requirePublicServiceRows(t, fixture.require(t, "nullable-backfill", true, "SELECT count(*) FROM "+table+" WHERE renamed IS NOT NULL"))
	if !reflect.DeepEqual(backfill, [][]any{{json.Number("0")}}) {
		t.Fatal("nullable evolution backfill changed")
	}
	fixture.require(t, "comment", false, "COMMENT ON TABLE "+table+" IS 'Issue 110 isolated public fixture'")
	created[copyName].attempted = true
	fixture.require(t, "ctas", false, "CREATE TABLE "+copied+
		" WITH (format = 'PARQUET', format_version = 2) AS SELECT id, note, CAST(ROW(note, id) AS ROW(label VARCHAR, counter BIGINT)) AS record, MAP(ARRAY['key'], ARRAY[id]) AS mapping FROM "+table)
	created[copyName].acknowledged = true
	fixture.requireNotes(t, "ctas-readback", copied, expected, true)

	rejected, rejectErr := fixture.invoke(false, "INSERT INTO "+table+" (id) VALUES (CAST('not-an-integer' AS BIGINT))")
	if rejectErr == nil || !errors.Is(rejectErr, trino.ErrOperation) || rejected.Complete() || rejected.Succeeded() ||
		!rejected.Terminal() || rejected.Submissions() != 1 || rejected.Effect() != trino.Unknown {
		t.Fatal("failed mutation ambiguity or one-submission evidence changed")
	}
	fixture.requireNotes(t, "failed-write-independent-readback", table, expected, false)

	fixture.require(t, "insert-select", false, "INSERT INTO "+table+" (id,note) SELECT id + 2000, note FROM "+copied)
	combined := make(map[int64]any, 2*len(expected))
	for id, note := range expected {
		combined[id], combined[id+2000] = note, note
	}
	fixture.requireNotes(t, "insert-select-readback", table, combined, false)
	for _, target := range []string{table, copied} {
		fixture.require(t, "truncate", false, "TRUNCATE TABLE "+target)
		counts := requirePublicServiceRows(t, fixture.require(t, "truncate-count", true, "SELECT count(*) FROM "+target))
		empty := fixture.require(t, "successful-empty", true, "SELECT id FROM "+target)
		if !reflect.DeepEqual(counts, [][]any{{json.Number("0")}}) || empty.Rows() != 0 || string(empty.DataCopy()) != "[]" {
			t.Fatal("truncate readback or successful empty completion changed")
		}
	}
	t.Log("qualified Trino 482 / Iceberg format-2 Parquet / direct JSON / explicit unauthenticated HTTP; public exact types, one 257-row Insert, DDL/DML/evolution and independent readbacks passed")
}

func TestPublicServiceReadbackOracleRejectsWrongState(t *testing.T) {
	row := func(id string, note any) []any {
		number := json.Number(id)
		return []any{number, note, []any{note, number}, map[string]any{"key": number}}
	}
	expected := map[int64]any{1: "merged", 2: nil}
	valid := [][]any{row("1", "merged"), row("2", nil)}
	if !serviceNotesMatch(valid, expected, true) {
		t.Fatal("exact positive service oracle rejected")
	}
	for _, rows := range [][][]any{
		{row("1", "merged"), row("1", "merged")},
		{row("2", nil), row("1", "merged")},
		{row("1", "merged")},
		{row("1", "merged"), row("3", nil)},
		{row("1", "wrong"), row("2", nil)},
		{row("1", "merged"), {json.Number("2"), nil, nil, nil}},
	} {
		if serviceNotesMatch(rows, expected, true) {
			t.Fatal("corrupted service state passed the acceptance oracle")
		}
	}
}
