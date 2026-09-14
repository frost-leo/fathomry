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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// The owner supplies this opt-in file AFTER confirming the existing service,
// permissions and isolation. It is never discovered from another provider.
type nativeServiceConfig struct {
	AuthorizeNativeFixture bool
	SQLAddress             string
	SQLServerName          string
	HTTPOrigins            []string
	Database               string
	User                   string
	Password               string
	RootCAPEM              string
	Plaintext              bool
	Replication            int
}

type nativeService struct {
	fixture
	options     OptionsV1
	replication int
	run         string
}

type nativeServiceTable struct {
	service *nativeService
	name    string
	settled bool
}

func openNativeService(t *testing.T) *nativeService {
	t.Helper()
	path := os.Getenv("FATHOMRY_DORIS_NATIVE_TEST_CONFIG")
	if path == "" {
		t.Skip("no owner-authorized existing Doris native fixture configuration")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("cannot open authorized native test configuration")
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, 128<<10+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(raw) > 128<<10 {
		t.Fatal("invalid native test configuration size/read")
	}
	var config nativeServiceConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF || !config.AuthorizeNativeFixture || !strings.HasPrefix(config.Database, "gh42_") ||
		config.SQLAddress == "" || len(config.HTTPOrigins) == 0 || config.Replication < 1 || config.Replication > 3 {
		t.Fatal("explicit native fixture authorization, gh42 database, endpoints and replication are required")
	}
	options := OptionsV1{Name: "gh42-native", SQLAddress: config.SQLAddress, SQLServerName: config.SQLServerName,
		HTTPOrigins: config.HTTPOrigins, Database: config.Database, User: config.User, Password: config.Password,
		RootCAPEM: config.RootCAPEM, Plaintext: config.Plaintext, Timeout: 30 * time.Second}
	f := bindFixture(t, options, 1)
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	run := hex.EncodeToString(nonce[:])
	t.Log("isolated native fixture run", run)
	return &nativeService{fixture: f, options: options, replication: config.Replication, run: run}
}

func (s *nativeService) sql(t *testing.T, ctx context.Context, query string, read bool) invocation.Result[Result] {
	t.Helper()
	var call invocationResult
	if read {
		call.receipt, call.err = s.client.Query(ctx, correlation("service"), query)
	} else {
		call.receipt, call.err = s.client.Exec(ctx, correlation("service"), query)
	}
	result := observe(t, call.receipt, call.err)
	drain(t, s.inbox, 1)
	return result
}

func (s *nativeService) create(t *testing.T, ctx context.Context, definition string) *nativeServiceTable {
	t.Helper()
	table := &nativeServiceTable{service: s, name: "gh42_" + s.run}
	absenceSQL := "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='" + table.name + "'"
	before := s.sql(t, ctx, absenceSQL, true)
	if before.Err() != nil || !before.Outcome.Value.Complete() || len(before.Outcome.Value.RowsCopy()) != 0 {
		t.Fatal("isolated target absence/connection not verified")
	}
	t.Cleanup(func() {
		if !table.settled {
			t.Error("test-owned mutation outcome unresolved; retain this run's table for owner reconciliation")
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		dropped := s.sql(t, cleanup, "DROP TABLE "+table.name, false)
		if dropped.Err() != nil || !dropped.Outcome.Value.SQLAcknowledged() {
			t.Error("test-owned table drop not acknowledged", dropped.Err())
			return
		}
		absent := s.sql(t, cleanup, absenceSQL, true)
		if absent.Err() != nil || !absent.Outcome.Value.Complete() || len(absent.Outcome.Value.RowsCopy()) != 0 {
			t.Error("test-owned table absence not observed")
			return
		}
		t.Log("test-owned table drop and exact Doris metadata absence observed")
	})
	table.exec(t, ctx, "CREATE TABLE "+table.name+" "+definition+" DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\"=\""+strconv.Itoa(s.replication)+"\")")
	table.settled = true
	return table
}

// A command acknowledgement alone does not settle a write. The caller must
// observe the expected rowset before permitting cleanup of the SQL DML fixture.
func (table *nativeServiceTable) exec(t *testing.T, ctx context.Context, query string) Result {
	t.Helper()
	table.settled = false
	result := table.service.sql(t, ctx, query, false)
	if result.Err() != nil || !result.Outcome.Value.SQLAcknowledged() || !result.Outcome.Value.Complete() {
		t.Fatal("synchronous native statement not acknowledged", serviceErrorLabels(result.Err()))
	}
	return result.Outcome.Value
}

func serviceLoadSettled(result Result, sameBatchPreviouslyVisible bool) bool {
	load, present := result.Load()
	if !present {
		return false
	}
	if load.Duplicate {
		return sameBatchPreviouslyVisible
	}
	switch load.State {
	case LoadNotDispatched, LoadVisible, LoadRejected, LoadAborted:
		return true
	default:
		return false
	}
}

// TestServiceNativeBatch qualifies this Doris profile via Doris endpoints only.
// It does not inspect, provision or clean a catalog or its backing storage.
func TestServiceNativeBatch(t *testing.T) {
	s := openNativeService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	table := s.create(t, ctx, "(id BIGINT NOT NULL, value STRING NULL, amount DECIMAL(38,5) NULL, wide LARGEINT, f FLOAT, happened DATETIME(6)) DUPLICATE KEY(id)")
	const rowCount = 256
	const amount = "12345678901234567890.00100"
	const wide = "170141183460469231731687303715884105727"
	const happened = "2026-09-14 01:02:03.123456"
	type inputRow struct {
		ID       int     `json:"id"`
		Value    *string `json:"value"`
		Amount   *string `json:"amount"`
		Wide     string  `json:"wide"`
		Float    float32 `json:"f"`
		Happened string  `json:"happened"`
	}
	inputs := make([]inputRow, rowCount)
	for index := range inputs {
		inputs[index] = inputRow{ID: index + 1, Wide: wide, Float: 0.1, Happened: happened}
		if index%3 != 2 {
			value, decimal := "", amount
			if index%3 == 1 {
				value = "Unicode: λ, 雪; quote ' and newline\n"
			}
			inputs[index].Value, inputs[index].Amount = &value, &decimal
		}
	}
	payload, err := json.Marshal(inputs)
	if err != nil {
		t.Fatal(err)
	}
	label := "gh42-" + s.run
	batch := Batch{Table: table.name, Label: label, JSON: payload}
	table.settled = false
	receipt, err := s.client.StreamLoad(ctx, correlation("native-load"), batch)
	loaded := observe(t, receipt, err)
	drain(t, s.inbox, 1)
	table.settled = serviceLoadSettled(loaded.Outcome.Value, false)
	load, _ := loaded.Outcome.Value.Load()
	if loaded.Err() != nil || !loaded.Outcome.Value.Complete() || load.LoadedRows != rowCount || load.State != LoadVisible {
		t.Logf("load diagnostics: state=%d http=%d transaction=%t counters=%t labels=%v", load.State, load.HTTPStatus, load.TransactionKnown, load.RowsKnown, serviceErrorLabels(loaded.Err()))
		t.Fatal("native load/row quality/visibility not qualified", loaded.Err())
	}
	readSQL := "SELECT id,value,amount,wide,f,happened FROM " + table.name + " ORDER BY id"
	verify := func() {
		t.Helper()
		readback := s.sql(t, ctx, readSQL, true)
		if readback.Err() != nil || !readback.Outcome.Value.Complete() {
			t.Fatal("independent native SQL read-back failed", readback.Err())
		}
		columns := readback.Outcome.Value.ColumnsCopy()
		if len(columns) != 6 || columns[2].Name != "amount" || !columns[2].PrecisionKnown || columns[2].Precision != 38 || columns[2].Scale != 5 {
			t.Fatal("native decimal metadata differs")
		}
		rows := readback.Outcome.Value.RowsCopy()
		if len(rows) != rowCount {
			t.Fatal("unexpected native row count")
		}
		for index, row := range rows {
			values := row.ValuesCopy()
			input := inputs[index]
			if len(values) != 6 || string(values[0]) != strconv.Itoa(input.ID) || string(values[3]) != wide || string(values[4]) != "0.1" || string(values[5]) != happened {
				t.Fatal("exact native numeric/time content differs at row", index)
			}
			for column, expected := range []*string{input.Value, input.Amount} {
				actual := values[column+1]
				if (expected == nil) != (actual == nil) || expected != nil && string(actual) != *expected {
					t.Fatal("exact native NULL/empty/text/decimal content differs at row", index)
				}
			}
		}
	}
	verify()

	for _, observation := range []struct {
		label string
		state LoadState
		kind  error
	}{{label, LoadVisible, nil}, {label + "-missing", LoadUnknown, ErrUncertain}} {
		receipt, err = s.client.InspectLabel(ctx, correlation("native-label"), observation.label)
		result := observe(t, receipt, err)
		drain(t, s.inbox, 1)
		evidence, present := result.Outcome.Value.Load()
		if !errors.Is(result.Err(), observation.kind) || !present || evidence.State != observation.state || result.Outcome.Value.Complete() != (observation.state == LoadVisible) || evidence.RowsKnown || evidence.TransactionKnown || evidence.Table != "" {
			t.Fatal("retained label observation differs", serviceErrorLabels(result.Err()))
		}
	}

	table.settled = false
	receipt, err = s.client.StreamLoad(ctx, correlation("native-duplicate"), batch)
	duplicate := observe(t, receipt, err)
	drain(t, s.inbox, 1)
	table.settled = serviceLoadSettled(duplicate.Outcome.Value, true)
	if !errors.Is(duplicate.Err(), ErrDuplicate) || duplicate.Outcome.Value.Complete() {
		t.Fatal("retained label did not reject repeated batch")
	}
	verify()

	table.settled = false
	receipt, err = s.client.StreamLoad(ctx, correlation("native-invalid"), Batch{Table: table.name, Label: label + "-invalid", JSON: []byte(`[{"id":"not-an-integer"}]`)})
	rejected := observe(t, receipt, err)
	drain(t, s.inbox, 1)
	table.settled = serviceLoadSettled(rejected.Outcome.Value, false)
	evidence, _ := rejected.Outcome.Value.Load()
	if !errors.Is(rejected.Err(), ErrLoad) || evidence.State != LoadRejected || rejected.Outcome.Value.Complete() {
		t.Fatal("invalid native row not rejected", serviceErrorLabels(rejected.Err()))
	}
	verify()

	limitedOptions := s.options
	limitedOptions.MaxRows = 4
	limited := bindFixture(t, limitedOptions, 1)
	receipt, err = limited.client.Query(ctx, correlation("native-row-limit"), readSQL)
	partial := observe(t, receipt, err)
	drain(t, limited.inbox, 1)
	if !errors.Is(partial.Err(), ErrLimit) || partial.Outcome.Value.Complete() || len(partial.Outcome.Value.RowsCopy()) != 4 {
		t.Fatal("native bounded query did not preserve its partial result")
	}
	for index, row := range partial.Outcome.Value.RowsCopy() {
		values := row.ValuesCopy()
		if len(values) != 6 || string(values[0]) != strconv.Itoa(index+1) {
			t.Fatal("native partial query prefix differs")
		}
	}
	verify()
	t.Log("256-row native batch, exact values/metadata, label observations, duplicate/filter rejection and bounded query passed")
}

func TestServiceNativeSQL(t *testing.T) {
	s := openNativeService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	table := s.create(t, ctx, "(id BIGINT NOT NULL, value STRING NULL) UNIQUE KEY(id)")
	var values []string
	expected := make(map[int]string)
	for id := 1; id <= 64; id++ {
		values = append(values, fmt.Sprintf("(%d,'before')", id))
		expected[id] = "before"
	}
	table.exec(t, ctx, "INSERT INTO "+table.name+" VALUES "+strings.Join(values, ","))
	assertRows := func() {
		t.Helper()
		result := s.sql(t, ctx, "SELECT id,value FROM "+table.name+" ORDER BY id", true)
		rows := result.Outcome.Value.RowsCopy()
		if result.Err() != nil || !result.Outcome.Value.Complete() || !serviceSQLRowsEqual(rows, expected) {
			t.Fatal("fresh native DML read-back differs", serviceErrorLabels(result.Err()))
		}
		table.settled = true
	}
	assertRows()
	table.exec(t, ctx, "UPDATE "+table.name+" SET value='after' WHERE id=1")
	expected[1] = "after"
	assertRows()
	table.exec(t, ctx, "DELETE FROM "+table.name+" WHERE id=64")
	delete(expected, 64)
	assertRows()
	table.exec(t, ctx, "MERGE INTO "+table.name+" target USING (SELECT 1 AS id, 'merged' AS value UNION ALL SELECT 65, 'new') source ON target.id=source.id WHEN MATCHED THEN UPDATE SET value=source.value WHEN NOT MATCHED THEN INSERT (id,value) VALUES (source.id,source.value)")
	expected[1], expected[65] = "merged", "new"
	assertRows()
	table.exec(t, ctx, "INSERT OVERWRITE TABLE "+table.name+" VALUES (1,'overwritten'),(2,'second')")
	expected = map[int]string{1: "overwritten", 2: "second"}
	assertRows()
	table.exec(t, ctx, "TRUNCATE TABLE "+table.name)
	clear(expected)
	assertRows()
	t.Log("native UNIQUE KEY SQL batch insert, UPDATE, DELETE, MERGE, overwrite and truncate passed with fresh reads")
}

func serviceSQLRowsEqual(rows []Row, expected map[int]string) bool {
	if len(rows) != len(expected) {
		return false
	}
	seen := make(map[int]bool, len(rows))
	for _, row := range rows {
		values := row.ValuesCopy()
		if len(values) != 2 || values[0] == nil || values[1] == nil {
			return false
		}
		id, err := strconv.Atoi(string(values[0]))
		value, exists := expected[id]
		if err != nil || !exists || seen[id] || string(values[1]) != value {
			return false
		}
		seen[id] = true
	}
	return true
}

func TestServiceSQLRowsetOracle(t *testing.T) {
	row := func(id, value string) Row { return Row{cells: []cell{{text: id}, {text: value}}} }
	expected := map[int]string{1: "merged", 65: "new"}
	if !serviceSQLRowsEqual([]Row{row("1", "merged"), row("65", "new")}, expected) || !serviceSQLRowsEqual(nil, nil) {
		t.Fatal("exact SQL fixture rowset rejected")
	}
	for _, rows := range [][]Row{
		{row("1", "merged"), row("2", "new")},
		{row("1", "merged"), row("65", "wrong")},
		{row("1", "merged"), row("1", "merged")},
		{row("1", "merged")},
		{row("1", "merged"), {cells: []cell{{text: "65"}, {null: true}}}},
		{row("1", "merged"), {}},
	} {
		if serviceSQLRowsEqual(rows, expected) {
			t.Fatal("incorrect DML fixture rowset certified")
		}
	}
}

func TestServiceCleanupRequiresSettledMutation(t *testing.T) {
	if serviceLoadSettled(Result{}, true) {
		t.Fatal("missing evidence authorized cleanup")
	}
	for _, test := range []struct {
		state     LoadState
		duplicate bool
		prior     bool
		want      bool
	}{
		{LoadUnknown, false, false, false}, {LoadPending, false, false, false}, {LoadCommitted, false, false, false},
		{LoadNotDispatched, false, false, true}, {LoadVisible, false, false, true}, {LoadRejected, false, false, true}, {LoadAborted, false, false, true},
		{LoadUnknown, true, false, false}, {LoadUnknown, true, true, true},
	} {
		result := Result{data: &resultData{loadPresent: true, load: LoadEvidence{State: test.state, Duplicate: test.duplicate}}}
		if serviceLoadSettled(result, test.prior) != test.want {
			t.Fatal("ambiguous effect evidence authorized destructive cleanup")
		}
	}
}

func serviceErrorLabels(err error) []string {
	var labels []string
	var visit func(error)
	visit = func(err error) {
		if err == nil || len(labels) >= 16 {
			return
		}
		if value, ok := err.(*fault.Error); ok {
			d := value.Diagnostic()
			labels = append(labels, string(d.Kind)+":"+d.Context.Operation)
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range value.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(value.Unwrap())
		}
	}
	visit(err)
	return labels
}
