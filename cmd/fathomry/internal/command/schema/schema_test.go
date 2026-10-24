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

package schema

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	"github.com/frost-leo/fathomry/failure/v1"
	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)

func TestOwnedManifestAndTargetBounds(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		contract, err := load(dialect)
		if err != nil || len(contract.expected) < 100 || len(contract.sql) < 20 {
			t.Fatal("owned contract unavailable", err)
		}
		if len(ownedObjects(contract.expected, historyTable)) < 2 {
			t.Fatal("bootstrap structure is not checked")
		}
		for _, target := range []string{"simple", "a\"b", "a`b", "a'b", "old", "new", "控制库"} {
			if _, err := quoteTarget(dialect, target); err != nil {
				t.Fatal("admitted target rejected", dialect, err)
			}
		}
		for _, target := range []string{"", "nul\x00", "line\n", "information_schema"} {
			if _, err := quoteTarget(dialect, target); !errors.Is(err, databasecmd.ErrInput) {
				t.Fatal("unsafe target admitted", dialect)
			}
		}
	}
	for _, target := range []string{strings.Repeat("x", 64), "$catalog_table$", "pg_temp"} {
		if _, err := quoteTarget("postgresql", target); !errors.Is(err, databasecmd.ErrInput) {
			t.Fatal("unsafe PostgreSQL target admitted")
		}
	}
	for _, target := range []string{strings.Repeat("x", 65), "emoji😀", "a.b", "a/b", "a\\b", "trailing ", "MYSQL"} {
		if _, err := quoteTarget("mysql", target); !errors.Is(err, databasecmd.ErrInput) {
			t.Fatal("unsafe MySQL target admitted")
		}
	}
	if _, err := quoteTarget("unknown", "x"); !errors.Is(err, databasecmd.ErrInput) {
		t.Fatal("unknown dialect admitted")
	}
}
func copyAssets(t *testing.T) fstest.MapFS {
	t.Helper()
	result := fstest.MapFS{}
	err := fs.WalkDir(pgassets.Assets(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := fs.ReadFile(pgassets.Assets(), path)
		if err != nil {
			return err
		}
		result[path] = &fstest.MapFile{Data: raw}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestManifestRejectsChangedSources(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(fstest.MapFS)
	}{
		{"bootstrap", func(assets fstest.MapFS) { assets["bootstrap/fathomry_schema_history.sql"].Data[0] = '!' }},
		{"table", func(assets fstest.MapFS) { assets["0001/i18n_text.sql"].Data[0] = '!' }},
		{"structure", func(assets fstest.MapFS) { assets["0001/structure.json"].Data[0] = '!' }},
		{"missing", func(assets fstest.MapFS) { delete(assets, "0001/structure.json") }},
		{"duplicate_json_key", func(assets fstest.MapFS) {
			assets["manifest.json"].Data = append([]byte("{\"family\":\"ignored\","), assets["manifest.json"].Data[1:]...)
		}},
		{"unknown_field", func(assets fstest.MapFS) {
			assets["manifest.json"].Data = append([]byte("{\"execute\":\"untrusted\","), assets["manifest.json"].Data[1:]...)
		}},
		{"oversize", func(assets fstest.MapFS) { assets["manifest.json"].Data = []byte(strings.Repeat(" ", 64<<10+1)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			assets := copyAssets(t)
			test.edit(assets)
			if value, err := loadAssets(assets, "postgresql"); value != nil || !errors.Is(err, databasecmd.ErrAssets) {
				t.Fatal("untrusted assets admitted", err)
			}
		})
	}
	for _, edit := range []func(*manifest){
		func(value *manifest) { value.Migrations[0].Revision = 2 },
		func(value *manifest) { value.Migrations[0].Transaction = "implicit-commit" },
		func(value *manifest) { value.Migrations[0].Steps[0].Path = "../outside.sql" },
		func(value *manifest) { value.Migrations[0].Steps[1] = value.Migrations[0].Steps[0] },
		func(value *manifest) { value.Bootstrap.Path = "0001/elsewhere.sql" },
		func(value *manifest) { value.Migrations[0].Steps = make([]asset, maxStatements+1) },
	} {
		assets := copyAssets(t)
		var value manifest
		if err := json.Unmarshal(assets["manifest.json"].Data, &value); err != nil {
			t.Fatal(err)
		}
		edit(&value)
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		assets["manifest.json"].Data = raw
		if actual, err := loadAssets(assets, "postgresql"); actual != nil || !errors.Is(err, databasecmd.ErrAssets) {
			t.Fatal("changed manifest admitted", err)
		}
	}
}
func TestIdentifierNormalizationDoesNotRewriteValuesOrAliases(t *testing.T) {
	cases := []struct{ sql, target, want string }{
		{`CREATE INDEX idx ON example.table USING btree (code)`, "example", `CREATE INDEX idx ON {{target}}.table USING btree (code)`},
		{`FOREIGN KEY (id) REFERENCES "a""b".table(id)`, `a"b`, `FOREIGN KEY (id) REFERENCES {{target}}.table(id)`},
		{`CREATE TRIGGER g BEFORE UPDATE ON old.table FOR EACH ROW WHEN (old.id <> new.id) EXECUTE FUNCTION old.guard()`, "old", `CREATE TRIGGER g BEFORE UPDATE ON {{target}}.table FOR EACH ROW WHEN (old.id <> new.id) EXECUTE FUNCTION {{target}}.guard()`},
		{`CREATE FUNCTION new.guard() RETURNS trigger AS $body$ SELECT 'new.text', new.value $body$ LANGUAGE sql`, "new", `CREATE FUNCTION {{target}}.guard() RETURNS trigger AS $body$ SELECT 'new.text', new.value $body$ LANGUAGE sql`},
		{`CHECK (value = 'ON example.table' OR value = E'quoted\\\\\' ON example.table')`, "example", `CHECK (value = 'ON example.table' OR value = E'quoted\\\\\' ON example.table')`},
		{`CHECK (value = 'it''s example.table')`, "example", `CHECK (value = 'it''s example.table')`},
	}
	for _, test := range cases {
		if got := pgassets.NormalizeInspection(test.sql, test.target); got != test.want {
			t.Fatalf("unsafe normalization: %q", got)
		}
	}
	for _, sql := range []string{
		"CREATE TABLE `a` (id bigint AUTO_INCREMENT COMMENT ' AUTO_INCREMENT=42 ') ENGINE=InnoDB AUTO_INCREMENT=23 COMMENT='x'' AUTO_INCREMENT=19 '",
		"CREATE TABLE `a` (id bigint AUTO_INCREMENT COMMENT ' AUTO_INCREMENT=42 ') ENGINE=InnoDB AUTO_INCREMENT=999 COMMENT='x'' AUTO_INCREMENT=19 '",
	} {
		want := "CREATE TABLE `a` (id bigint AUTO_INCREMENT COMMENT ' AUTO_INCREMENT=42 ') ENGINE=InnoDB COMMENT='x'' AUTO_INCREMENT=19 '"
		if got := mysqlassets.NormalizeInspection(sql); got != want {
			t.Fatal("counter normalization changed a quoted value", got)
		}
	}
}
func TestDirectPrivilegeProofIsConservative(t *testing.T) {
	rows := func(values ...string) [][][]byte {
		result := [][][]byte{}
		for _, value := range values {
			result = append(result, [][]byte{[]byte(value)})
		}
		return result
	}
	for _, test := range []struct {
		grants          [][][]byte
		selectOK, super bool
	}{
		{rows("GRANT ALL PRIVILEGES ON *.* TO `operator`@`localhost`"), true, true},
		{rows("GRANT SELECT, TRIGGER ON `selected`.* TO `reader`@`localhost`"), true, false},
		{rows("GRANT SELECT ON `other`.* TO `reader`@`localhost`"), false, false},
		{rows("GRANT SELECT ON `select%`.* TO `reader`@`localhost`"), false, false},
		{rows("GRANT `all_role`@`%` TO `reader`@`localhost`"), false, false},
		{rows("GRANT SELECT ON *.* TO `reader`@`localhost`", "REVOKE SELECT ON `selected`.* FROM `reader`@`localhost`"), false, false},
	} {
		actual, super := directGrants(test.grants, "`selected`")
		if actual["SELECT"] != test.selectOK || super != test.super {
			t.Fatal("unsupported privilege inference")
		}
	}
}
func TestRunnerLocalOwnershipAndSafeFailure(t *testing.T) {
	if value, err := PostgreSQL(nil, "x", databasecmd.Options{}); value != nil || !errors.Is(err, databasecmd.ErrInput) {
		t.Fatal("nil capability admitted")
	}
	for _, timeout := range []time.Duration{-1, time.Nanosecond, time.Minute + 1} {
		if value, err := PostgreSQL(new(postgres.Client), "x", databasecmd.Options{Timeout: timeout}); value != nil || !errors.Is(err, databasecmd.ErrInput) {
			t.Fatal("invalid timeout admitted")
		}
	}
	runner, err := PostgreSQL(new(postgres.Client), "x", databasecmd.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if runner.state.pending != nil || !runner.Released() {
		t.Fatal("construction acquired ownership")
	}
	cause := errors.New("private-credential-canary")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	status, err := runner.Status(ctx)
	if status.State != Indeterminate || !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation evidence lost", err)
	}
	for _, display := range []string{fmt.Sprint(err), fmt.Sprintf("%#v", fail(databasecmd.ErrDatabase, cause)), fmt.Sprintf("%+v", runner)} {
		if strings.Contains(display, "private-credential") {
			t.Fatal("native diagnostic leaked")
		}
	}
	if _, err := json.Marshal(runner); !errors.Is(err, databasecmd.ErrSerialization) {
		t.Fatal("runtime serialized")
	}
	if err := runner.Close(nil); !errors.Is(err, databasecmd.ErrInput) {
		t.Fatal("nil close context accepted")
	}
	runner.state.mu.Lock()
	if err := runner.Close(context.Background()); !errors.Is(err, databasecmd.ErrBusy) {
		t.Fatal("overlapping close admitted")
	}
	runner.state.mu.Unlock()
	if _, err := runner.Status(context.Background()); !errors.Is(err, databasecmd.ErrClosed) {
		t.Fatal("closing runner accepted work")
	}
	if err := runner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner.state.cleanup = fail(databasecmd.ErrCleanup, cause)
	for range 2 {
		if err := runner.Close(context.Background()); !errors.Is(err, cause) {
			t.Fatal("late cleanup history erased")
		}
	}
}
func TestRunnerPreservesDatabaseFailureOccurrence(t *testing.T) {
	runner, err := PostgreSQL(new(postgres.Client), "control", databasecmd.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func(context.Context) error{
		func(ctx context.Context) error { _, err := runner.Status(ctx); return err },
		func(ctx context.Context) error { _, err := runner.Plan(ctx); return err },
		func(ctx context.Context) error { _, err := runner.Apply(ctx); return err },
	} {
		err := operation(context.Background())
		core, known := failure.Inspect(err)
		if !known || core.Diagnostic().Definition.Code != databasecmd.ErrDatabase || !errors.Is(err, postgres.ErrInput) || !runner.Released() {
			t.Fatal("cleanup without a failure erased the operation occurrence", err)
		}
	}
}

func TestObjectComparisonAndSQLBudget(t *testing.T) {
	first := []object{{Kind: "column", Owner: "table", Name: "key", Attributes: []string{"bigint", "true"}}}
	second := slices.Clone(first)
	second[0].Attributes = slices.Clone(second[0].Attributes)
	second[0].Attributes[0] = "integer"
	if sameObjects(first, second) || !sameObjects(first, first) {
		t.Fatal("structure comparison ignored type changes")
	}
	session := &connection{ctx: context.Background(), calls: 256}
	if err := session.admit(); !errors.Is(err, databasecmd.ErrLimit) {
		t.Fatal("SQL ceiling bypassed")
	}
	objects := []object{}
	oversized := object{Kind: "column", Owner: "table", Name: "key", Attributes: []string{strings.Repeat("x", maxStructureBytes)}}
	if err := appendObject(&objects, oversized); !errors.Is(err, databasecmd.ErrLimit) || len(objects) != 0 {
		t.Fatal("oversize object retained")
	}
	objects = make([]object, maxObjects)
	if err := appendObject(&objects, first[0]); !errors.Is(err, databasecmd.ErrLimit) || len(objects) != maxObjects {
		t.Fatal("object count bound bypassed")
	}
}
func FuzzTargetAndSQLNormalization(f *testing.F) {
	f.Add("old", `CREATE TRIGGER g BEFORE UPDATE ON old.t WHEN (old.id <> new.id) EXECUTE FUNCTION old.guard()`)
	f.Add("quoted", `CHECK (a='ON quoted.t')`)
	f.Fuzz(func(t *testing.T, target, sql string) {
		if len(target) > 256 || len(sql) > 64<<10 {
			t.Skip()
		}
		for _, dialect := range []string{"postgresql", "mysql"} {
			quoted, err := quoteTarget(dialect, target)
			if err == nil && (len(quoted) < 2 || quoted[0] != quoted[len(quoted)-1]) {
				t.Fatal("invalid quote boundary")
			}
		}
		one := pgassets.NormalizeInspection(sql, target)
		if len(one) > len(sql)*12+1 {
			t.Fatal("unbounded normalization expansion")
		}
		_ = mysqlassets.NormalizeInspection(sql)
	})
}
