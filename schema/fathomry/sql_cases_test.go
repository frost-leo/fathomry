//go:build schema_service

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

package fathomry_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"reflect"
	"strconv"
	"strings"
	"testing"

	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)

func (fixture sqlFixture) parameters(count int) string {
	values := make([]string, count)
	for index := range values {
		values[index] = "?"
		if fixture.dialect == "postgresql" {
			values[index] = "$" + strconv.Itoa(index+1)
		}
	}
	return strings.Join(values, ", ")
}

func (fixture sqlFixture) identity(t *testing.T, query string) int64 {
	t.Helper()
	value, err := strconv.ParseInt(fixture.scalar(t, query), 10, 64)
	if err != nil || value <= 0 {
		t.Fatal("fixture identity is not a positive generated int64")
	}
	return value
}

func (fixture sqlFixture) binary(expression string) string {
	if fixture.dialect == "postgresql" {
		return "convert_to(" + expression + ", 'UTF8')"
	}
	return expression
}

func verifyStoredComments(t *testing.T, fixture sqlFixture) {
	t.Helper()
	expected := sourceComments(t, fixture.assets, fixture.dialect)
	actual := make(map[string]string)
	var tables, columns [][][]byte
	name := strings.Trim(fixture.target, "\x60\"")
	if fixture.dialect == "postgresql" {
		tables = fixture.rows(t, "SELECT c.relname, pg_catalog.obj_description(c.oid, 'pg_class') FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='"+name+"' AND c.relkind='r'")
		columns = fixture.rows(t, "SELECT c.relname, a.attname, pg_catalog.col_description(c.oid, a.attnum) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid WHERE n.nspname='"+name+"' AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped")
	} else {
		tables = fixture.rows(t, "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='"+name+"' AND TABLE_TYPE='BASE TABLE'")
		columns = fixture.rows(t, "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='"+name+"'")
	}
	for _, row := range tables {
		if len(row) != 2 || row[1] == nil {
			t.Fatal("missing persisted table comment")
		}
		actual[string(row[0])] = string(row[1])
	}
	for _, row := range columns {
		if len(row) != 3 || row[2] == nil {
			t.Fatal("missing persisted column comment")
		}
		actual[string(row[0])+"."+string(row[1])] = string(row[2])
	}
	if len(tables) != 9 || len(columns) != 74 || !reflect.DeepEqual(actual, expected) {
		t.Fatal("database comments differ from complete multilingual source", len(tables), len(columns))
	}
	t.Log("verified exact en/zh-CN comments: 9 tables + 74 columns")
}

func exerciseCatalogSQL(t *testing.T, fixture sqlFixture) {
	t.Helper()
	verifyStoredComments(t, fixture)
	fixture.reject(t, "DELETE FROM {{target}}.i18n_locale WHERE code='en'", "23514", "1644")
	fixture.reject(t, "UPDATE {{target}}.i18n_locale SET enabled=FALSE WHERE code='en'", "23514", "3819")
	fixture.reject(t, "UPDATE {{target}}.i18n_locale SET code='EN' WHERE code='en'", "23514", "1644")
	fixture.reject(t, "INSERT INTO {{target}}.i18n_locale (code,native_name) VALUES ('EN','English')", "23505", "1062")
	fixture.execute(t, "INSERT INTO {{target}}.i18n_locale (code,native_name) VALUES ('zh-CN','中文')")
	fixture.execute(t, "INSERT INTO {{target}}.workflow_category (namespace,code) VALUES ('alpha','commerce'),('beta','commerce')")
	fixture.execute(t, "INSERT INTO {{target}}.workflow_definition (namespace,code) VALUES ('alpha','orders'),('alpha','other'),('Alpha','orders'),('alpha ','orders')")
	fixture.reject(t, "INSERT INTO {{target}}.workflow_definition (namespace,code) VALUES ('alpha','orders')", "23505", "1062")
	definition := fixture.identity(t, "SELECT id FROM {{target}}.workflow_definition WHERE namespace='alpha' AND code='orders'")
	other := fixture.identity(t, "SELECT id FROM {{target}}.workflow_definition WHERE namespace='alpha' AND code='other'")
	if fixture.scalar(t, "SELECT COUNT(*) FROM {{target}}.workflow_definition") != "4" {
		t.Fatal("case or trailing-byte machine identities collapsed")
	}
	defaults := fixture.rows(t, fmt.Sprintf("SELECT CASE WHEN enabled THEN 1 ELSE 0 END, default_version_id FROM {{target}}.workflow_definition WHERE id=%d", definition))
	if len(defaults) != 1 || string(defaults[0][0]) != "0" || defaults[0][1] != nil {
		t.Fatal("initial admission/default state differs")
	}
	category := fixture.identity(t, "SELECT id FROM {{target}}.workflow_category WHERE namespace='alpha'")
	foreignCategory := fixture.identity(t, "SELECT id FROM {{target}}.workflow_category WHERE namespace='beta'")
	fixture.execute(t, fmt.Sprintf("UPDATE {{target}}.workflow_definition SET category_id=%d WHERE id=%d", category, definition))
	fixture.reject(t, fmt.Sprintf("UPDATE {{target}}.workflow_definition SET category_id=%d WHERE id=%d", foreignCategory, definition), "23503", "1452")
	fixture.execute(t, fmt.Sprintf("UPDATE {{target}}.workflow_category SET enabled=FALSE WHERE id=%d", category))
	if fixture.scalar(t, fmt.Sprintf("SELECT category_id FROM {{target}}.workflow_definition WHERE id=%d", definition)) != strconv.FormatInt(category, 10) {
		t.Fatal("category disabling changed an existing association")
	}
	fixture.execute(t, fmt.Sprintf("INSERT INTO {{target}}.workflow_definition_tag (workflow_definition_id,tag_key,tag_value) VALUES (%d,'note','')", definition))

	bundle, err := wf.Compile(context.Background(), health.Declarations())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bundle.BytesCopy()
	if err != nil {
		t.Fatal(err)
	}
	var portable struct {
		Workflows []struct {
			Spec  json.RawMessage `json:"spec"`
			Hash  string          `json:"spec_sha256"`
			Modes []struct {
				Config json.RawMessage `json:"config"`
				Hash   string          `json:"config_sha256"`
			} `json:"modes"`
		} `json:"workflows"`
	}
	if err := json.Unmarshal(raw, &portable); err != nil {
		t.Fatal(err)
	}
	declared := portable.Workflows[0]
	digest, err := hex.DecodeString(declared.Hash)
	if err != nil {
		t.Fatal(err)
	}
	insertVersion := "INSERT INTO {{target}}.workflow_version (workflow_definition_id,version_code,spec_format,fingerprint_profile,spec_sha256,spec_document) VALUES (" + fixture.parameters(6) + ")"
	fixture.execute(t, insertVersion, definition, "v1", wf.SpecFormat, wf.SpecProfile, digest, []byte(declared.Spec))
	fixture.execute(t, insertVersion, other, "v1", wf.SpecFormat, wf.SpecProfile, digest, []byte(declared.Spec))
	version := fixture.identity(t, fmt.Sprintf("SELECT id FROM {{target}}.workflow_version WHERE workflow_definition_id=%d", definition))
	foreignVersion := fixture.identity(t, fmt.Sprintf("SELECT id FROM {{target}}.workflow_version WHERE workflow_definition_id=%d", other))
	fixture.execute(t, fmt.Sprintf("INSERT INTO {{target}}.workflow_mode (workflow_version_id,code) VALUES (%d,'standard'),(%d,'standard')", version, foreignVersion))
	mode := fixture.identity(t, fmt.Sprintf("SELECT id FROM {{target}}.workflow_mode WHERE workflow_version_id=%d", version))
	foreignMode := fixture.identity(t, fmt.Sprintf("SELECT id FROM {{target}}.workflow_mode WHERE workflow_version_id=%d", foreignVersion))
	modeDigest, err := hex.DecodeString(declared.Modes[0].Hash)
	if err != nil {
		t.Fatal(err)
	}
	insertMode := "INSERT INTO {{target}}.workflow_mode_version (workflow_mode_id,version_code,config_format,fingerprint_profile,config_sha256,config_document) VALUES (" + fixture.parameters(6) + ")"
	fixture.execute(t, insertMode, mode, "r1", wf.ModeFormat, wf.ModeProfile, modeDigest, []byte(declared.Modes[0].Config))
	fixture.execute(t, insertMode, foreignMode, "r1", wf.ModeFormat, wf.ModeProfile, modeDigest, []byte(declared.Modes[0].Config))
	revision := fixture.identity(t, fmt.Sprintf("SELECT id FROM {{target}}.workflow_mode_version WHERE workflow_mode_id=%d", mode))
	foreignRevision := fixture.identity(t, fmt.Sprintf("SELECT id FROM {{target}}.workflow_mode_version WHERE workflow_mode_id=%d", foreignMode))

	fixture.execute(t, fmt.Sprintf("UPDATE {{target}}.workflow_definition SET default_version_id=%d WHERE id=%d", version, definition))
	fixture.execute(t, fmt.Sprintf("UPDATE {{target}}.workflow_version SET default_mode_id=%d WHERE id=%d", mode, version))
	fixture.execute(t, fmt.Sprintf("UPDATE {{target}}.workflow_mode SET default_mode_version_id=%d WHERE id=%d", revision, mode))
	fixture.reject(t, fmt.Sprintf("UPDATE {{target}}.workflow_definition SET default_version_id=%d WHERE id=%d", foreignVersion, definition), "23503", "1452")
	fixture.reject(t, fmt.Sprintf("UPDATE {{target}}.workflow_version SET default_mode_id=%d WHERE id=%d", foreignMode, version), "23503", "1452")
	fixture.reject(t, fmt.Sprintf("UPDATE {{target}}.workflow_mode SET default_mode_version_id=%d WHERE id=%d", foreignRevision, mode), "23503", "1452")

	key := "v1;255:" + strings.Repeat("n", 255) + "128:" + strings.Repeat("w", 128) + "128:" + strings.Repeat("V", 128) + "128:" + strings.Repeat("m", 128) + "128:" + strings.Repeat("R", 128) + "12:display_name"
	fixture.execute(t, "INSERT INTO {{target}}.i18n_resource (scope,code) VALUES ("+fixture.parameters(2)+")", "workflow.mode_revision", key)
	fixture.execute(t, "INSERT INTO {{target}}.i18n_resource (scope,code) VALUES ('workflow', REPEAT('x',1024))")
	resource := fixture.identity(t, "SELECT id FROM {{target}}.i18n_resource WHERE scope='workflow'")
	english := fixture.identity(t, "SELECT id FROM {{target}}.i18n_locale WHERE code='en'")
	fixture.execute(t, "INSERT INTO {{target}}.i18n_text (resource_id,locale_id,value) VALUES ("+fixture.parameters(3)+")", resource, english, "")
	empty := fixture.rows(t, "SELECT value FROM {{target}}.i18n_text")
	if len(empty) != 1 || empty[0][0] == nil || len(empty[0][0]) != 0 {
		t.Fatal("intentional empty text lost")
	}
	verifyImmutableGuards(t, fixture, definition, version, mode, revision)
	before := fixture.rows(t, fmt.Sprintf("SELECT row_revision,created_at,updated_at FROM {{target}}.workflow_version WHERE id=%d", version))
	fixture.execute(t, fmt.Sprintf("UPDATE {{target}}.workflow_version SET version_code=version_code,spec_document=spec_document WHERE id=%d", version))
	if !reflect.DeepEqual(before, fixture.rows(t, fmt.Sprintf("SELECT row_revision,created_at,updated_at FROM {{target}}.workflow_version WHERE id=%d", version))) {
		t.Fatal("same-value update changed management facts")
	}
	read := "SELECT spec_document FROM {{target}}.workflow_version WHERE id=" + strconv.FormatInt(version, 10)
	stored := fixture.rows(t, read)[0][0]
	if fixture.dialect == "postgresql" {
		if !bytes.HasPrefix(stored, []byte("\\x")) {
			t.Fatal("expected bytea hex")
		}
		stored, err = hex.DecodeString(string(stored[2:]))
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(stored, declared.Spec) {
		t.Fatal("canonical binary storage changed bytes")
	}
	verifyStorageBounds(t, fixture, other, foreignMode)
	verifyConstraintAblation(t, fixture, definition, foreignVersion, revision)
}

func verifyImmutableGuards(t *testing.T, fixture sqlFixture, definition, version, mode, revision int64) {
	t.Helper()
	cases := []string{
		"UPDATE {{target}}.i18n_resource SET scope='other' WHERE scope='workflow'",
		"UPDATE {{target}}.i18n_text SET resource_id=resource_id+100000",
		"UPDATE {{target}}.workflow_category SET namespace='changed'",
		fmt.Sprintf("UPDATE {{target}}.workflow_definition SET code='changed' WHERE id=%d", definition),
		fmt.Sprintf("UPDATE {{target}}.workflow_definition_tag SET tag_key='changed' WHERE workflow_definition_id=%d", definition),
		fmt.Sprintf("UPDATE {{target}}.workflow_version SET spec_document=%s WHERE id=%d", fixture.binary("'changed'"), version),
		fmt.Sprintf("UPDATE {{target}}.workflow_mode SET workflow_version_id=workflow_version_id+100000 WHERE id=%d", mode),
		fmt.Sprintf("UPDATE {{target}}.workflow_mode_version SET config_document=%s WHERE id=%d", fixture.binary("'changed'"), revision),
	}
	for index, sql := range cases {
		t.Run(fmt.Sprintf("immutable_%d", index), func(t *testing.T) { fixture.reject(t, sql, "23514", "1644") })
	}
	fixture.reject(t, "UPDATE {{target}}.workflow_definition SET row_revision=0", "23514", "3819")
	for _, table := range catalogTables {
		if table == "workflow_definition_tag" {
			continue
		}
		change := "created_at + INTERVAL '1 second'"
		if fixture.dialect == "mysql" {
			change = "created_at + INTERVAL 1 SECOND"
		}
		fixture.reject(t, "UPDATE {{target}}."+table+" SET created_at="+change, "23514", "1644")
	}
}

func verifyStorageBounds(t *testing.T, fixture sqlFixture, definition, mode int64) {
	t.Helper()
	fixture.execute(t, "INSERT INTO {{target}}.workflow_definition (namespace,code) VALUES (REPEAT('中',85),'utf8_bound')")
	if fixture.scalar(t, "SELECT OCTET_LENGTH(namespace) FROM {{target}}.workflow_definition WHERE code='utf8_bound'") != "255" {
		t.Fatal("UTF-8 byte identity bound was not preserved")
	}
	fixture.reject(t, "INSERT INTO {{target}}.workflow_definition (namespace,code) VALUES (REPEAT('中',86),'utf8_over')", "23514", "1406")
	fixture.reject(t, "INSERT INTO {{target}}.workflow_definition (namespace,code) VALUES ('','empty')", "23514", "3819")
	fixture.reject(t, "INSERT INTO {{target}}.i18n_resource (scope,code) VALUES ('workflow',REPEAT('x',1025))", "23514", "1406")
	version := func(label string, digestBytes, documentBytes int) string {
		return fmt.Sprintf("INSERT INTO {{target}}.workflow_version (workflow_definition_id,version_code,spec_format,fingerprint_profile,spec_sha256,spec_document) VALUES (%d,'%s','%s','%s',%s,%s)",
			definition, label, wf.SpecFormat, wf.SpecProfile,
			fixture.binary(fmt.Sprintf("REPEAT('0',%d)", digestBytes)),
			fixture.binary(fmt.Sprintf("REPEAT('x',%d)", documentBytes)))
	}
	fixture.execute(t, version("synthetic_size_bound", 32, wf.MaxSpecBytes))
	fixture.reject(t, version("synthetic_size_over", 32, wf.MaxSpecBytes+1), "23514", "3819")
	fixture.reject(t, version("synthetic_short_digest", 31, 1), "23514", "3819")
	fixture.reject(t, version("synthetic_empty_document", 32, 0), "23514", "3819")
	modeVersion := func(label string, size int) string {
		return fmt.Sprintf("INSERT INTO {{target}}.workflow_mode_version (workflow_mode_id,version_code,config_format,fingerprint_profile,config_sha256,config_document) VALUES (%d,'%s','%s','%s',%s,%s)",
			mode, label, wf.ModeFormat, wf.ModeProfile, fixture.binary("REPEAT('0',32)"), fixture.binary(fmt.Sprintf("REPEAT('x',%d)", size)))
	}
	fixture.execute(t, modeVersion("synthetic_size_bound", wf.MaxModeBytes))
	fixture.reject(t, modeVersion("synthetic_size_over", wf.MaxModeBytes+1), "23514", "3819")
	t.Log("synthetic server-side BLOB size controls passed; this is not large-document Adapter transport qualification")
}

func verifyConstraintAblation(t *testing.T, fixture sqlFixture, definition, foreignVersion, revision int64) {
	t.Helper()
	drop := "ALTER TABLE {{target}}.workflow_definition DROP CONSTRAINT fk_workflow_definition_default_version"
	if fixture.dialect == "mysql" {
		drop = "ALTER TABLE {{target}}.workflow_definition DROP FOREIGN KEY fk_workflow_definition_default_version"
	}
	fixture.execute(t, drop)
	fixture.execute(t, "ALTER TABLE {{target}}.workflow_definition ADD CONSTRAINT weak_default FOREIGN KEY (default_version_id) REFERENCES {{target}}.workflow_version(id)")
	fixture.execute(t, fmt.Sprintf("UPDATE {{target}}.workflow_definition SET default_version_id=%d WHERE id=%d", foreignVersion, definition))
	t.Log("negative control: existence-only default FK admits a foreign owner")
	drop = "DROP TRIGGER immutable_workflow_mode_version ON {{target}}.workflow_mode_version"
	if fixture.dialect == "mysql" {
		drop = "DROP TRIGGER {{target}}.immutable_workflow_mode_version"
	}
	fixture.execute(t, drop)
	fixture.execute(t, fmt.Sprintf("UPDATE {{target}}.workflow_mode_version SET config_document=%s WHERE id=%d", fixture.binary("'changed'"), revision))
	t.Log("negative control: removing immutable guard admits changed control bytes")
	sql, err := fs.ReadFile(fixture.assets, "0001/guards/workflow_mode_version.sql")
	if err != nil {
		t.Fatal(err)
	}
	fixture.execute(t, string(sql))
	fixture.reject(t, fmt.Sprintf("UPDATE {{target}}.workflow_mode_version SET config_document=%s WHERE id=%d", fixture.binary("'changed-again'"), revision), "23514", "1644")
}
