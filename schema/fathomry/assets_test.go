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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"os/exec"
	"path"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"
	"unicode"
	"unicode/utf8"

	mysqlschema "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	postgreschema "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)

type manifest struct {
	Format    string `json:"format"`
	Family    string `json:"family"`
	Dialect   string `json:"dialect"`
	Profile   string `json:"profile"`
	Bootstrap step   `json:"bootstrap"`
	Structure struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	} `json:"structure"`
	DocumentationLocales []string    `json:"documentation_locales"`
	Migrations           []migration `json:"migrations"`
	License              string      `json:"license"`
	LicenseNotice        []string    `json:"license_notice"`
}
type migration struct {
	Revision    int    `json:"revision"`
	Name        string `json:"name"`
	Transaction string `json:"transaction"`
	Steps       []step `json:"steps"`
}
type step struct {
	ID     string `json:"id"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

var catalogTables = []string{
	"i18n_locale", "i18n_resource", "i18n_text", "workflow_category",
	"workflow_definition", "workflow_definition_tag", "workflow_version",
	"workflow_mode", "workflow_mode_version",
}

func readManifest(t *testing.T, assets fs.FS) manifest {
	t.Helper()
	raw, err := fs.ReadFile(assets, "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value manifest
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatal("trailing manifest data", err)
	}
	return value
}

func TestDialectAssets(t *testing.T) {
	var logical []string
	for _, selected := range []struct {
		dialect string
		profile string
		mode    string
		assets  fs.FS
	}{
		{"postgresql", "postgresql-18-utf8", "required", postgreschema.Assets()},
		{"mysql", "mysql-8.4-innodb-16k-dynamic-strict", "implicit-commit", mysqlschema.Assets()},
	} {
		t.Run(selected.dialect, func(t *testing.T) {
			value := readManifest(t, selected.assets)
			if value.Format != "fathomry.schema-manifest/v1" || value.Family != "fathomry.control" ||
				value.Dialect != selected.dialect || value.Profile != selected.profile ||
				value.License != "GPL-3.0-or-later" || len(value.LicenseNotice) < 16 {
				t.Fatal("manifest identity or license mismatch")
			}
			if !slices.Equal(value.DocumentationLocales, []string{"en", "zh-CN"}) {
				t.Fatal("schema documentation locales differ from the supported contract")
			}
			if len(value.Migrations) != 1 || value.Migrations[0].Revision != 1 ||
				value.Migrations[0].Name != "workflow_catalog" || value.Migrations[0].Transaction != selected.mode {
				t.Fatal("unexpected initial migration contract")
			}
			listed := map[string]bool{"manifest.json": true}
			var provisioning struct {
				Statements []step `json:"statements"`
			}
			provisioningRaw, err := fs.ReadFile(selected.assets, "provision/manifest.json")
			if err != nil || json.Unmarshal(provisioningRaw, &provisioning) != nil {
				t.Fatal("missing separate provisioning inventory")
			}
			listed["provision/manifest.json"] = true
			for _, entry := range provisioning.Statements {
				raw, err := fs.ReadFile(selected.assets, entry.Path)
				digest := sha256.Sum256(raw)
				if err != nil || hex.EncodeToString(digest[:]) != entry.SHA256 || !strings.HasPrefix(entry.Path, "provision/") || len(raw) > 4096 || listed[entry.Path] {
					t.Fatal("invalid provisioning inventory", entry.Path, err)
				}
				listed[entry.Path] = true
			}
			if value.Bootstrap.ID != "bootstrap.history" || value.Bootstrap.Path != "bootstrap/fathomry_schema_history.sql" || value.Structure.Path != "0001/structure.json" {
				t.Fatal("missing controlled bootstrap or derived structure")
			}
			for _, entry := range []step{value.Bootstrap, {Path: value.Structure.Path, SHA256: value.Structure.SHA256}} {
				raw, err := fs.ReadFile(selected.assets, entry.Path)
				digest := sha256.Sum256(raw)
				if err != nil || hex.EncodeToString(digest[:]) != entry.SHA256 || len(raw) > 1<<20 {
					t.Fatal("bootstrap or derived structure checksum mismatch", entry.Path, err)
				}
				listed[entry.Path] = true
			}
			identities := map[string]bool{}
			var common []string
			tableCount := 0
			for _, entry := range value.Migrations[0].Steps {
				if !fs.ValidPath(entry.Path) || !strings.HasPrefix(entry.Path, "0001/") ||
					path.Ext(entry.Path) != ".sql" || listed[entry.Path] || identities[entry.ID] || entry.ID == "" {
					t.Fatal("invalid/duplicate step", entry.ID)
				}
				listed[entry.Path], identities[entry.ID] = true, true
				raw, err := fs.ReadFile(selected.assets, entry.Path)
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(raw)
				if hex.EncodeToString(digest[:]) != entry.SHA256 {
					t.Fatal("changed SQL asset", entry.Path)
				}
				sql := string(raw)
				if len(raw) > 64<<10 || !strings.Contains(sql, "{{target}}.") ||
					strings.Contains(strings.ReplaceAll(sql, "{{target}}", ""), "{{") ||
					strings.Contains(sql, "IF NOT EXISTS") || strings.Contains(sql, "DELIMITER") ||
					!strings.Contains(sql, "SPDX-License-Identifier: GPL-3.0-or-later") {
					t.Fatal("statement template exceeds its supported shape", entry.Path)
				}
				if strings.HasPrefix(entry.ID, "table.") {
					name := strings.TrimPrefix(entry.ID, "table.")
					if !slices.Contains(catalogTables, name) || entry.Path != "0001/"+name+".sql" ||
						strings.Count(sql, "CREATE TABLE ") != 1 {
						t.Fatal("table does not have one table-named CREATE asset", entry.ID)
					}
					if selected.dialect == "postgresql" &&
						(strings.Count(sql, "$catalog_table$") != 2 || !strings.HasSuffix(strings.TrimSpace(sql), "$catalog_table$;")) {
						t.Fatal("table metadata is not one static DO statement", entry.ID)
					}
					tableCount++
				}
				if !strings.HasPrefix(entry.ID, "function.") && !strings.HasPrefix(entry.ID, "index.") {
					common = append(common, entry.ID)
				}
			}
			if tableCount != len(catalogTables) {
				t.Fatal("catalog table inventory incomplete")
			}
			for _, table := range catalogTables {
				if !identities["table."+table] || !identities["guard."+table] {
					t.Fatal("missing table/immutable guard", table)
				}
			}
			for _, name := range []string{"default.workflow_definition", "default.workflow_version", "default.workflow_mode", "guard.english_locale", "seed.english"} {
				if !identities[name] {
					t.Fatal("missing required statement", name)
				}
			}
			if err := fs.WalkDir(selected.assets, ".", func(name string, entry fs.DirEntry, err error) error {
				if err == nil && !entry.IsDir() && !listed[name] {
					t.Error("unlisted embedded asset", name)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			paths := make([]string, 0, len(listed))
			for name := range listed {
				paths = append(paths, name)
			}
			if err := fstest.TestFS(selected.assets, paths...); err != nil {
				t.Fatal(err)
			}
			if logical == nil {
				logical = common
			} else if !reflect.DeepEqual(logical, common) {
				t.Fatal("dialects disagree on logical tables, guards or order")
			}
		})
	}
}

func TestAssetReadsAreDetached(t *testing.T) {
	for _, assets := range []fs.FS{postgreschema.Assets(), mysqlschema.Assets()} {
		want, err := fs.ReadFile(assets, "manifest.json")
		if err != nil {
			t.Fatal(err)
		}
		var workers sync.WaitGroup
		for range 16 {
			workers.Go(func() {
				raw, err := fs.ReadFile(assets, "manifest.json")
				if err != nil || !bytes.Equal(raw, want) {
					t.Error("read-only asset changed", err)
					return
				}
				clear(raw)
			})
		}
		workers.Wait()
		got, err := fs.ReadFile(assets, "manifest.json")
		if err != nil || !bytes.Equal(want, got) {
			t.Fatal("caller mutation reached embedded storage", err)
		}
		if _, err := assets.Open("../manifest.json"); err == nil {
			t.Fatal("parent traversal admitted")
		}
		if _, err := assets.Open("assets.go"); err == nil {
			t.Fatal("source code exposed as schema asset")
		}
	}
}

func sourceComments(t *testing.T, assets fs.FS, dialect string, files ...string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	columnPattern := regexp.MustCompile("(?m)^\\s+\x60?([a-z_][a-z0-9_]*)\x60? (?:BIGINT|TIMESTAMPTZ|DATETIME|TEXT|VARBINARY|VARCHAR|BOOLEAN|INTEGER|BYTEA|LONGBLOB)\\b")
	pgPattern := regexp.MustCompile(`(?m)^\s*COMMENT ON (TABLE|COLUMN) \{\{target\}\}\.([a-z_][a-z0-9_]*)(?:\.([a-z_][a-z0-9_]*))? IS '((?:[^']|'')*)';$`)
	mysqlPattern := regexp.MustCompile(`COMMENT(?: =)? '((?:[^']|'')*)'`)
	if len(files) == 0 {
		for _, table := range catalogTables {
			files = append(files, "0001/"+table+".sql")
		}
	}
	for _, file := range files {
		table := strings.TrimSuffix(path.Base(file), ".sql")
		raw, err := fs.ReadFile(assets, file)
		if err != nil {
			t.Fatal(err)
		}
		sql := string(raw)
		expected := map[string]bool{table: true}
		for _, column := range columnPattern.FindAllStringSubmatch(sql, -1) {
			expected[table+"."+column[1]] = true
		}
		if len(expected) < 4 {
			t.Fatal("column inventory was not read", table)
		}
		found := map[string]string{}
		add := func(key, value string) {
			if _, exists := found[key]; exists || !expected[key] {
				t.Fatal("duplicate or unknown comment", key)
			}
			value = strings.ReplaceAll(value, "''", "'")
			parts := strings.Split(value, "; zh-CN: ")
			if len(parts) != 2 || !strings.HasPrefix(parts[0], "en: ") ||
				strings.TrimSpace(strings.TrimPrefix(parts[0], "en: ")) == "" ||
				strings.TrimSpace(parts[1]) == "" ||
				!strings.ContainsFunc(parts[1], func(character rune) bool { return unicode.Is(unicode.Han, character) }) ||
				utf8.RuneCountInString(value) > 1024 {
				t.Fatal("incomplete or oversized combined comment", key)
			}
			found[key] = value
		}
		if dialect == "postgresql" {
			for _, comment := range pgPattern.FindAllStringSubmatch(sql, -1) {
				key := comment[2]
				if comment[1] == "COLUMN" {
					key += "." + comment[3]
				}
				add(key, comment[4])
			}
		} else {
			for _, line := range strings.Split(sql, "\n") {
				match := mysqlPattern.FindStringSubmatch(line)
				if match == nil {
					continue
				}
				key := table
				if !strings.HasPrefix(strings.TrimSpace(line), ") ENGINE") {
					key += "." + strings.Trim(strings.Fields(line)[0], "\x60")
				}
				add(key, match[1])
			}
		}
		if len(found) != len(expected) {
			t.Fatal("table/column documentation incomplete", table, len(found), len(expected))
		}
		for key, value := range found {
			result[key] = value
		}
	}
	return result
}

func TestCommentCoverageAndParity(t *testing.T) {
	postgres := sourceComments(t, postgreschema.Assets(), "postgresql")
	mysql := sourceComments(t, mysqlschema.Assets(), "mysql")
	if len(postgres) != 83 || !reflect.DeepEqual(postgres, mysql) {
		t.Fatal("expected equal combined documentation for 9 tables and 74 columns")
	}
}

func TestHistoryCommentCoverageAndParity(t *testing.T) {
	postgres := sourceComments(t, postgreschema.Assets(), "postgresql", "bootstrap/fathomry_schema_history.sql")
	mysql := sourceComments(t, mysqlschema.Assets(), "mysql", "bootstrap/fathomry_schema_history.sql")
	if len(postgres) != 10 || !reflect.DeepEqual(postgres, mysql) {
		t.Fatal("expected equal en/zh-CN documentation for the journal and 9 columns")
	}
}

func TestAssetDependencies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "list", "-mod=readonly", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./postgresql/v1", "./mysql/v1")
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatal("dependency inspection failed", err, string(raw))
	}
	got := strings.Fields(string(raw))
	slices.Sort(got)
	want := []string{
		"github.com/frost-leo/fathomry/schema/fathomry/mysql/v1",
		"github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("pure asset packages acquired runtime dependencies", got)
	}
	command = exec.CommandContext(ctx, "go", "list", "-mod=readonly", "-f", "{{len .GoFiles}} {{len .CgoFiles}}", ".")
	raw, err = command.CombinedOutput()
	if err != nil || strings.TrimSpace(string(raw)) != "0 0" {
		t.Fatal("dialect test harness became a grouping-level production package", err)
	}
}

func TestInspectionQueriesAreDetachedReadOnlyData(t *testing.T) {
	postgres, err := postgreschema.InspectionQueries()
	if err != nil || len(postgres) != 10 {
		t.Fatal("PostgreSQL inspection profile unavailable", err)
	}
	mysql, err := mysqlschema.InspectionQueries()
	if err != nil || len(mysql) != 3 {
		t.Fatal("MySQL inspection profile unavailable", err)
	}
	check := func(kind, query string) {
		t.Helper()
		if kind == "" || !strings.HasPrefix(query, "SELECT ") || !strings.Contains(query, "LIMIT 513") {
			t.Fatal("unbounded or non-read inspection statement")
		}
	}
	for _, query := range postgres {
		check(query.Kind, query.SQL)
	}
	for _, query := range mysql {
		check(query.Kind, query.SQL)
	}
	postgres[0].Kind, postgres[0].SQL = "mutated", "DELETE FROM anything"
	mysql[0].Kind, mysql[0].SQL = "mutated", "DELETE FROM anything"
	pgAgain, err := postgreschema.InspectionQueries()
	if err != nil || pgAgain[0].Kind == "mutated" {
		t.Fatal("PostgreSQL query slice leaked mutable state", err)
	}
	myAgain, err := mysqlschema.InspectionQueries()
	if err != nil || myAgain[0].Kind == "mutated" {
		t.Fatal("MySQL query slice leaked mutable state", err)
	}
}
