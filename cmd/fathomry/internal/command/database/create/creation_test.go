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

package create

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	"github.com/frost-leo/fathomry/failure/v1"
)

func TestCreatorPreservesDatabaseFailureOccurrence(t *testing.T) {
	runner, err := PostgreSQLCreation(new(postgres.Client), new(postgres.Client), CreateOptions{Dialect: "postgresql", Database: "test_control", Schema: "control", Owner: "test_owner"})
	if err != nil {
		t.Fatal(err)
	}
	change, err := runner.Create(context.Background())
	core, known := failure.Inspect(err)
	if !known || core.Diagnostic().Definition.Code != databasecmd.ErrDatabase || !errors.Is(err, postgres.ErrInput) || !change.CleanupComplete || !runner.Released() {
		t.Fatal("cleanup without a failure erased the operation occurrence", err)
	}
	if core.Diagnostic().Location.Operation != "database.create" {
		t.Fatal("creation inherited another command's operation", core.Diagnostic().Location.Operation)
	}
}

func TestCreationAssetsAndIdentifiers(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		options := CreateOptions{Dialect: dialect, Database: "db`{{owner}}"}
		if dialect == "postgresql" {
			options.Database = "db\"{{owner}}"
			options.Schema = "control"
			options.Owner = "role\"name"
		}
		value, sql, err := normalizeCreation(options)
		if err != nil || value.Timeout != 45*time.Second || value.CleanupTimeout != 5*time.Second {
			t.Fatal("valid creation rejected", err)
		}
		if !strings.Contains(sql.database, "{{owner}}") {
			t.Fatal("replacement interpreted an inserted identifier token")
		}
		if strings.Contains(sql.database, "IF NOT EXISTS") || strings.Contains(sql.database, "DROP ") || strings.Contains(sql.database, "GRANT ") {
			t.Fatal("creation gained repair authority")
		}
	}
	for _, test := range []struct{ dialect, name string }{
		{"postgresql", "postgres"}, {"postgresql", "template0"}, {"postgresql", "template1"}, {"postgresql", strings.Repeat("a", 64)},
		{"mysql", "mysql"}, {"mysql", "INFORMATION_SCHEMA"}, {"mysql", "db.name"}, {"mysql", "dir/name"}, {"mysql", "trailing "}, {"mysql", "a\x00"},
	} {
		if _, err := quoteDatabase(test.dialect, test.name); !errors.Is(err, databasecmd.ErrInput) {
			t.Fatal("unsafe target admitted", test)
		}
	}
	quoted, err := quotePG(`owner"; SELECT 1;--`)
	if err != nil || quoted != `"owner""; SELECT 1;--"` {
		t.Fatal("owner identifier escaped quoting", err)
	}
}
func TestDatabaseDuplicateClassification(t *testing.T) {
	if !databaseExists("42P04", "", "", "") || !databaseExists("23505", "pg_catalog", "pg_database", "pg_database_datname_index") {
		t.Fatal("known duplicate not classified")
	}
	for _, test := range [][4]string{
		{"23505", "", "", "pg_database_datname_index"},
		{"23505", "public", "pg_database", "pg_database_datname_index"},
		{"23505", "pg_catalog", "pg_database", "pg_database_oid_index"},
		{"42501", "pg_catalog", "pg_database", "pg_database_datname_index"},
	} {
		if databaseExists(test[0], test[1], test[2], test[3]) {
			t.Fatal("unrelated server failure classified as an existing target")
		}
	}
}
func FuzzCreationIdentifier(f *testing.F) {
	for _, value := range []string{"simple", `a"; SELECT 1;--`, "{{owner}}", "a`b", "数据库"} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 1024 {
			return
		}
		if quoted, err := quotePG(value); err == nil {
			if len(value) > 63 || !strings.HasPrefix(quoted, "\"") || !strings.HasSuffix(quoted, "\"") {
				t.Fatal("invalid PostgreSQL quote")
			}
			if strings.ReplaceAll(quoted[1:len(quoted)-1], "\"\"", "\"") != value {
				t.Fatal("PostgreSQL quote changed identity")
			}
		}
	})
}
