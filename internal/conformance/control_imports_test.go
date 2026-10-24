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

package conformance_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func cliControlUseAllowed(relative, imported string, item *ast.ImportSpec, file *ast.File) bool {
	if item.Name == nil || item.Name.Name == "." || item.Name.Name == "_" {
		return false
	}
	var allowed []string
	switch imported {
	case "github.com/frost-leo/fathomry/framework/v1":
		if relative == "v1/control.go" || relative == "v1/control_test.go" || relative == "internal/command/schema/service_test.go" {
			allowed = []string{"New", "Options", "StartReceiver", "ReceiverOptions"}
		}
	case "github.com/frost-leo/fathomry/framework/configuration/v1":
		switch relative {
		case "v1/control.go":
			allowed = []string{"Encoding", "YAML", "JSON", "TOML", "Viper", "ViperOptions", "File", "Base", "Load", "Declaration", "Schema", "Dependencies"}
		case "v1/database.go", "v1/database_test.go":
			allowed = []string{"Record"}
		}
	case "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema":
		switch relative {
		case "v1/cli.go":
			allowed = []string{"Components", "New", "Runner", "Status", "Plan", "Change"}
		case "v1/control.go":
			allowed = []string{"Runner", "WorkBytes", "PostgreSQL", "MySQL"}
		case "v1/control_test.go":
			allowed = []string{"Runner"}
		case "internal/command/schema/status/status.go", "internal/command/schema/status/status_test.go",
			"internal/command/schema/plan/plan.go", "internal/command/schema/plan/plan_test.go",
			"internal/command/schema/apply/apply.go", "internal/command/schema/apply/apply_test.go":
			allowed = []string{"Status", "Plan", "Change"}
		case "v1/creation_service_test.go", "v1/creation_fault_service_test.go":
			allowed = []string{"Runner", "WorkBytes", "PostgreSQL", "MySQL", "NotInstalled", "Applied"}

		}
	case "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database":
		switch relative {
		case "v1/cli.go":
			allowed = []string{"Components", "New"}
		case "v1/database.go", "v1/database_test.go":
			allowed = []string{"Options", "ValidateTarget", "Definitions", "ErrInput", "ErrConfiguration", "ErrCanceled", "ErrLimit", "ErrSerialization"}
		case "v1/control.go", "v1/control_test.go":
			allowed = []string{"Options", "ErrConfiguration", "ErrLimit", "ErrCleanup"}
		case "internal/command/schema/runner.go", "internal/command/schema/connection.go", "internal/command/schema/diagnostics.go",
			"internal/command/schema/manifest.go", "internal/command/schema/inspection.go", "internal/command/schema/journal.go",
			"internal/command/schema/profile.go", "internal/command/schema/schema_test.go", "internal/command/schema/service_test.go",
			"internal/command/schema/state_service_test.go", "internal/command/schema/fault_service_test.go",
			"internal/command/database/create/creation.go", "internal/command/database/create/creation_assets.go",
			"internal/command/database/create/creation_postgres.go", "internal/command/database/create/creation_mysql.go",
			"internal/command/database/create/creation_test.go", "v1/creation_service_test.go", "v1/creation_fault_service_test.go":
			allowed = []string{"Options", "ValidateTarget", "Fail", "ErrInput", "ErrConfiguration", "ErrClosed", "ErrBusy", "ErrLimit", "ErrAssets", "ErrProfile", "ErrAccess", "ErrSchema", "ErrDatabase", "ErrCleanup", "ErrCanceled", "ErrSerialization", "ErrDatabaseExists"}
		}
	case "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create":
		switch relative {
		case "v1/cli.go":
			allowed = []string{"Creator", "DatabaseChange", "New", "Component"}
		case "v1/control.go":
			allowed = []string{"Creator", "WorkBytes", "PostgreSQLCreation", "MySQLCreation"}
		case "v1/database.go":
			allowed = []string{"CreateOptions", "ValidateCreation"}
		case "v1/control_test.go":
			allowed = []string{"Creator"}
		case "v1/creation_service_test.go", "v1/creation_fault_service_test.go":
			allowed = []string{"Creator", "WorkBytes", "DatabaseChange", "PostgreSQLCreation", "MySQLCreation", "DatabaseCreated", "DatabaseUnchanged", "DatabaseUnknown", "DatabaseNotApplicable", "DatabaseNotStarted"}
		}
	}
	if allowed == nil {
		return false
	}
	valid := true
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		owner, ok := selector.X.(*ast.Ident)
		if ok && owner.Name == item.Name.Name && !slices.Contains(allowed, selector.Sel.Name) {
			valid = false
		}
		return true
	})
	return valid
}

func TestControlCLIImportBoundary(t *testing.T) {
	const schema = "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema"
	for _, test := range []struct {
		name, path, imported, alias, expression string
		allowed                                 bool
	}{
		{"entry_result", "v1/cli.go", schema, "schema", "schema.Status{}", true},
		{"entry_cannot_construct", "v1/cli.go", schema, "schema", "schema.PostgreSQL(nil,\"\",options)", false},
		{"composition", "v1/control.go", schema, "schema", "schema.PostgreSQL(nil,\"\",options)", true},
		{"shared_runtime", "v1/control.go", "github.com/frost-leo/fathomry/framework/v1", "framework", "framework.New(nil,framework.Options{})", true},
		{"leaf_data", "internal/command/schema/apply/apply.go", schema, "schema", "schema.Change{}", true},
		{"leaf_no_ownership", "internal/command/schema/apply/apply.go", schema, "schema", "schema.Runner{}", false},
		{"leaf_no_construction", "internal/command/schema/apply/apply.go", schema, "schema", "schema.MySQL(nil,\"\",schema.Options{})", false},
		{"leaf_no_runtime", "internal/command/schema/apply/apply.go", "github.com/frost-leo/fathomry/framework/v1", "framework", "framework.New(nil,framework.Options{})", false},
		{"leaf_no_configuration", "internal/command/database/create/create.go", "github.com/frost-leo/fathomry/framework/configuration/v1", "configuration", "configuration.Load(nil,nil,nil)", false},
		{"creation_no_schema_executor", "internal/command/database/create/creation.go", schema, "schema", "schema.Runner{}", false},
		{"configuration_no_schema_contracts", "v1/database.go", schema, "schema", "schema.Options{}", false},
		{"creation_common_contracts", "internal/command/database/create/creation.go", "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database", "databasecmd", "databasecmd.Options{}", true},
		{"common_owner_no_leaf", "internal/command/database/database.go", "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create", "createcmd", "createcmd.Creator{}", false},
		{"schema_no_catalog_dependency", "v1/control.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.Open(nil,nil)", false},
		{"unapproved_leaf", "internal/command/schema/repair/repair.go", schema, "schema", "schema.Change{}", false},
		{"dot_import", "v1/control.go", schema, ".", "Options{}", false},
		{"blank_import", "v1/control.go", schema, "_", "0", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "probe.go", "package probe\nimport "+test.alias+" "+strconv.Quote(test.imported)+"\nvar _ = "+test.expression+"\n", 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := cliControlUseAllowed(test.path, test.imported, file.Imports[0], file); got != test.allowed {
				t.Fatal("incorrect control import boundary", got)
			}
		})
	}
}

func TestControlHasNoParallelDatabaseOwner(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"database", "cmd/fathomry/internal/database", "framework/schema"} {
		if _, err := os.Stat(filepath.Join(root, removed)); !os.IsNotExist(err) {
			t.Errorf("removed management layer exists: %s", removed)
		}
	}
	for _, relative := range []string{"cmd/fathomry/internal/command/schema", "cmd/fathomry/internal/command/database"} {
		err := filepath.WalkDir(filepath.Join(root, relative), func(path string, entry fs.DirEntry, problem error) error {
			if problem != nil {
				return problem
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			aliases := map[string]string{}
			for _, item := range file.Imports {
				imported, _ := strconv.Unquote(item.Path.Value)
				if strings.HasPrefix(imported, "github.com/frost-leo/fathomry/internal/") || strings.HasPrefix(imported, "github.com/frost-leo/fathomry/framework/workflow/") ||
					imported == "github.com/frost-leo/fathomry/framework/v1" || imported == "github.com/frost-leo/fathomry/framework/configuration/v1" ||
					imported == "github.com/frost-leo/fathomry/resource/v1" || imported == "database/sql" {
					t.Errorf("operation layer acquired composition dependency: %s: %s", path, imported)
				}
				if item.Name != nil {
					aliases[item.Name.Name] = imported
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				owner, ok := selector.X.(*ast.Ident)
				if !ok || !strings.HasPrefix(aliases[owner.Name], "github.com/frost-leo/fathomry/adapters/database/") {
					return true
				}
				if slices.Contains([]string{"Open", "Owner", "Using", "Handle"}, selector.Sel.Name) {
					t.Errorf("operation layer owns a database source: %s: %s", path, selector.Sel.Name)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestControlSharedContractsHaveNoExecutorDependency(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"cmd/fathomry/internal/command/database", "cmd/fathomry/internal/command/database/create"} {
		entries, err := os.ReadDir(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, relative, entry.Name())
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range file.Imports {
				imported, _ := strconv.Unquote(item.Path.Value)
				if imported == "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema" ||
					relative == "cmd/fathomry/internal/command/database" && (strings.Contains(imported, "/command/database/create") || strings.Contains(imported, "/adapters/database/") || strings.Contains(imported, "/schema/fathomry/")) {
					t.Errorf("shared contract or creation depends on an executor: %s: %s", path, imported)
				}
			}
		}
	}
}
