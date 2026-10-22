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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The exception is a concrete data-only capability at a concrete CLI boundary,
// not permission for arbitrary Framework scenarios or a future version.
func cliWorkflowDeclarationUseAllowed(relative, imported string, item *ast.ImportSpec, file *ast.File) bool {
	const workflow = "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	testFile := filepath.ToSlash(filepath.Dir(relative)) == "internal/command/workflow/vet" && strings.HasSuffix(relative, "_test.go")
	buildTest := relative == "internal/command/project/build/inspection_test.go"
	entryTest := relative == "v1/cli_test.go"
	var allowed []string
	name := ""
	switch {
	case imported == workflow+"/testdata/health" && (testFile || buildTest || entryTest):
		name = "health"
		allowed = []string{"Declarations"}
	case imported == workflow:
		if item.Name == nil {
			return false
		}
		if relative == "internal/app/catalog.go" {
			allowed = []string{"Component"}
		} else if relative == "v1/cli.go" || entryTest {
			allowed = []string{"Declaration", "Component"}
		} else if relative == "internal/command/project/build/build.go" {
			allowed = []string{"Entry"}
		} else if relative == "internal/command/project/build/inspection.go" || buildTest {
			allowed = []string{"Declaration", "MaxInputs", "MaxInputBytes", "MaxBundleBytes", "Compile", "Parse", "Check", "Report", "Bundle", "Entry", "Conflict", "InputDiagnostic"}
			if buildTest {
				allowed = append(allowed, "Decimal")
			}
		} else {
			if relative != "internal/command/workflow/vet/vet.go" && !testFile {
				return false
			}
			allowed = []string{"Report", "Bundle", "Entry", "Conflict", "InputDiagnostic", "MaxInputs", "MaxBundleBytes", "MaxInputBytes", "Parse", "Check", "Diagnose"}
			if testFile {
				allowed = append(allowed, "Compile", "Component", "Decimal")
			}
		}
	default:
		return false
	}
	if item.Name != nil {
		name = item.Name.Name
	}
	if name == "" || name == "." || name == "_" {
		return false
	}
	valid := true
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		owner, ok := selector.X.(*ast.Ident)
		if ok && owner.Name == name && !slices.Contains(allowed, selector.Sel.Name) {
			valid = false
		}
		return true
	})
	return valid
}

func TestWorkflowCLIImportException(t *testing.T) {
	const workflow = "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	for _, test := range []struct {
		name, path, imported, alias, expression string
		allowed                                 bool
	}{
		{"parse", "internal/command/workflow/vet/vet.go", workflow, "wf", "wf.Parse(nil,nil)", true},
		{"check", "internal/command/workflow/vet/vet.go", workflow, "wf", "wf.Check(nil)", true},
		{"diagnostic", "internal/command/workflow/vet/vet.go", workflow, "wf", "wf.Diagnose(nil)", true},
		{"metadata", "internal/app/catalog.go", workflow, "wf", "wf.Component()", true},
		{"fixture_compile", "internal/command/workflow/vet/vet_test.go", workflow, "wf", "wf.Compile(nil,wf.Report{})", true},
		{"fixture_data", "internal/command/workflow/vet/vet_test.go", workflow + "/testdata/health", "health", "health.Declarations()", true},
		{"production_compile", "internal/command/workflow/vet/vet.go", workflow, "wf", "wf.Compile(nil,nil)", false},
		{"runtime", "internal/command/workflow/vet/vet.go", "github.com/frost-leo/fathomry/framework/v1", "wf", "wf.New(nil,wf.Options{})", false},
		{"configuration", "internal/command/workflow/vet/vet.go", "github.com/frost-leo/fathomry/framework/configuration/v1", "wf", "wf.Load(nil,nil)", false},
		{"future_version", "internal/command/workflow/vet/vet.go", "github.com/frost-leo/fathomry/framework/workflow/declaration/v2", "wf", "wf.Parse(nil,nil)", false},
		{"former_package", "internal/command/workflow/vet/vet.go", "github.com/frost-leo/fathomry/framework/workflow/v1", "wf", "wf.Parse(nil,nil)", false},
		{"group_execution", "internal/command/workflow/workflow.go", workflow, "wf", "wf.Parse(nil,nil)", false},
		{"group_test", "internal/command/workflow/workflow_test.go", workflow, "wf", "wf.Compile(nil,nil)", false},
		{"other_workflow_command", "internal/command/workflow/run/run.go", workflow, "wf", "wf.Parse(nil,nil)", false},
		{"other_workflow_test", "internal/command/workflow/run/run_test.go", workflow, "wf", "wf.Compile(nil,nil)", false},
		{"other_command", "internal/command/project/project.go", workflow, "wf", "wf.Parse(nil,nil)", false},
		{"metadata_execution", "internal/app/catalog.go", workflow, "wf", "wf.Parse(nil,nil)", false},
		{"fixture_in_production", "internal/command/workflow/vet/vet.go", workflow + "/testdata/health", "health", "health.Declarations()", false},
		{"dot_import", "internal/app/catalog.go", workflow, ".", "Parse(nil,nil)", false},
		{"blank_import", "internal/app/catalog.go", workflow, "_", "0", false},
		{"unknown_selector", "internal/command/workflow/vet/vet.go", workflow, "wf", "wf.StartWorker(nil)", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := "package probe\nimport " + test.alias + " " + strconv.Quote(test.imported) + "\nvar _ = " + test.expression + "\n"
			file, err := parser.ParseFile(token.NewFileSet(), "probe.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := cliWorkflowDeclarationUseAllowed(test.path, test.imported, file.Imports[0], file); got != test.allowed {
				t.Fatal("incorrect import boundary", got)
			}
		})
	}
}

func workflowFoundationImport(imported string) bool {
	if imported == "github.com/frost-leo/fathomry/failure/v1" || imported == "github.com/frost-leo/fathomry/i18n/v1" {
		return true
	}
	first, _, _ := strings.Cut(imported, "/")
	return imported != "C" && !strings.Contains(first, ".")
}

func TestWorkflowDeclarationHasOnlyDataFoundations(t *testing.T) {
	root := filepath.Join("..", "..", "framework", "workflow", "declaration", "v1")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(root, entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range file.Imports {
			imported, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if !workflowFoundationImport(imported) {
				t.Errorf("workflow declaration depends on a runtime capability: %s: %s", path, imported)
			}
		}
	}
	for _, forbidden := range []string{"internal/fault", "adapters/v1", "framework/v1", "framework/configuration/v1", "settings/v1", "resource/v1"} {
		if workflowFoundationImport("github.com/frost-leo/fathomry/" + forbidden) {
			t.Fatal("foundation negative control admitted", forbidden)
		}
	}
	for _, forbidden := range []string{"C", "go.temporal.io/sdk/workflow", "github.com/example/sdk"} {
		if workflowFoundationImport(forbidden) {
			t.Fatal("non-foundation dependency admitted", forbidden)
		}
	}
}
