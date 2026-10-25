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

// The exception is a concrete offline admission capability at a CLI boundary,
// not permission for arbitrary Framework scenarios or a future version.
func cliWorkflowDeclarationUseAllowed(relative, imported string, item *ast.ImportSpec, file *ast.File) bool {
	const workflow = "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	testFile := filepath.ToSlash(filepath.Dir(relative)) == "internal/command/workflow/vet" && strings.HasSuffix(relative, "_test.go")
	buildTest := relative == "internal/command/project/build/inspection_test.go" || relative == "internal/command/project/build/implementation_inspection_test.go"
	entryTest := relative == "v1/cli_test.go"
	projectTest := relative == "internal/command/workflow/workflow_test.go" || relative == "internal/command/workflow/implementations_test.go"
	publicationTest := relative == "internal/command/workflow/publication_test.go"
	var allowed []string
	name := ""
	switch {
	case imported == workflow+"/testdata/health" && (testFile || buildTest || entryTest || projectTest):
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
			if entryTest {
				allowed = append(allowed, "Decimal")
			}
		} else if relative == "internal/command/workflow/publication.go" {
			allowed = []string{"Diagnostic", "MaxInputs"}
		} else if relative == "internal/command/workflow/comparison.go" {
			allowed = []string{"Diagnostic", "MaxInputs"}
		} else if relative == "internal/command/workflow/workflow.go" || projectTest || publicationTest {
			allowed = []string{"Declaration", "WorkflowDeclaration", "ModeDeclaration", "Identity", "Report", "Bundle", "Entry", "Conflict", "InputDiagnostic", "Compile", "Check", "Diagnose", "MaxInputs", "MaxInputBytes", "MaxWorkflows", "MaxModes", "MaxDiagnostics"}
			if projectTest || publicationTest {
				allowed = append(allowed, "Decimal", "Fixed", "DataSchema", "ErrLimit")
			}
		} else if relative == "internal/command/project/build/build.go" {
			allowed = []string{"Entry", "Report"}
		} else if relative == "internal/command/workflow/implementations.go" {
			allowed = []string{"Identity", "Bundle", "Declaration", "WorkflowDeclaration", "MaxWorkflows"}
		} else if relative == "internal/command/project/build/implementation_inspection.go" {
			allowed = []string{"MaxInputBytes", "MaxInputs", "MaxWorkflows", "Report", "Identity", "Bundle"}
		} else if relative == "internal/command/project/build/inspection.go" || buildTest {
			allowed = []string{"Declaration", "MaxInputs", "MaxInputBytes", "MaxBundleBytes", "Compile", "Parse", "Check", "Report", "Bundle", "Entry", "Conflict", "InputDiagnostic"}
			if buildTest {
				allowed = append(allowed, "Decimal", "MaxWorkflows")
			}
		} else {
			if relative != "internal/command/workflow/vet/vet.go" && !testFile {
				return false
			}
			allowed = []string{"Report", "Bundle", "Entry", "Identity", "Conflict", "InputDiagnostic", "MaxInputs", "MaxBundleBytes", "MaxInputBytes", "Parse", "Check", "Diagnose"}
			if testFile {
				allowed = append(allowed, "Compile", "Component", "Decimal")
			}
		}
	case imported == "github.com/frost-leo/fathomry/framework/workflow/implementation/v1":
		if item.Name == nil {
			return false
		}
		switch relative {
		case "v1/cli.go":
			allowed = []string{"Component"}
		case "internal/command/workflow/workflow.go":
			allowed = []string{"VersionBindings"}
		case "internal/command/workflow/implementations.go":
			allowed = []string{"Binding", "VersionBindings", "Diagnostic", "Manifest", "Describe", "Prepare", "Diagnose"}
		case "internal/command/project/build/implementation_inspection.go":
			allowed = []string{"MaxManifestBytes", "ErrLimit", "Describe", "ValidateManifest"}
		case "internal/command/workflow/implementations_test.go", "internal/command/project/build/implementation_inspection_test.go":
			allowed = []string{"Activity", "Binding", "VersionBindings", "Callable", "MaxManifestBytes", "ErrInvalid", "ErrUnsupported"}
		default:
			return false
		}
	case imported == "github.com/frost-leo/fathomry/framework/workflow/catalog/v1":
		if item.Name == nil {
			return false
		}
		name = item.Name.Name
		switch relative {
		case "v1/cli.go":
			allowed = []string{"Component", "ListRequest", "Page", "ReadRequest", "View"}
		case "v1/control.go":
			allowed = []string{"Catalog", "ErrLimit", "Options", "PostgreSQL", "MySQL", "WorkBytes", "ListRequest", "Page", "ReadRequest", "View", "ValidateList", "ValidateRead"}
		case "internal/command/workflow/list/list.go", "internal/command/workflow/list/list_test.go":
			allowed = []string{"ListRequest", "Page"}
		case "internal/command/workflow/show/show.go", "internal/command/workflow/show/show_test.go":
			allowed = []string{"ReadRequest", "View"}
		case "v1/cli_test.go", "internal/command/workflow/workflow_test.go", "internal/command/project/build/inspection_test.go":
			allowed = []string{"Metadata", "Resource", "Target", "Definition", "Version", "Mode", "Node", "Name", "Description", "Text", "MaxResources"}
		case "internal/command/workflow/workflow.go":
			allowed = []string{"Metadata", "Diagnostic", "PreparedRegistration", "Prepare", "Registration", "Diagnose", "Definition", "Mode", "ModeRevision", "MaxResources", "MaxTags"}
		case "internal/command/workflow/publication.go":
			allowed = []string{"Diagnostic", "Publication", "PreparedRegistration", "Applied", "Unchanged", "Unknown"}
		case "internal/command/workflow/comparison.go":
			allowed = []string{"Diagnostic", "Comparison", "PreparedRegistration"}
		case "internal/command/workflow/publication_test.go", "internal/command/workflow/comparison_test.go":
			allowed = []string{"PreparedRegistration", "Publication", "Comparison", "Effect", "Applied", "Unchanged", "NotStarted", "RolledBack", "Unknown", "Prepare", "Registration", "Diagnose"}
		default:
			return false
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
		{"binding_check", "internal/command/workflow/implementations.go", "github.com/frost-leo/fathomry/framework/workflow/implementation/v1", "impl", "impl.Prepare(nil,nil,struct{}{},nil)", true},
		{"binding_no_execution", "internal/command/workflow/implementations.go", "github.com/frost-leo/fathomry/framework/workflow/implementation/v1", "impl", "impl.StartWorker(nil)", false},
		{"binding_no_leaf_preparation", "internal/command/workflow/vet/vet.go", "github.com/frost-leo/fathomry/framework/workflow/implementation/v1", "impl", "impl.Prepare(nil,nil,struct{}{},nil)", false},
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
		{"group_test", "internal/command/workflow/integration_test.go", workflow, "wf", "wf.Compile(nil,nil)", false},
		{"other_workflow_command", "internal/command/workflow/run/run.go", workflow, "wf", "wf.Parse(nil,nil)", false},
		{"other_workflow_test", "internal/command/workflow/run/run_test.go", workflow, "wf", "wf.Compile(nil,nil)", false},
		{"other_command", "internal/command/project/project.go", workflow, "wf", "wf.Parse(nil,nil)", false},
		{"metadata_execution", "internal/app/catalog.go", workflow, "wf", "wf.Parse(nil,nil)", false},
		{"fixture_in_production", "internal/command/workflow/vet/vet.go", workflow + "/testdata/health", "health", "health.Declarations()", false},
		{"dot_import", "internal/app/catalog.go", workflow, ".", "Parse(nil,nil)", false},
		{"blank_import", "internal/app/catalog.go", workflow, "_", "0", false},
		{"unknown_selector", "internal/command/workflow/vet/vet.go", workflow, "wf", "wf.StartWorker(nil)", false},
		{"shared_analysis_prepares", "internal/command/workflow/workflow.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.Prepare(nil,catalog.Registration{})", true},
		{"publication_does_not_prepare_again", "internal/command/workflow/publication.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.Prepare(nil,catalog.Registration{})", false},
		{"publication_does_not_compile_again", "internal/command/workflow/publication.go", workflow, "wf", "wf.Compile(nil,nil)", false},
		{"composition_has_no_batch_policy", "v1/control.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.Applied", false},
		{"publication_cannot_open", "internal/command/workflow/publication.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.PostgreSQL(nil,\"\",catalog.Options{})", false},
		{"register_leaf_cannot_open", "internal/command/workflow/register/register.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.PostgreSQL(nil,\"\",catalog.Options{})", false},
		{"entry_catalog_composition", "v1/control.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.PostgreSQL(nil,\"\",catalog.Options{})", true},
		{"entry_no_catalog_composition", "v1/cli.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.PostgreSQL(nil,\"\",catalog.Options{})", false},
		{"list_leaf_data", "internal/command/workflow/list/list.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.ListRequest{}", true},
		{"list_leaf_no_runtime", "internal/command/workflow/list/list.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.PostgreSQL(nil,\"\",catalog.Options{})", false},
		{"show_leaf_data", "internal/command/workflow/show/show.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.ReadRequest{}", true},
		{"comparison_data", "internal/command/workflow/comparison.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.Comparison{}", true},
		{"comparison_no_owner", "internal/command/workflow/comparison.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.PostgreSQL(nil,\"\",catalog.Options{})", false},
		{"compare_leaf_no_owner", "internal/command/workflow/compare/compare.go", "github.com/frost-leo/fathomry/framework/workflow/catalog/v1", "catalog", "catalog.PostgreSQL(nil,\"\",catalog.Options{})", false},
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
