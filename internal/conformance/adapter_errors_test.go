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
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var publicDataAdapters = []string{"database/postgres", "database/mysql", "objectstore/minio", "broker/kafka", "cache/redis", "sqlengine/duckdb", "sqlengine/trino", "sqlengine/doris", "httpclient/nethttp", "httpclient/tlsclient"}

// These files are offline metadata boundaries. Native translation and runtime
// diagnostics stay separately reviewable without changing public package APIs.
func TestPublicAdapterErrorResponsibilities(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for path, profile := range adapterPackageProfiles {
		if !profile.catalog {
			continue
		}
		t.Run(path, func(t *testing.T) {
			directory := filepath.Join(root, "adapters", path)
			for _, name := range []string{"definitions.go", "error.go", "diagnostics.go", "resources.go"} {
				syntax, err := parser.ParseFile(token.NewFileSet(), filepath.Join(directory, name), nil, parser.ParseComments)
				if err != nil {
					t.Fatal(err)
				}
				if name == "definitions.go" || name == "resources.go" {
					allowed := map[string]bool{"github.com/frost-leo/fathomry/failure/v1": true}
					if name == "resources.go" {
						allowed = map[string]bool{"embed": true, "io/fs": true}
					}
					for _, item := range syntax.Imports {
						path, err := strconv.Unquote(item.Path.Value)
						if err != nil || !allowed[path] {
							t.Fatalf("%s acquired runtime/native dependency %s", name, item.Path.Value)
						}
					}
				}
				if name == "error.go" {
					for _, decl := range syntax.Decls {
						switch decl := decl.(type) {
						case *ast.FuncDecl:
							if decl.Name.Name == "Definitions" || decl.Name.Name == "Resources" || decl.Name.Name == "CacheDefinitions" || decl.Name.Name == "MessagingDefinitions" {
								t.Fatal("offline declarations returned to runtime translation file")
							}
						case *ast.GenDecl:
							if stableErrorCodes(syntax, decl) {
								t.Fatal("stable code declarations returned to runtime translation file")
							}
						}
					}
				}
			}
		})
	}
}

func stableErrorCodes(syntax *ast.File, declaration *ast.GenDecl) bool {
	if declaration.Tok != token.CONST {
		return false
	}
	aliases := map[string]bool{}
	for _, item := range syntax.Imports {
		path, err := strconv.Unquote(item.Path.Value)
		if err == nil && path == "github.com/frost-leo/fathomry/failure/v1" {
			name := "failure"
			if item.Name != nil {
				name = item.Name.Name
			}
			aliases[name] = true
		}
	}
	for _, specification := range declaration.Specs {
		value, ok := specification.(*ast.ValueSpec)
		if !ok {
			continue
		}
		for _, name := range value.Names {
			if name.IsExported() && strings.HasPrefix(name.Name, "Err") &&
				(len(name.Name) == 3 || name.Name[3] >= 'A' && name.Name[3] <= 'Z') {
				return true
			}
		}
		found := false
		ast.Inspect(value, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "Code" {
				if name, ok := selector.X.(*ast.Ident); ok && aliases[name.Name] {
					found = true
				}
			}
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

func TestPublicAdapterErrorDeclarationControls(t *testing.T) {
	for _, test := range []struct {
		name, source string
		code         bool
	}{
		{"private_budget", "const traversalLimit = 64", false},
		{"private_text", "const description = \"bounded\"", false},
		{"reply_state", "const ErrorReply = 3", false},
		{"exported_error", "const ErrLimit = 1", true},
		{"typed_code", "import f \"github.com/frost-leo/fathomry/failure/v1\"\nconst privateCode f.Code = 1", true},
		{"inferred_code", "import f \"github.com/frost-leo/fathomry/failure/v1\"\nconst privateCode = f.Code(1)", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			syntax, err := parser.ParseFile(token.NewFileSet(), "control.go", "package control\n"+test.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, item := range syntax.Decls {
				if declaration, ok := item.(*ast.GenDecl); ok {
					found = found || stableErrorCodes(syntax, declaration)
				}
			}
			if found != test.code {
				t.Fatal("error-code guard confused stable declarations and private implementation constants")
			}
		})
	}
}

func TestPublicAdapterPolicyResponsibilities(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range publicDataAdapters {
		t.Run(provider, func(t *testing.T) {
			for name, required := range map[string][]string{
				"options.go": {"Settings", "Dependencies", "Validate"},
				"policy.go":  {"Recommend"},
			} {
				syntax, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "adapters", provider, "v1", name), nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				declarations := map[string]bool{}
				for _, declaration := range syntax.Decls {
					switch declaration := declaration.(type) {
					case *ast.FuncDecl:
						if declaration.Recv == nil {
							declarations[declaration.Name.Name] = true
						}
					case *ast.GenDecl:
						for _, specification := range declaration.Specs {
							if value, ok := specification.(*ast.TypeSpec); ok {
								declarations[value.Name.Name] = true
							}
						}
					}
				}
				for _, symbol := range required {
					if !declarations[symbol] {
						t.Errorf("%s must keep %s separately reviewable", name, symbol)
					}
				}
				for _, symbol := range []string{"Open", "Using", "DisableNativeLogging", "NewPassword"} {
					if declarations[symbol] {
						t.Errorf("%s mixes configuration/policy with live authority %s", name, symbol)
					}
				}
			}
		})
	}
}
