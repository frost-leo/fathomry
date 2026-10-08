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
	"fmt"
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

type adapterPackageProfile struct {
	role     string
	catalog  bool
	required []string
}

var adapterPackageProfiles = map[string]adapterPackageProfile{
	"v1":                      {"mechanism", true, []string{"options.go", "runtime.go", "call.go", "evidence.go"}},
	"database/v1":             {"capability", false, []string{"policy.go", "metadata.go"}},
	"sqlengine/v1":            {"capability", false, []string{"policy.go", "metadata.go"}},
	"broker/v1":               {"capability", false, []string{"policy.go", "metadata.go"}},
	"cache/v1":                {"capability", false, []string{"policy.go", "metadata.go"}},
	"objectstore/v1":          {"capability", false, []string{"policy.go", "metadata.go"}},
	"httpclient/v1":           {"capability", false, []string{"policy.go", "metadata.go"}},
	"configsource/v1":         {"preparation", true, []string{"options.go", "schema.go", "prepare.go", "decode.go", "acquisition.go"}},
	"configsource/viper/v1":   {"configuration-provider", true, []string{"options.go", "client.go", "load.go", "document.go", "watch.go", "acquisition.go"}},
	"configsource/nacos/v1":   {"configuration-provider", true, []string{"options.go", "client.go", "source.go", "metadata.go", "read.go", "watch.go", "acquisition.go"}},
	"database/postgres/v1":    {"data-provider", true, nil},
	"database/mysql/v1":       {"data-provider", true, nil},
	"sqlengine/duckdb/v1":     {"data-provider", true, nil},
	"sqlengine/trino/v1":      {"data-provider", true, nil},
	"sqlengine/doris/v1":      {"data-provider", true, nil},
	"objectstore/minio/v1":    {"data-provider", true, nil},
	"broker/kafka/v1":         {"data-provider", true, nil},
	"cache/redis/v1":          {"data-provider", true, nil},
	"httpclient/nethttp/v1":   {"data-provider", true, nil},
	"httpclient/tlsclient/v1": {"data-provider", true, nil},
	"httpclient/surf/v1":      {"data-provider", true, nil},
	"internal/errorbridge":    {"private", false, []string{"bridge.go", "containment.go", "details.go"}},
}

func adapterDirectories(root string) (map[string][]string, error) {
	result := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "testdata" || strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		result[relative] = append(result[relative], entry.Name())
		return nil
	})
	return result, err
}

func packageInventoryError(packages map[string][]string, profiles map[string]adapterPackageProfile) error {
	for path := range packages {
		if _, exists := profiles[path]; !exists {
			return fmt.Errorf("unclassified Adapter package: %s", path)
		}
	}
	for path := range profiles {
		if _, exists := packages[path]; !exists {
			return fmt.Errorf("documented Adapter package is absent: %s", path)
		}
	}
	return nil
}

func TestPublicAdapterPackageInventory(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	packages, err := adapterDirectories(filepath.Join(root, "adapters"))
	if err != nil {
		t.Fatal(err)
	}
	if err := packageInventoryError(packages, adapterPackageProfiles); err != nil {
		t.Fatal(err)
	}
	readme, err := os.ReadFile(filepath.Join(root, "adapters", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	for path, profile := range adapterPackageProfiles {
		t.Run(path, func(t *testing.T) {
			if !strings.Contains(string(readme), "| `"+path+"` | "+profile.role+" |") {
				t.Fatal("Adapter README omits this package's explicit role")
			}
			required := append([]string{"doc.go", "diagnostics.go"}, profile.required...)
			if profile.catalog {
				required = append(required, "definitions.go", "error.go", "resources.go")
			}
			if profile.role == "data-provider" {
				required = append(required, "options.go", "policy.go", "source.go", "client.go", "result.go")
			}
			for _, name := range required {
				if !slices.Contains(packages[path], name) {
					t.Errorf("missing responsibility file %s", name)
				}
			}
			contract := filepath.Join(root, "docs", "reference", "adapters", filepath.FromSlash(path), "interface.md")
			if _, err := os.Stat(contract); err != nil {
				t.Fatal("package calling contract missing", err)
			}
			for _, name := range packages[path] {
				syntax, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "adapters", path, name), nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				for _, imported := range syntax.Imports {
					dependency, err := strconv.Unquote(imported.Path.Value)
					if err != nil || !adapterDependencyAllowed(path, profile.role, dependency) {
						t.Errorf("%s acquired dependency outside its %s role: %s", name, profile.role, imported.Path.Value)
					}
				}
				for _, declaration := range syntax.Decls {
					switch value := declaration.(type) {
					case *ast.FuncDecl:
						if expected := responsibilityFile(profile, value); expected != "" && name != expected {
							t.Errorf("%s belongs in %s, not %s", value.Name.Name, expected, name)
						}
					case *ast.GenDecl:
						if profile.catalog && name != "definitions.go" && stableErrorCodes(syntax, value) {
							t.Errorf("stable error codes escaped definitions.go into %s", name)
						}
						for _, specification := range value.Specs {
							if item, ok := specification.(*ast.TypeSpec); ok {
								if (profile.role == "data-provider" || profile.role == "configuration-provider") && (item.Name.Name == "Settings" || item.Name.Name == "Dependencies") && name != "options.go" {
									t.Error("provider settings/dependencies escaped options.go")
								}
								if profile.role == "capability" && (item.Name.Name == "Policy" || item.Name.Name == "Budget") && name != "policy.go" {
									t.Error("shared budget vocabulary escaped policy.go")
								}
							}
						}
					}
				}
			}
		})
	}
}

func responsibilityFile(profile adapterPackageProfile, declaration *ast.FuncDecl) string {
	switch declaration.Name.Name {
	case "Format", "LogValue", "MarshalJSON", "UnmarshalJSON":
		if declaration.Recv != nil {
			return "diagnostics.go"
		}
	}
	if profile.catalog {
		switch declaration.Name.Name {
		case "fail", "failureOf", "translate", "InspectError":
			return "error.go"
		case "Definitions", "CacheDefinitions", "MessagingDefinitions":
			return "definitions.go"
		case "Resources", "CacheResources", "MessagingResources":
			return "resources.go"
		}
	}
	return ""
}

func adapterDependencyAllowed(path, role, dependency string) bool {
	if !strings.Contains(strings.Split(dependency, "/")[0], ".") {
		return true
	}
	const module = "github.com/frost-leo/fathomry/"
	switch role {
	case "mechanism":
		return dependency == module+"failure/v1" || dependency == module+"resource/v1"
	case "capability":
		return dependency == module+"adapters/v1"
	case "preparation":
		return dependency == module+"failure/v1" || dependency == module+"settings/v1" ||
			dependency == "github.com/pelletier/go-toml/v2" || dependency == "github.com/pelletier/go-toml/v2/unstable" || dependency == "go.yaml.in/yaml/v3"
	case "private":
		return dependency == module+"failure/v1" || dependency == module+"internal/fault"
	default:
		if strings.HasPrefix(dependency, module+"framework/") {
			return false
		}
		if other, exists := adapterPackageProfiles[strings.TrimPrefix(dependency, module+"adapters/")]; exists &&
			strings.HasSuffix(other.role, "-provider") && dependency != module+"adapters/"+path {
			return false
		}
		return true
	}
}

func TestPublicAdapterInventoryRejectingControls(t *testing.T) {
	known := map[string]adapterPackageProfile{"v1": {role: "mechanism"}}
	for _, test := range []struct {
		name     string
		packages map[string][]string
		rejected bool
	}{
		{"known", map[string][]string{"v1": {"doc.go"}}, false},
		{"unclassified", map[string][]string{"v1": {"doc.go"}, "future/v1": {"doc.go"}}, true},
		{"stale", map[string][]string{}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if (packageInventoryError(test.packages, known) != nil) != test.rejected {
				t.Fatal("package inventory guard failed its control")
			}
		})
	}
	for _, test := range []struct {
		role, dependency string
		allowed          bool
	}{
		{"mechanism", "github.com/frost-leo/fathomry/failure/v1", true},
		{"mechanism", "github.com/frost-leo/fathomry/internal/invocation", false},
		{"capability", "github.com/frost-leo/fathomry/adapters/database/mysql/v1", false},
		{"preparation", "go.yaml.in/yaml/v3", true},
		{"preparation", "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1", false},
		{"data-provider", "github.com/frost-leo/fathomry/adapters/configsource/viper/v1", false},
		{"private", "github.com/frost-leo/fathomry/adapters/v1", false},
	} {
		if adapterDependencyAllowed("database/mysql/v1", test.role, test.dependency) != test.allowed {
			t.Fatal("dependency guard failed its control")
		}
	}
}
