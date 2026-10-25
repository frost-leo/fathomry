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
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestIndependentModuleRejectsInternalAndWithdrawnPackages(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	write := func(name string, data []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", []byte("module example.org/consumer\n\ngo 1.26.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => "+fmt.Sprintf("%q", filepath.ToSlash(root))+"\n"))
	header, err := os.ReadFile(filepath.Join(root, ".github/LICENSE_HEADER"))
	if err != nil {
		t.Fatal(err)
	}
	notice := "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(header)), "\n", "\n * ") + "\n */\n"
	write("schema.go", []byte(notice+`package consumer
import (
 "io/fs"
 cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
 catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
 mysql "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
 postgresql "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)
var _ fs.FS = mysql.Assets()
var _ fs.FS = postgresql.Assets()
var _ = cli.DatabaseConfiguration{}
var _ = catalog.Registration{}
var _ = catalog.Prepare
`))
	write("catalog_test.go", []byte(notice+`package consumer
import (
 "context"
 "errors"
 "testing"
 postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
 catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
 declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
 "github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)
func TestCatalogPublicationUsesPublicContracts(t *testing.T) {
 bundle,err:=declaration.Compile(context.Background(),health.Declarations())
 if err!=nil {t.Fatal(err)}
 prepared,err:=catalog.Prepare(context.Background(),catalog.Registration{Bundle:bundle,Resources:[]catalog.Resource{
  {Target:catalog.Target{Kind:catalog.Definition},Field:catalog.Name,Text:catalog.Text{English:"Server health"}},
  {Target:catalog.Target{Kind:catalog.Mode,Version:"1",Mode:"standard"},Field:catalog.Name,Text:catalog.Text{English:"Standard"}},
  {Target:catalog.Target{Kind:catalog.Mode,Version:"1",Mode:"patient"},Field:catalog.Name,Text:catalog.Text{English:"Patient"}},
 }})
 if err!=nil {t.Fatal(err)}
 info,err:=prepared.Inspect()
 if err!=nil||info.Versions!=1||info.Modes!=2||info.Revisions!=2 {t.Fatal(info,err)}
 store,err:=catalog.PostgreSQL(new(postgres.Client),"catalog",catalog.Options{})
 if err!=nil||!store.Released(){t.Fatal("offline public construction failed",err)}
 ctx,cancel:=context.WithCancel(context.Background());cancel()
 publication,err:=store.Register(ctx,prepared)
 if !errors.Is(err,context.Canceled)||publication.Effect!=catalog.NotStarted||!publication.CleanupComplete{t.Fatal("public admission semantics differ",err)}
 comparison,err:=store.Compare(ctx,prepared)
 if !errors.Is(err,context.Canceled)||comparison.Complete{t.Fatal("public comparison admission differs",err)}
 if err:=store.Close(context.Background());err!=nil{t.Fatal(err)}
}
`))
	write("consumer.go", []byte(notice+"package consumer\nimport (\n failure \"github.com/frost-leo/fathomry/failure/v1\"\n settings \"github.com/frost-leo/fathomry/settings/v1\"\n i18n \"github.com/frost-leo/fathomry/i18n/v1\"\n resource \"github.com/frost-leo/fathomry/resource/v1\"\n adapters \"github.com/frost-leo/fathomry/adapters/v1\"\n configsource \"github.com/frost-leo/fathomry/adapters/configsource/v1\"\n viper \"github.com/frost-leo/fathomry/adapters/configsource/viper/v1\"\n nacos \"github.com/frost-leo/fathomry/adapters/configsource/nacos/v1\"\n framework \"github.com/frost-leo/fathomry/framework/v1\"\n configuration \"github.com/frost-leo/fathomry/framework/configuration/v1\"\n)\nvar _ error = failure.ErrCode\nvar _ = settings.NewStore[int]()\nvar _ = i18n.CoreComponents()\nvar _ = resource.Fixed\nvar _ = adapters.ErrOptions\nvar _ = configsource.ErrDecode\nvar _ = viper.Settings{}\nvar _ = nacos.Settings{}\nvar _ = framework.Options{}\nvar _ = configuration.ErrDeclaration\n"))
	run := func(workdir string, args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = workdir
		command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("independent module check exceeded its bound")
		}
		return output, err
	}
	output, err := run(directory, "test", "-mod=mod", "-count=1", "-v", "./...")
	if err != nil {
		t.Fatalf("independent module smoke compilation failed: %v\n%s", err, output)
	}
	t.Logf("independent module public-failure/settings smoke compilation:\n%s", output)
	for _, name := range []string{"fault", "resource", "invocation", "compatibility", "conformance", "configsource/viper/v1", "configsource/nacos/v2", "database/pgx/v5", "database/mysql/v1", "cache/redis/v9"} {
		t.Run("reject-internal-"+name, func(t *testing.T) {
			path := "github.com/frost-leo/fathomry/internal/" + name
			write("forbidden.go", []byte(notice+"package consumer\nimport _ "+fmt.Sprintf("%q", path)+"\n"))
			output, err := run(directory, "test", "-mod=mod", "-count=1", "./...")
			if err == nil || !strings.Contains(string(output), "use of internal package "+path+" not allowed") {
				t.Fatalf("wrong internal import rejection: %v\n%s", err, output)
			}
		})
	}
	for _, name := range []string{
		"source", "operation", "compatibility", "failure",
		"cli", "adapters/database/doris/v1", "database/v1", "cmd/fathomry/internal/database", "framework/schema/v1",
	} {
		t.Run("withdrawn-"+name, func(t *testing.T) {
			path := "github.com/frost-leo/fathomry/" + name
			write("forbidden.go", []byte(notice+"package consumer\nimport _ "+fmt.Sprintf("%q", path)+"\n"))
			output, err := run(directory, "test", "-mod=mod", "-count=1", "./...")
			if err == nil || !strings.Contains(string(output), path) || !strings.Contains(string(output), "does not contain package") {
				t.Fatalf("withdrawn package was not rejected as absent: %v\n%s", err, output)
			}
		})
	}
	for _, name := range []string{"schema", "database/create"} {
		t.Run("reject-cli-operation-"+name, func(t *testing.T) {
			path := "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/" + name
			write("forbidden.go", []byte(notice+"package consumer\nimport _ "+strconv.Quote(path)+"\n"))
			output, err := run(directory, "test", "-mod=mod", "-count=1", "./...")
			if err == nil || !strings.Contains(string(output), "use of internal package "+path+" not allowed") {
				t.Fatalf("private command implementation escaped: %v\n%s", err, output)
			}
		})
	}
	t.Run("reject-adapter-errorbridge", func(t *testing.T) {
		path := "github.com/frost-leo/fathomry/adapters/internal/errorbridge"
		write("forbidden.go", []byte(notice+"package consumer\nimport _ "+fmt.Sprintf("%q", path)+"\n"))
		output, err := run(directory, "test", "-mod=mod", "-count=1", "./...")
		if err == nil || !strings.Contains(string(output), "use of internal package "+path+" not allowed") {
			t.Fatalf("private adapter helper escaped: %v\n%s", err, output)
		}
	})
	output, err = run(root, "list", "./...")
	if err != nil {
		t.Fatal("package inventory could not be inspected")
	}
	cliPackages := map[string]bool{
		"github.com/frost-leo/fathomry/cmd/fathomry":                                    true,
		"github.com/frost-leo/fathomry/cmd/fathomry/v1":                                 true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/app":                       true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command":                   true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/errorcatalog":      true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/messages":          true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project":           true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project/new":       true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project/build":     true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow":          true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/vet":      true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/register": true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/list":     true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/show":     true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/compare":  true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema":            true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema/status":     true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema/plan":       true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema/apply":      true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database":          true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create":   true,
	}
	for _, path := range strings.Fields(string(output)) {
		if cliPackages[path] {
			continue
		}
		if path == "github.com/frost-leo/fathomry/adapters/internal/errorbridge" {
			continue // Private integration helper, not another public capability.
		}
		if path == "github.com/frost-leo/fathomry/framework/workflow/declaration/v1" {
			continue // Pure declaration admission, not a Workflow host.
		}
		if path == "github.com/frost-leo/fathomry/framework/workflow/catalog/v1" {
			continue // Catalog publication admission, not database resource ownership.
		}
		if path == "github.com/frost-leo/fathomry/framework/workflow/implementation/v1" {
			continue // Static callable binding, not an Activity executor or Worker.
		}
		switch path {
		case "github.com/frost-leo/fathomry/schema/fathomry":
			continue // Test-only dialect parity and service qualification harness.
		case "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1",
			"github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1":
			continue // Read-only, standard-library-only schema assets.
		}
		switch path {
		case "github.com/frost-leo/fathomry/adapters/broker/v1",
			"github.com/frost-leo/fathomry/adapters/notification/v1",
			"github.com/frost-leo/fathomry/adapters/notification/mail/v1",
			"github.com/frost-leo/fathomry/adapters/notification/lark/v1",
			"github.com/frost-leo/fathomry/adapters/orchestration/v1",
			"github.com/frost-leo/fathomry/adapters/telemetry/v1",
			"github.com/frost-leo/fathomry/adapters/httpclient/v1",
			"github.com/frost-leo/fathomry/adapters/httpclient/nethttp/v1",
			"github.com/frost-leo/fathomry/adapters/httpclient/tlsclient/v1",
			"github.com/frost-leo/fathomry/adapters/httpclient/surf/v1",
			"github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1",
			"github.com/frost-leo/fathomry/adapters/httpclient/nuki/v1",
			"github.com/frost-leo/fathomry/adapters/telemetry/otel/v1",
			"github.com/frost-leo/fathomry/adapters/orchestration/temporal/v1",
			"github.com/frost-leo/fathomry/adapters/logging/v1",
			"github.com/frost-leo/fathomry/adapters/logging/slog/v1",
			"github.com/frost-leo/fathomry/adapters/logging/zap/v1",
			"github.com/frost-leo/fathomry/adapters/logging/zap/otel/v1",
			"github.com/frost-leo/fathomry/adapters/logging/zerolog/v1",
			"github.com/frost-leo/fathomry/adapters/logging/zerolog/otel/v1",
			"github.com/frost-leo/fathomry/adapters/broker/kafka/v1",
			"github.com/frost-leo/fathomry/adapters/cache/v1",
			"github.com/frost-leo/fathomry/adapters/cache/redis/v1",
			"github.com/frost-leo/fathomry/adapters/database/v1",
			"github.com/frost-leo/fathomry/adapters/database/postgres/v1",
			"github.com/frost-leo/fathomry/adapters/database/mysql/v1",
			"github.com/frost-leo/fathomry/adapters/sqlengine/duckdb/v1",
			"github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1",
			"github.com/frost-leo/fathomry/adapters/sqlengine/doris/v1",
			"github.com/frost-leo/fathomry/adapters/sqlengine/v1",
			"github.com/frost-leo/fathomry/adapters/tableformat/v1",
			"github.com/frost-leo/fathomry/adapters/browser/v1",
			"github.com/frost-leo/fathomry/adapters/browser/chromedp/v1",
			"github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1",
			"github.com/frost-leo/fathomry/adapters/objectstore/v1",
			"github.com/frost-leo/fathomry/adapters/objectstore/minio/v1":
			continue
		}
		if !strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") && path != "github.com/frost-leo/fathomry/failure/v1" && path != "github.com/frost-leo/fathomry/settings/v1" && path != "github.com/frost-leo/fathomry/i18n/v1" && path != "github.com/frost-leo/fathomry/resource/v1" && path != "github.com/frost-leo/fathomry/adapters/v1" && path != "github.com/frost-leo/fathomry/adapters/configsource/v1" && path != "github.com/frost-leo/fathomry/adapters/configsource/viper/v1" && path != "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1" && path != "github.com/frost-leo/fathomry/framework/v1" && path != "github.com/frost-leo/fathomry/framework/configuration/v1" {
			t.Errorf("unexpected public package: %s", path)
		}
	}
	output, err = run(root, "list", "-deps", "./internal/fault", "./internal/resource", "./internal/invocation", "./internal/compatibility")
	if err != nil {
		t.Fatal("internal dependency check failed")
	}
	for _, path := range strings.Fields(string(output)) {
		if path == "testing" || path == "github.com/frost-leo/fathomry/internal/conformance" ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/configsource/") || path == "github.com/spf13/viper" ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/database/") || strings.HasPrefix(path, "github.com/jackc/pgx/") || path == "github.com/go-sql-driver/mysql" ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/") && !strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") {
			t.Errorf("technical mechanisms depend on framework errors or test support: %s", path)
		}
	}
}

func TestCLIConsumesPublicCapabilities(t *testing.T) {
	root, err := filepath.Abs("../../cmd/fathomry")
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, item := range file.Imports {
			imported, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				return err
			}
			if strings.HasPrefix(imported, "github.com/frost-leo/fathomry/internal/") {
				t.Errorf("CLI bypasses public capabilities: %s: %s", path, imported)
			}
			if imported == "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema" || imported == "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create" || imported == "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database" {
				relative, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				if !cliControlUseAllowed(filepath.ToSlash(relative), imported, item, file) {
					t.Errorf("CLI command crossed its operation boundary: %s: %s", path, imported)
				}
			}
			if strings.HasPrefix(imported, "github.com/frost-leo/fathomry/framework/") {
				relative, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				if cliWorkflowDeclarationUseAllowed(filepath.ToSlash(relative), imported, item, file) ||
					cliControlUseAllowed(filepath.ToSlash(relative), imported, item, file) {
					continue
				}
				if filepath.ToSlash(relative) != "internal/app/catalog.go" || item.Name == nil || item.Name.Name == "." || item.Name.Name == "_" {
					t.Errorf("CLI execution depends on Framework: %s: %s", path, imported)
					continue
				}
				name := item.Name.Name
				ast.Inspect(file, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					owner, ok := selector.X.(*ast.Ident)
					if ok && owner.Name == name && selector.Sel.Name != "Components" && selector.Sel.Name != "Definitions" && selector.Sel.Name != "Resources" {
						t.Errorf("metadata composition calls Framework runtime: %s: %s", path, selector.Sel.Name)
					}
					return true
				})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFrameworkHasNoDirectInternalImports(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(filepath.Join(root, "framework"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, item := range file.Imports {
			imported, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				return err
			}
			if strings.HasPrefix(imported, "github.com/frost-leo/fathomry/internal/") {
				t.Errorf("Framework bypasses public Adapters: %s: %s", path, imported)
			}
			if strings.HasPrefix(imported, "github.com/frost-leo/fathomry/cmd/") {
				relative, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				if imported == "github.com/frost-leo/fathomry/cmd/fathomry/v1" &&
					(filepath.ToSlash(relative) == "framework/workflow/implementation/v1/testdata/consumer/main.go" ||
						filepath.ToSlash(relative) == "framework/workflow/implementation/v1/testdata/consumer/workflows/example/server_health/v1/workflow.go") {
					continue // Independent module assembly, never imported by Framework.
				}
				t.Errorf("Framework depends on command tools: %s: %s", path, imported)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
