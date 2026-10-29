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
	write("consumer.go", []byte(notice+`package consumer
import (
 cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
 failure "github.com/frost-leo/fathomry/failure/v1"
 settings "github.com/frost-leo/fathomry/settings/v1"
 i18n "github.com/frost-leo/fathomry/i18n/v1"
 resource "github.com/frost-leo/fathomry/resource/v1"
 adapters "github.com/frost-leo/fathomry/adapters/v1"
 configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
 viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
 nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
 framework "github.com/frost-leo/fathomry/framework/v1"
 configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
)
var _ = cli.Run
var _ error = failure.ErrCode
var _ = settings.NewStore[int]()
var _ = i18n.CoreComponents()
var _ = resource.Fixed
var _ = adapters.ErrOptions
var _ = configsource.ErrDecode
var _ = viper.Settings{}
var _ = nacos.Settings{}
var _ = framework.Prepare
var _ = configuration.LoadBootstrap
`))
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
		"cli", "adapters/database/doris/v1", "database/v1", "cmd/fathomry/internal/database", "framework/schema/v1", "framework/workflow/declaration/v1", "framework/workflow/implementation/v1", "framework/workflow/catalog/v1", "schema/fathomry/mysql/v1", "schema/fathomry/postgresql/v1",
		"cmd/fathomry/internal/command/project/new", "cmd/fathomry/internal/command/project/build", "cmd/fathomry/internal/command/workflow", "cmd/fathomry/internal/command/schema", "cmd/fathomry/internal/command/database/create",
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
	for _, retired := range []struct{ path, symbol string }{
		{"framework/v1", "New"}, {"framework/configuration/v1", "Load"},
	} {
		t.Run("retired-api-"+retired.path, func(t *testing.T) {
			path := "github.com/frost-leo/fathomry/" + retired.path
			write("forbidden.go", []byte(notice+"package consumer\nimport legacy "+strconv.Quote(path)+"\nvar _ = legacy."+retired.symbol+"\n"))
			output, err := run(directory, "test", "-mod=mod", "-count=1", "./...")
			if err == nil || !strings.Contains(string(output), "undefined: legacy."+retired.symbol) {
				t.Fatalf("retired API was restored or rejected for another reason: %v\n%s", err, output)
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
		"github.com/frost-leo/fathomry/cmd/fathomry":                                 true,
		"github.com/frost-leo/fathomry/cmd/fathomry/v1":                              true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/app":                    true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command":                true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project":        true,
		"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project/create": true,
	}
	for _, path := range strings.Fields(string(output)) {
		if cliPackages[path] {
			continue
		}
		if path == "github.com/frost-leo/fathomry/adapters/internal/errorbridge" {
			continue // Private integration helper, not another public capability.
		}
		switch path {
		case "github.com/frost-leo/fathomry/framework/v1",
			"github.com/frost-leo/fathomry/framework/configuration/v1",
			"github.com/frost-leo/fathomry/adapters/broker/v1",
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
		if !strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") && path != "github.com/frost-leo/fathomry/failure/v1" && path != "github.com/frost-leo/fathomry/settings/v1" && path != "github.com/frost-leo/fathomry/i18n/v1" && path != "github.com/frost-leo/fathomry/resource/v1" && path != "github.com/frost-leo/fathomry/adapters/v1" && path != "github.com/frost-leo/fathomry/adapters/configsource/v1" && path != "github.com/frost-leo/fathomry/adapters/configsource/viper/v1" && path != "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1" {
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
			if strings.HasPrefix(imported, "github.com/frost-leo/fathomry/framework/") ||
				strings.HasPrefix(imported, "github.com/frost-leo/fathomry/schema/") {
				t.Errorf("CLI imports out-of-scope Framework/schema surface: %s: %s", path, imported)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
