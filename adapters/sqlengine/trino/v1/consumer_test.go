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

package trino_test

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestIndependentConsumers(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"direct", "framework"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			files := map[string]string{"main.go": "testdata/consumer/main.go"}
			if kind == "framework" {
				files = map[string]string{"peer_test.go": "peer_test.go", "source_test.go": "source_test.go", "framework_test.go": "framework_test.go"}
			}
			for target, source := range files {
				content, err := os.ReadFile(source)
				if err != nil {
					t.Fatal(err)
				}
				syntax, err := parser.ParseFile(token.NewFileSet(), source, content, parser.ImportsOnly)
				if err != nil {
					t.Fatal(err)
				}
				for _, imported := range syntax.Imports {
					path, err := strconv.Unquote(imported.Path.Value)
					if err != nil || strings.Contains(path, "/internal/") || strings.Contains(path, "trino-go-client") || kind == "direct" && strings.Contains(path, "/framework/") {
						t.Fatal("consumer bypasses the independent public API boundary", source)
					}
				}
				if err := os.WriteFile(filepath.Join(directory, target), content, 0600); err != nil {
					t.Fatal(err)
				}
			}
			sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
			if err != nil {
				t.Fatal(err)
			}
			for name, content := range map[string][]byte{"go.sum": sums, "go.mod": []byte(fmt.Sprintf("module example.org/trino-%s-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", kind, filepath.ToSlash(root)))} {
				if err := os.WriteFile(filepath.Join(directory, name), content, 0600); err != nil {
					t.Fatal(err)
				}
			}
			run := func(offline bool, arguments ...string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer cancel()
				command := exec.CommandContext(ctx, "go", arguments...)
				command.Dir = directory
				command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
				if offline {
					command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
				}
				return command.CombinedOutput()
			}
			mustRun := func(offline bool, arguments ...string) []byte {
				output, err := run(offline, arguments...)
				if err != nil {
					t.Fatalf("independent %s consumer %v: %v\n%s", kind, arguments, err, output)
				}
				return output
			}
			mustRun(false, "mod", "tidy")
			mustRun(true, "mod", "tidy", "-diff")
			dependencies := mustRun(true, "list", "-mod=readonly", "-deps", "-test", ".")
			for _, name := range strings.Fields(string(dependencies)) {
				for _, forbidden := range []string{"/fathomry/adapters/database/", "/fathomry/adapters/sqlengine/duckdb/", "/fathomry/internal/database/", "/fathomry/internal/sqlengine/duckdb/", "/fathomry/internal/sqlengine/doris/"} {
					if strings.Contains(name, forbidden) {
						t.Fatal("single-provider consumer acquired unrelated implementation", name)
					}
				}
				if kind == "direct" && strings.Contains(name, "/fathomry/framework/") {
					t.Fatal("independent provider consumer acquired Framework")
				}
			}
			if kind == "framework" {
				binary := filepath.Join(directory, "framework-consumer.test")
				mustRun(true, "test", "-mod=readonly", "-race", "-c", "-o", binary, ".")
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				command := exec.CommandContext(ctx, binary, "-test.run=^TestFrameworkFixedFollowAndCandidateOwnership$", "-test.timeout=25s", "-test.v")
				command.Dir = directory
				if output, err := command.CombinedOutput(); err != nil || !bytes.Contains(output, []byte("PASS")) {
					t.Fatalf("independent actual Framework consumer: %v\n%s", err, output)
				}
				return
			}
			binary := filepath.Join(directory, "consumer")
			mustRun(true, "build", "-mod=readonly", "-race", "-o", binary, ".")
			build, err := buildinfo.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			pinned := false
			for _, dependency := range build.Deps {
				if dependency.Path == "github.com/trinodb/trino-go-client" {
					pinned = dependency.Version == "v0.333.0" && dependency.Replace == nil && dependency.Sum != ""
				}
			}
			if !pinned {
				t.Fatal("independent binary lost the selected native SDK pin")
			}
			peer := newWirePeer(t, func(writer http.ResponseWriter, _ *http.Request, _ []byte) {
				writePage(t, writer, map[string]any{"id": "consumer", "columns": []any{wireColumn("value", "bigint", "bigint")}, "data": [][]any{{json.Number("9223372036854775807")}}, "updateCount": 2})
			})
			configuration, err := json.Marshal(peer.settings())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary)
			command.Stdin = bytes.NewReader(configuration)
			if output, err := command.CombinedOutput(); err != nil || string(output) != "trino independent consumer passed\n" {
				t.Fatalf("independent public consumer: %v\n%s", err, output)
			}
			if peer.posts.Load() != 4 || peer.readiness.Load() != 1 {
				t.Fatal("independent complete capability set was not exactly dispatched")
			}
			for _, forbidden := range []struct{ name, code, message string }{
				{"client-close", "var _ = (*p.Client).Close", "has no field or method Close"},
				{"raw-connection", "var _ = (*p.Client).Conn", "has no field or method Conn"},
				{"handle-owner", "var _ = p.Owner(p.Handle{})", "cannot convert"},
			} {
				source := "package main\nimport p \"github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1\"\n" + forbidden.code + "\n"
				if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(source), 0600); err != nil {
					t.Fatal(err)
				}
				if output, err := run(true, "build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), forbidden.message) {
					t.Fatalf("public authority boundary %s not enforced: %v\n%s", forbidden.name, err, output)
				}
			}
			private := "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
			if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte("package main\nimport _ "+strconv.Quote(private)+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if output, err := run(true, "build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), "use of internal package "+private+" not allowed") {
				t.Fatalf("independent module reached Internal API: %v\n%s", err, output)
			}
		})
	}
}
