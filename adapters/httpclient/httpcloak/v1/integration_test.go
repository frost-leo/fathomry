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

package httpcloak

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []struct{ name, path, output string }{
		{"direct", "testdata/direct/main.go", "httpcloak direct public consumer passed\n"},
		{"framework", "testdata/framework/main.go", "httpcloak framework public consumer passed\n"},
	} {
		t.Run(variant.name, func(t *testing.T) {
			fixture, err := os.ReadFile(variant.path)
			if err != nil {
				t.Fatal(err)
			}
			notice, _, _ := strings.Cut(string(fixture), "package main")
			syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imported := range syntax.Imports {
				path, err := strconv.Unquote(imported.Path.Value)
				if err != nil || strings.Contains(path, "/internal/") || path == "github.com/sardanioss/httpcloak" || variant.name == "direct" && strings.Contains(path, "/framework/") {
					t.Fatal("consumer bypassed public boundary")
				}
			}
			directory := consumerJob(t)
			for name, content := range map[string][]byte{
				"main.go": fixture,
				"go.sum":  sums,
				"go.mod":  []byte(fmt.Sprintf("module example.org/httpcloak-%s-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\nreplace github.com/sardanioss/httpcloak => %q\nreplace github.com/sardanioss/net => %q\nreplace github.com/sardanioss/quic-go => %q\nreplace github.com/sardanioss/udpbara => %q\n", variant.name, filepath.ToSlash(root), filepath.ToSlash(filepath.Join(root, "third_party/httpcloak")), filepath.ToSlash(filepath.Join(root, "third_party/httpcloak-net")), filepath.ToSlash(filepath.Join(root, "third_party/httpcloak-quic-go")), filepath.ToSlash(filepath.Join(root, "third_party/udpbara")))),
			} {
				if err := os.WriteFile(filepath.Join(directory, name), content, 0600); err != nil {
					t.Fatal(err)
				}
			}
			run := func(offline bool, args ...string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), args...)
				command.Dir = directory
				command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
				if offline {
					command.Env = append(command.Env, "GOPROXY=off", "GONOPROXY=none", "GOSUMDB=off")
				}
				output, err := command.CombinedOutput()
				if ctx.Err() != nil {
					t.Fatal("independent consumer build exceeded its bound")
				}
				return output, err
			}
			if output, err := run(false, "mod", "tidy"); err != nil {
				t.Fatalf("consumer module preparation: %v\n%s", err, output)
			}
			binary := filepath.Join(directory, "consumer")
			if output, err := run(true, "build", "-mod=readonly", "-race", "-o", binary, "."); err != nil {
				t.Fatalf("consumer build: %v\n%s", err, output)
			}
			if output, err := run(true, "mod", "tidy", "-diff"); err != nil {
				t.Fatalf("consumer graph unstable offline: %v\n%s", err, output)
			}
			output, err := run(true, "list", "-mod=readonly", "-deps", ".")
			if err != nil {
				t.Fatalf("consumer dependencies: %v\n%s", err, output)
			}
			for _, path := range strings.Fields(string(output)) {
				if strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/database/") ||
					strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/httpclient/") && !strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/") ||
					strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/sqlengine/doris/") ||
					variant.name == "direct" && strings.HasPrefix(path, "github.com/frost-leo/fathomry/framework/") {
					t.Fatalf("single-provider consumer acquired unrelated provider dependency: %s", path)
				}
			}
			build, err := buildinfo.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(build.GoVersion, "go1.27") {
				t.Fatal("consumer lost selected standard-library provenance", build.GoVersion)
			}
			wanted := map[string]string{
				"github.com/sardanioss/httpcloak": "httpcloak",
				"github.com/sardanioss/net":       "httpcloak-net",
				"github.com/sardanioss/quic-go":   "httpcloak-quic-go",
				"github.com/sardanioss/udpbara":   "udpbara",
			}
			for _, module := range build.Deps {
				if directory, expected := wanted[module.Path]; expected {
					if module.Replace == nil || module.Replace.Path != filepath.Join(root, "third_party", directory) {
						t.Fatalf("wrong selected native replacement: %s", module.Path)
					}
					if module.Path == "github.com/sardanioss/httpcloak" && module.Version != "v1.7.2" {
						t.Fatal("SDK version changed")
					}
					delete(wanted, module.Path)
				}
			}
			if len(wanted) != 0 {
				t.Fatal("consumer lost explicit development SDK replacement")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary)
			var diagnostics bytes.Buffer
			command.Stderr = &diagnostics
			if output, err := command.Output(); err != nil || string(output) != variant.output {
				t.Fatalf("independent native consumer: %v\n%s\n%s", err, output, diagnostics.String())
			}
			if diagnostics.Len() != 0 {
				t.Logf("native peer diagnostics: %s", diagnostics.String())
			}
			if variant.name != "direct" {
				return
			}
			for _, invalid := range []struct{ name, imports, source, want string }{
				{"client_shutdown", "", "var _ = (*p.Client).Close", "has no field or method Close"},
				{"native_connection", "", "var _ = (*p.Client).Connect", "has no field or method Connect"},
				{"handle_owner", "", "var _ = p.Owner(p.Handle{})", "cannot convert"},
				{"cross_provider_handle", `import mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"`, "var _ = p.Handle(mysql.Handle{})", "cannot convert"},
			} {
				t.Run(invalid.name, func(t *testing.T) {
					source := notice + "package main\nimport p \"github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1\"\n" + invalid.imports + "\n" + invalid.source + "\n"
					if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(source), 0600); err != nil {
						t.Fatal(err)
					}
					if output, err := run(false, "mod", "tidy"); err != nil {
						t.Fatalf("authority fixture preparation: %v\n%s", err, output)
					}
					if output, err := run(true, "build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), invalid.want) {
						t.Fatalf("authority boundary not enforced: %v\n%s", err, output)
					}
				})
			}
			internal := "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1"
			if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(notice+"package main\nimport _ "+strconv.Quote(internal)+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if output, err := run(true, "build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), "use of internal package "+internal+" not allowed") {
				t.Fatalf("Internal import boundary not enforced: %v\n%s", err, output)
			}
		})
	}
}

func consumerJob(t testing.TB) string {
	t.Helper()
	command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "env", "GOTMPDIR")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	temporary := strings.TrimSpace(string(output))
	if temporary == "" {
		temporary = filepath.Join(os.TempDir(), "fathomry")
	}
	jobs := filepath.Join(filepath.Dir(temporary), "jobs")
	if err := os.MkdirAll(jobs, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(jobs, "build.")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	return directory
}
