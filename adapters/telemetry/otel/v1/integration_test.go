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

package otel

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	for _, variant := range []struct{ name, output string }{
		{"direct", "otel direct public consumer passed\n"},
		{"framework", "otel framework public consumer passed\n"},
	} {
		t.Run(variant.name, func(t *testing.T) {
			fixture, err := os.ReadFile(filepath.Join("testdata", variant.name, "main.go"))
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
				if err != nil || strings.Contains(path, "/internal/") || strings.HasPrefix(path, "go.opentelemetry.io/otel/sdk") ||
					concreteLogger(path) || variant.name == "direct" && strings.Contains(path, "/framework/") {
					t.Fatal("consumer bypassed its public standalone boundary", path)
				}
			}
			directory := consumerJob(t)
			metricPath := filepath.Join(root, "third_party", "otel-metric")
			tracePath := filepath.Join(root, "third_party", "otel-trace")
			module := fmt.Sprintf(`module example.org/otel-%s-consumer

go 1.27.0

require github.com/frost-leo/fathomry v0.0.0

replace github.com/frost-leo/fathomry => %q
replace go.opentelemetry.io/otel/sdk/metric => %q
replace go.opentelemetry.io/otel/exporters/otlp/otlptrace => %q
`, variant.name, filepath.ToSlash(root), filepath.ToSlash(metricPath), filepath.ToSlash(tracePath))
			for name, content := range map[string][]byte{"main.go": fixture, "go.sum": sums, "go.mod": []byte(module)} {
				if err := os.WriteFile(filepath.Join(directory, name), content, 0600); err != nil {
					t.Fatal(err)
				}
			}
			run := func(offline bool, args ...string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				command := exec.CommandContext(ctx, "go", args...)
				command.Dir = directory
				command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
				if offline {
					command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
				}
				output, err := command.CombinedOutput()
				if ctx.Err() != nil {
					t.Fatalf("independent consumer command %q exceeded its bound: %v\n%s", args, ctx.Err(), output)
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
				if concreteLogger(path) || variant.name == "direct" && strings.HasPrefix(path, "github.com/frost-leo/fathomry/framework/") ||
					strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/httpclient/") ||
					strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/sqlengine/") ||
					strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/database/") {
					t.Fatalf("telemetry consumer acquired an unrelated provider: %s", path)
				}
			}
			expected := map[string]string{
				"go.opentelemetry.io/otel": "v1.47.0", "go.opentelemetry.io/otel/log": "v1.47.0",
				"go.opentelemetry.io/otel/trace": "v1.47.0", "go.opentelemetry.io/otel/metric": "v1.47.0",
				"go.opentelemetry.io/otel/sdk": "v1.47.0", "go.opentelemetry.io/otel/sdk/log": "v1.47.0",
				"go.opentelemetry.io/otel/sdk/metric":                               "v1.47.0",
				"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp":       "v0.23.0",
				"go.opentelemetry.io/otel/exporters/otlp/otlptrace":                 "v1.47.0",
				"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp":   "v1.47.0",
				"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp": "v1.47.0",
			}
			replacements := map[string]string{"go.opentelemetry.io/otel/sdk/metric": metricPath, "go.opentelemetry.io/otel/exporters/otlp/otlptrace": tracePath}
			paths := make([]string, 0, len(expected))
			for path := range expected {
				paths = append(paths, path)
			}
			sort.Strings(paths)
			output, err = run(true, append([]string{"list", "-mod=readonly", "-m", "-json"}, paths...)...)
			if err != nil {
				t.Fatalf("selected consumer module graph: %v\n%s", err, output)
			}
			type selectedModule struct {
				Path, Version, Sum string
				Replace            *selectedModule
			}
			decoder := json.NewDecoder(strings.NewReader(string(output)))
			selected := make(map[string]selectedModule, len(expected))
			for {
				var module selectedModule
				if err := decoder.Decode(&module); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				selected[module.Path] = module
			}
			build, err := buildinfo.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			linked := make(map[string]selectedModule, len(build.Deps))
			for _, module := range build.Deps {
				value := selectedModule{Path: module.Path, Version: module.Version, Sum: module.Sum}
				if module.Replace != nil {
					value.Replace = &selectedModule{Path: module.Replace.Path, Version: module.Replace.Version, Sum: module.Replace.Sum}
				}
				linked[module.Path] = value
			}
			for name, graph := range map[string]map[string]selectedModule{"resolved": selected, "linked": linked} {
				for path, version := range expected {
					module, exists := graph[path]
					if !exists || module.Version != version {
						t.Fatalf("%s graph lost exact %s@%s: %+v", name, path, version, module)
					}
					if replacement := replacements[path]; replacement != "" {
						localVersion := ""
						if name == "linked" {
							localVersion = "(devel)"
						}
						if module.Replace == nil || filepath.Clean(module.Replace.Path) != filepath.Clean(replacement) || module.Replace.Version != localVersion || module.Replace.Sum != "" {
							t.Fatalf("%s graph did not select explicit corrected source %s: got %+v, want %s", name, path, module.Replace, replacement)
						}
					} else if module.Replace != nil || module.Sum == "" {
						t.Fatalf("%s graph changed unpatched upstream selection %s", name, path)
					}
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary)
			if output, err := command.CombinedOutput(); err != nil || string(output) != variant.output {
				t.Fatalf("independent native consumer: %v\n%s", err, output)
			}
			if variant.name == "direct" {
				internal := "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
				if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(notice+"package main\nimport _ "+strconv.Quote(internal)+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if output, err := run(true, "build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), "use of internal package "+internal+" not allowed") {
					t.Fatalf("Internal import boundary not enforced: %v\n%s", err, output)
				}
			}
		})
	}
}

func concreteLogger(path string) bool {
	return strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/logging/") ||
		strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/logging/") ||
		path == "go.uber.org/zap" || strings.HasPrefix(path, "go.uber.org/zap/") ||
		path == "github.com/rs/zerolog" || strings.HasPrefix(path, "github.com/rs/zerolog/")
}

func consumerJob(t testing.TB) string {
	t.Helper()
	output, err := exec.Command("go", "env", "GOTMPDIR").Output()
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
