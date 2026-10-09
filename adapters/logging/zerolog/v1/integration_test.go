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

package zerolog

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
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
	if runtime.GOOS != "linux" {
		t.Skip("independent managed-file consumers require the supported Linux file profile")
	}
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"direct", "framework"} {
		t.Run(variant, func(t *testing.T) {
			fixture, err := os.ReadFile(filepath.Join("testdata", variant, "main.go"))
			if err != nil {
				t.Fatal(err)
			}
			syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imported := range syntax.Imports {
				path, err := strconv.Unquote(imported.Path.Value)
				if err != nil || strings.Contains(path, "/internal/") || forbiddenDependency(path, variant) {
					t.Fatal("independent consumer crossed its public local-only boundary", path)
				}
			}
			directory := consumerJob(t)
			temporary := filepath.Join(directory, "runtime-tmp")
			if err := os.Mkdir(temporary, 0700); err != nil {
				t.Fatal(err)
			}
			manifest := fmt.Sprintf("module example.org/zerolog-%s-consumer\n\ngo 1.27.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\n\nreplace github.com/frost-leo/fathomry => %q\n", variant, filepath.ToSlash(root))
			for name, content := range map[string][]byte{"main.go": fixture, "go.sum": sums, "go.mod": []byte(manifest)} {
				if err := os.WriteFile(filepath.Join(directory, name), content, 0600); err != nil {
					t.Fatal(err)
				}
			}
			run := func(offline bool, args ...string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				command := exec.CommandContext(ctx, "go", args...)
				command.Dir = directory
				command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "TMPDIR="+temporary)
				if offline {
					command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
				}
				output, err := command.CombinedOutput()
				if ctx.Err() != nil {
					t.Fatalf("consumer command %q exceeded its bound: %v\n%s", args, ctx.Err(), output)
				}
				return output, err
			}
			if output, err := run(false, "mod", "tidy"); err != nil {
				t.Fatalf("consumer preparation: %v\n%s", err, output)
			}
			binary := filepath.Join(directory, "consumer")
			if output, err := run(true, "build", "-mod=readonly", "-race", "-o", binary, "."); err != nil {
				t.Fatalf("consumer build: %v\n%s", err, output)
			}
			if output, err := run(true, "mod", "tidy", "-diff"); err != nil {
				t.Fatalf("consumer graph changed offline: %v\n%s", err, output)
			}
			output, err := run(true, "list", "-mod=readonly", "-deps", ".")
			if err != nil {
				t.Fatalf("consumer package graph: %v\n%s", err, output)
			}
			for _, path := range strings.Fields(string(output)) {
				if forbiddenDependency(path, variant) {
					t.Fatal("local-only consumer acquired an unnecessary provider or telemetry SDK", path)
				}
			}
			output, err = run(true, "list", "-mod=readonly", "-m", "-json", "github.com/rs/zerolog")
			if err != nil {
				t.Fatalf("consumer selected SDK: %v\n%s", err, output)
			}
			var selected struct {
				Version, Sum string
				Replace      any
			}
			if json.Unmarshal(output, &selected) != nil || selected.Version != "v1.35.1" || selected.Replace != nil || selected.Sum == "" {
				t.Fatal("consumer changed the selected original zerolog source")
			}
			build, err := buildinfo.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, dependency := range build.Deps {
				if dependency.Path == "github.com/rs/zerolog" {
					found = dependency.Version == "v1.35.1" && dependency.Replace == nil && dependency.Sum == selected.Sum
				}
				if strings.HasPrefix(dependency.Path, "go.opentelemetry.io/") || dependency.Path == "go.uber.org/zap" {
					t.Fatal("local-only binary linked an unselected telemetry/logger implementation", dependency.Path)
				}
			}
			if !found {
				t.Fatal("actual consumer binary lost native zerolog provenance")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary)
			command.Dir = directory
			command.Env = append(os.Environ(), "TMPDIR="+temporary)
			if output, err := command.CombinedOutput(); err != nil || string(output) != "zerolog "+variant+" public consumer passed\n" {
				t.Fatalf("independent consumer execution: %v\n%s", err, output)
			}
			if variant == "direct" {
				notice, _, _ := strings.Cut(string(fixture), "package main")
				internal := "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
				if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(notice+"package main\nimport _ "+strconv.Quote(internal)+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if output, err := run(true, "build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), "use of internal package "+internal+" not allowed") {
					t.Fatalf("independent Internal import refusal was not enforced: %v\n%s", err, output)
				}
			}
		})
	}
}

func forbiddenDependency(path, variant string) bool {
	return strings.HasPrefix(path, "go.opentelemetry.io/") || strings.HasPrefix(path, "go.uber.org/zap") ||
		strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/telemetry/") ||
		strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/telemetry/") ||
		strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/logging/zap/") ||
		strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/logging/zap/") ||
		variant == "direct" && strings.HasPrefix(path, "github.com/frost-leo/fathomry/framework/")
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
