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

package orchestration_test

import (
	"context"
	"debug/buildinfo"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const categoryPath = "github.com/frost-leo/fathomry/adapters/orchestration/v1"

func TestCommonTypesAreDefinedAndHaveNoProviderImports(t *testing.T) {
	packages, err := parser.ParseDir(token.NewFileSet(), ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	pack, present := packages["orchestration"]
	if !present {
		t.Fatal("common contract package missing")
	}
	for _, file := range pack.Files {
		for _, imported := range file.Imports {
			path, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(path, ".") && path != "github.com/frost-leo/fathomry/adapters/v1" {
				t.Fatal("common source acquired an implementation dependency", path)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			declaration, ok := node.(*ast.TypeSpec)
			if ok && token.IsExported(declaration.Name.Name) && declaration.Assign.IsValid() {
				t.Fatal("public common contract is an implementation alias", declaration.Name.Name)
			}
			return true
		})
	}
}

func TestIndependentCommonConsumer(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("testdata/consumer/main.go")
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
		if err != nil || strings.Contains(path, "/internal/") || strings.Contains(path, ".") && path != categoryPath && path != "github.com/frost-leo/fathomry/adapters/v1" {
			t.Fatal("consumer bypassed category contracts", path)
		}
	}
	job := consumerJob(t)
	moduleDirectory := filepath.Join(job, "module")
	if err := os.Mkdir(moduleDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	module := fmt.Sprintf("module example.org/orchestration-contract-consumer\n\ngo 1.27.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\n\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))
	for name, content := range map[string][]byte{"main.go": fixture, "go.sum": sums, "go.mod": []byte(module)} {
		if err := os.WriteFile(filepath.Join(moduleDirectory, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(offline bool, arguments ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", arguments...)
		command.Dir = moduleDirectory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "TMPDIR="+job)
		if offline {
			command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
		}
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("independent category command exceeded its bound", arguments)
		}
		return output, err
	}
	if output, err := run(false, "mod", "tidy"); err != nil {
		t.Fatalf("consumer module preparation: %v\n%s", err, output)
	}
	if output, err := run(true, "mod", "tidy", "-diff"); err != nil {
		t.Fatalf("consumer module graph changed offline: %v\n%s", err, output)
	}
	output, err := run(true, "list", "-mod=readonly", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".")
	if err != nil {
		t.Fatalf("consumer dependency graph: %v\n%s", err, output)
	}
	found := false
	for _, path := range strings.Fields(string(output)) {
		if path == categoryPath {
			found = true
		}
		if !allowedCategoryDependency(path) {
			t.Fatal("common consumer acquired provider, SDK or host code", path)
		}
	}
	if !found {
		t.Fatal("consumer graph omitted its category")
	}
	binary := filepath.Join(job, "consumer")
	if output, err := run(true, "build", "-mod=readonly", "-race", "-o", binary, "."); err != nil {
		t.Fatalf("independent consumer build: %v\n%s", err, output)
	}
	build, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	foundModule := false
	for _, dependency := range build.Deps {
		if dependency.Path != "github.com/frost-leo/fathomry" {
			t.Fatal("common consumer linked an external SDK module", dependency.Path)
		}
		if dependency.Replace == nil || filepath.Clean(dependency.Replace.Path) != filepath.Clean(root) {
			t.Fatal("consumer did not link the selected repository")
		}
		foundModule = true
	}
	if !foundModule {
		t.Fatal("consumer did not link the common contract module")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "TMPDIR="+job)
	output, err = command.CombinedOutput()
	if err != nil || string(output) != "orchestration public contracts composed without native I/O\n" {
		t.Fatalf("independent consumer result: %v\n%s", err, output)
	}
	internal := "github.com/frost-leo/fathomry/internal/fault"
	if err := os.WriteFile(filepath.Join(moduleDirectory, "forbidden.go"), []byte(notice+"package main\nimport _ "+strconv.Quote(internal)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	output, err = run(true, "build", "-mod=readonly", "-o", filepath.Join(job, "forbidden"), ".")
	if err == nil || !strings.Contains(string(output), "use of internal package "+internal+" not allowed") {
		t.Fatalf("external consumer imported Internal authority: %v\n%s", err, output)
	}
}

func allowedCategoryDependency(path string) bool {
	const root = "github.com/frost-leo/fathomry/"
	if path == "example.org/orchestration-contract-consumer" {
		return true
	}
	if !strings.HasPrefix(path, root) || strings.HasPrefix(path, root+"framework/") {
		return false
	}
	if strings.HasPrefix(path, root+"adapters/") {
		return path == categoryPath || path == root+"adapters/v1"
	}
	if strings.HasPrefix(path, root+"internal/") {
		for _, shared := range []string{"invocation", "resource", "fault"} {
			if path == root+"internal/"+shared || strings.HasPrefix(path, root+"internal/"+shared+"/") {
				return true
			}
		}
		return false
	}
	return true
}

func TestDependencyOracleSeparatesMechanismsFromProviders(t *testing.T) {
	const root = "github.com/frost-leo/fathomry/"
	for _, path := range []string{categoryPath, root + "adapters/v1", root + "failure/v1", root + "resource/v1", root + "settings/v1", root + "internal/invocation", root + "internal/resource", root + "internal/fault"} {
		if !allowedCategoryDependency(path) {
			t.Fatal("existing shared mechanism was incorrectly classified as a provider", path)
		}
	}
	for _, path := range []string{root + "adapters/orchestration/temporal/v1", root + "adapters/telemetry/otel/v1", root + "internal/orchestration/temporal/v1", root + "internal/telemetry/otel/v1", root + "internal/httpclient/surf/v1", root + "framework/v1", "go.temporal.io/sdk/client", "go.opentelemetry.io/otel/sdk", "github.com/nexus-rpc/sdk-go/nexus", "google.golang.org/grpc"} {
		if allowedCategoryDependency(path) {
			t.Fatal("provider or native dependency escaped the category graph guard", path)
		}
	}
}

func consumerJob(t testing.TB) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "env", "GOTMPDIR")
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local")
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
