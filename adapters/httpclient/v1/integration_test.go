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

package httpclient_test

import (
	"context"
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

func TestCommonDependencyBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "list", "-mod=readonly", "-deps", "github.com/frost-leo/fathomry/adapters/httpclient/v1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("dependency inspection: %v\n%s", err, output)
	}
	for _, path := range strings.Fields(string(output)) {
		if strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/database/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/framework/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/httpclient/") && path != "github.com/frost-leo/fathomry/adapters/httpclient/v1" {
			t.Fatalf("common contracts acquired an implementation dependency: %s", path)
		}
		for _, prefix := range []string{"github.com/duckdb/", "github.com/trinodb/", "github.com/go-sql-driver/", "github.com/jackc/"} {
			if strings.HasPrefix(path, prefix) {
				t.Fatalf("common contracts acquired an SDK dependency: %s", path)
			}
		}
	}
	packages, err := parser.ParseDir(token.NewFileSet(), ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range packages["httpclient"].Files {
		ast.Inspect(file, func(node ast.Node) bool {
			if declaration, ok := node.(*ast.TypeSpec); ok && declaration.Assign.IsValid() {
				t.Fatalf("public common type is an alias: %s", declaration.Name)
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
	tree, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range tree.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil || strings.Contains(path, "/internal/") || strings.Contains(path, "/database/") || strings.Contains(path, "/framework/") {
			t.Fatal("consumer bypassed common HTTP contracts")
		}
	}
	baseOutput, err := exec.Command("go", "env", "GOTMPDIR").Output()
	if err != nil {
		t.Fatal(err)
	}
	base := strings.TrimSpace(string(baseOutput))
	if base == "" {
		base = filepath.Join(os.TempDir(), "fathomry")
	}
	jobs := filepath.Join(filepath.Dir(base), "jobs")
	if err := os.MkdirAll(jobs, 0700); err != nil {
		t.Fatal(err)
	}
	job, err := os.MkdirTemp(jobs, "build.")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(job); err != nil {
			t.Error(err)
		}
	})
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"main.go": fixture, "go.sum": sums,
		"go.mod": []byte(fmt.Sprintf("module example.org/httpclient-common-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
	} {
		if err := os.WriteFile(filepath.Join(job, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(offline bool, args ...string) []byte {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = job
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
		if offline {
			command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
		}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("independent common consumer: %v\n%s", err, output)
		}
		return output
	}
	run(false, "mod", "tidy")
	run(true, "mod", "tidy", "-diff")
	binary := filepath.Join(job, "consumer")
	run(true, "build", "-mod=readonly", "-race", "-o", binary, ".")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, binary).CombinedOutput()
	if err != nil || string(output) != "httpclient public contracts composed without native I/O\n" {
		t.Fatalf("common consumer: %v\n%s", err, output)
	}
}
