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

package database_test

import (
	"context"
	"fmt"
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

func TestIndependentCombinedConsumer(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	fixture, err := os.ReadFile("testdata/consumer/main.go")
	if err != nil {
		t.Fatal(err)
	}
	notice, _, _ := strings.Cut(string(fixture), "package main")
	syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	imports := make(map[string]bool)
	for _, imported := range syntax.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(path, "/internal/") || strings.Contains(path, "/framework/") {
			t.Fatal("combined consumer bypassed public Adapters")
		}
		imports[path] = true
	}
	for _, provider := range []string{"postgres", "mysql"} {
		if !imports["github.com/frost-leo/fathomry/adapters/database/"+provider+"/v1"] {
			t.Fatal("consumer did not select both providers")
		}
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"main.go": fixture,
		"go.sum":  sums,
		"go.mod":  []byte(fmt.Sprintf("module example.org/database-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
	} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("combined consumer build exceeded bound")
		}
		return output, err
	}
	if output, err := run("mod", "tidy"); err != nil {
		t.Fatalf("consumer module preparation: %v\n%s", err, output)
	}
	binary := filepath.Join(directory, "consumer")
	if output, err := run("build", "-mod=readonly", "-race", "-o", binary, "."); err != nil {
		t.Fatalf("combined consumer build: %v\n%s", err, output)
	}
	if output, err := run("mod", "tidy", "-diff"); err != nil {
		t.Fatalf("combined consumer graph unstable: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, binary).CombinedOutput(); err != nil || string(output) != "database consumers composed without service I/O\n" {
		t.Fatalf("combined consumer execution: %v\n%s", err, output)
	}
	for _, kind := range []string{"Handle", "Result", "Statement", "Transaction"} {
		t.Run("cross_provider_"+kind, func(t *testing.T) {
			content := notice + fmt.Sprintf("package main\nimport p \"github.com/frost-leo/fathomry/adapters/database/postgres/v1\"\nimport m \"github.com/frost-leo/fathomry/adapters/database/mysql/v1\"\nvar _ = p.%[1]s(m.%[1]s{})\n", kind)
			if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if output, err := run("build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), "cannot convert") {
				t.Fatalf("cross-provider authority converted: %v\n%s", err, output)
			}
		})
	}
	t.Log("Separate provider consumers exercise protocol I/O; this combined executable qualifies only co-composition and local ownership.")
}

func TestCommonDependencyBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "list", "-deps", "github.com/frost-leo/fathomry/adapters/database/v1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("dependency inspection: %v\n%s", err, output)
	}
	for _, path := range strings.Fields(string(output)) {
		if strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/database/postgres/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/database/mysql/") ||
			strings.HasPrefix(path, "github.com/jackc/") || path == "github.com/go-sql-driver/mysql" {
			t.Fatalf("common public contracts acquired a native dependency: %s", path)
		}
	}
}
