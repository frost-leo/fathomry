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

package doris

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
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

func TestIndependentPublicConsumer(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	input, err := os.ReadFile("testdata/consumer/main.go")
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", input, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range syntax.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(path, "/internal/") || strings.HasPrefix(path, "github.com/go-sql-driver/") {
			t.Fatal("consumer escaped public boundary")
		}
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"main.go": input, "go.sum": sums,
		"go.mod": []byte(fmt.Sprintf("module example.org/doris-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
	} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
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
		return command.CombinedOutput()
	}
	if output, err := run(false, "mod", "tidy"); err != nil {
		t.Fatalf("prepare: %v\n%s", err, output)
	}
	if output, err := run(true, "mod", "tidy", "-diff"); err != nil {
		t.Fatalf("readonly graph: %v\n%s", err, output)
	}
	if output, err := run(true, "list", "-mod=readonly", "-deps", "."); err != nil {
		t.Fatalf("dependencies: %v\n%s", err, output)
	} else {
		for _, unrelated := range []string{"internal/database/mysql/", "minio-go", "iceberg", "duckdb", "trino", "adapters/database/", "adapters/broker"} {
			if strings.Contains(string(output), unrelated) {
				t.Fatal("unrelated provider dependency", unrelated)
			}
		}
	}
	binary := filepath.Join(directory, "consumer")
	if output, err := run(true, "build", "-mod=readonly", "-race", "-o", binary, "."); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	build, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, dependency := range build.Deps {
		if dependency.Path == "github.com/go-sql-driver/mysql" {
			found = dependency.Version == "v1.10.1" && dependency.Replace == nil
		}
	}
	if !found {
		t.Fatal("selected driver absent")
	}
	firstPeer, secondPeer := newSQLPeer(t, false), newSQLPeer(t, false)
	first, second := firstPeer.options(), secondPeer.options()
	first.Name = "first"
	second.Name = "second"
	first.MaxRows = 1
	second.MaxRows = 1
	first.MaxPageRows = 2
	second.MaxPageRows = 2
	body, err := json.Marshal(struct{ First, Second Settings }{first, second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Stdin = bytes.NewReader(body)
	output, err := command.CombinedOutput()
	if err != nil || string(output) != "doris direct and Framework Fixed/Follow consumers passed\n" {
		t.Fatalf("consumer: %v\n%s", err, output)
	}
	if firstPeer.queries.Load() != 4 || secondPeer.queries.Load() != 2 {
		t.Fatal("source/dispatch count changed", firstPeer.queries.Load(), secondPeer.queries.Load())
	}
}
