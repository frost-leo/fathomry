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

package failure_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func consumerCommand(t *testing.T, directory string, args ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("consumer deadline: %s", output)
	}
	return output, err
}

func TestIndependentModule(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	module := fmt.Sprintf("module example.org/independent-failure-consumer\n\ngo 1.27.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("testdata/consumer/consumer_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "consumer_test.go"), source, 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"test", "-race", "-count=1", "-mod=readonly", "-timeout=30s", "-v", "."},
		{"mod", "tidy", "-diff"},
	} {
		output, err := consumerCommand(t, directory, args...)
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
		t.Logf("%v\n%s", args, output)
	}
	dependencies, err := consumerCommand(t, directory, "list", "-mod=readonly", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/frost-leo/fathomry/failure/v1")
	if err != nil || strings.TrimSpace(string(dependencies)) != "github.com/frost-leo/fathomry/failure/v1" {
		t.Fatalf("production isolation: %v\n%s", err, dependencies)
	}
	t.Logf("production non-stdlib closure: %s", dependencies)
	forbidden := "package consumer\nimport _ \"github.com/frost-leo/fathomry/internal/fault\"\n"
	if err := os.WriteFile(filepath.Join(directory, "forbidden_test.go"), []byte(forbidden), 0600); err != nil {
		t.Fatal(err)
	}
	output, err := consumerCommand(t, directory, "test", "-mod=readonly", ".")
	if err == nil || !strings.Contains(string(output), "use of internal package github.com/frost-leo/fathomry/internal/fault not allowed") {
		t.Fatalf("wrong private-import control: %v\n%s", err, output)
	}
}
