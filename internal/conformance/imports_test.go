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
	"os"
	"os/exec"
	"path/filepath"
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
	write("consumer.go", []byte(notice+"package consumer\n"))
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
	t.Logf("independent module smoke compilation (no public API):\n%s", output)
	for _, name := range []string{"fault", "resource", "invocation", "compatibility", "conformance"} {
		t.Run("reject-internal-"+name, func(t *testing.T) {
			path := "github.com/frost-leo/fathomry/internal/" + name
			write("forbidden.go", []byte(notice+"package consumer\nimport _ "+fmt.Sprintf("%q", path)+"\n"))
			output, err := run(directory, "test", "-mod=mod", "-count=1", "./...")
			if err == nil || !strings.Contains(string(output), "use of internal package "+path+" not allowed") {
				t.Fatalf("wrong internal import rejection: %v\n%s", err, output)
			}
		})
	}
	for _, name := range []string{"source", "operation", "compatibility", "failure"} {
		t.Run("withdrawn-"+name, func(t *testing.T) {
			path := "github.com/frost-leo/fathomry/" + name
			write("forbidden.go", []byte(notice+"package consumer\nimport _ "+fmt.Sprintf("%q", path)+"\n"))
			output, err := run(directory, "test", "-mod=mod", "-count=1", "./...")
			if err == nil || !strings.Contains(string(output), path) || !strings.Contains(string(output), "does not contain package") {
				t.Fatalf("withdrawn package was not rejected as absent: %v\n%s", err, output)
			}
		})
	}
	output, err = run(root, "list", "./...")
	if err != nil {
		t.Fatal("package inventory could not be inspected")
	}
	for _, path := range strings.Fields(string(output)) {
		if !strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") {
			t.Errorf("unexpected public package: %s", path)
		}
	}
	output, err = run(root, "list", "-deps", "./internal/fault", "./internal/resource", "./internal/invocation", "./internal/compatibility")
	if err != nil {
		t.Fatal("internal dependency check failed")
	}
	for _, path := range strings.Fields(string(output)) {
		if path == "testing" || path == "github.com/frost-leo/fathomry/internal/conformance" ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/") && !strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") {
			t.Errorf("technical mechanisms depend on framework errors or test support: %s", path)
		}
	}
}
