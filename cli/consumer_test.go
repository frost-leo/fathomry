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

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func goCommand(t *testing.T, directory string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go %v: %v\n%s", args, err, output)
	}
	return output
}

func buildBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "fathomry")
	goCommand(t, "..", "build", "-o", binary, "./cmd/fathomry")
	return binary
}

func invokeBinary(t *testing.T, binary string, args ...string) (int, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if ctx.Err() != nil {
		t.Fatal("child exceeded deadline")
	}
	status := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatal(err)
		}
		status = exit.ExitCode()
	}
	return status, stdout.String(), stderr.String()
}

func TestUnrelatedModulePublicEntries(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	module := "module example.org/unrelated-cli-consumer\n\ngo 1.27.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => " + fmt.Sprintf("%q", filepath.ToSlash(root)) + "\n"
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("testdata/consumer/main.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "main.go"), source, 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(directory, "consumer")
	goCommand(t, directory, "mod", "tidy")
	goCommand(t, directory, "mod", "tidy", "-diff")
	goCommand(t, directory, "build", "-mod=readonly", "-buildvcs=false", "-o", binary, ".")
	for _, prefix := range [][]string{nil, {"run"}} {
		for _, test := range []struct {
			args     []string
			status   int
			expected string
		}{
			{nil, 0, "Usage"}, {[]string{"help"}, 0, "Usage"}, {[]string{"--lang", "zh-CN", "--help"}, 0, "用法"},
			{[]string{"missing", "--help"}, 2, ""}, {[]string{"sample"}, 2, ""}, {[]string{"__complete"}, 2, ""},
		} {
			args := append(append([]string{}, prefix...), test.args...)
			status, out, diagnostic := invokeBinary(t, binary, args...)
			if status != test.status || !strings.Contains(out, test.expected) || status != 0 && diagnostic == "" {
				t.Fatalf("%v: status=%d out=%q stderr=%q", args, status, out, diagnostic)
			}
		}
	}
	var metadata struct {
		Replace []struct{ Old, New struct{ Path string } }
	}
	if err := json.Unmarshal(goCommand(t, directory, "mod", "edit", "-json"), &metadata); err != nil {
		t.Fatal(err)
	}
	if len(metadata.Replace) != 1 || metadata.Replace[0].Old.Path != "github.com/frost-leo/fathomry" || metadata.Replace[0].New.Path != root {
		t.Fatalf("unexpected replacements: %+v", metadata.Replace)
	}
	dependencies := goCommand(t, directory, "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".")
	allowed := map[string]bool{
		"example.org/unrelated-cli-consumer":                         true,
		"github.com/frost-leo/fathomry/cli":                          true,
		"github.com/frost-leo/fathomry/cli/internal/command/project": true,
		"github.com/spf13/cobra":                                     true,
		"github.com/spf13/pflag":                                     true,
		"golang.org/x/mod/internal/lazyregexp":                       true,
		"golang.org/x/mod/semver":                                    true,
		"golang.org/x/mod/module":                                    true,
		"golang.org/x/mod/modfile":                                   true,
	}
	for _, path := range strings.Fields(string(dependencies)) {
		if !allowed[path] {
			t.Errorf("unexpected production dependency %s", path)
		}
	}
	t.Logf("GOWORK=off; exactly one source replacement; selected non-standard packages:\n%s", bytes.TrimSpace(dependencies))
	notice := strings.SplitN(string(source), "package main", 2)[0]
	forbidden := []byte(notice + "package main\nimport _ \"github.com/frost-leo/fathomry/cli/internal/testdata/commandfamily\"\n")
	if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), forbidden, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-mod=mod", "-o", filepath.Join(directory, "forbidden"), ".")
	command.Dir = directory
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "use of internal package github.com/frost-leo/fathomry/cli/internal/testdata/commandfamily not allowed") {
		t.Fatalf("private import wrong rejection: %v %s", err, output)
	}
}

func TestActualBinaryRootHelp(t *testing.T) {
	binary := buildBinary(t)
	for _, test := range []struct {
		args     []string
		status   int
		expected string
	}{
		{nil, 0, "Commands:\n  help\tShow help for a command"}, {[]string{"help", "help"}, 0, "Show help for a command"},
		{[]string{"--lang=ZH-cn", "help"}, 0, "用法"}, {[]string{"--unknown"}, 2, ""},
		{[]string{"missing", "--help"}, 2, ""}, {[]string{"version"}, 2, ""},
	} {
		status, out, diagnostic := invokeBinary(t, binary, test.args...)
		if status != test.status || !strings.Contains(out, test.expected) || status != 0 && diagnostic == "" {
			t.Fatalf("%v: %d %q %q", test.args, status, out, diagnostic)
		}
	}
}
