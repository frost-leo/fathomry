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

package temporal

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
	"strconv"
	"strings"
	"testing"
	"time"
)

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

func TestConsumerJobUsesSelectedTemporaryDirectory(t *testing.T) {
	for _, selection := range []string{"configured", "system-default"} {
		t.Run(selection, func(t *testing.T) {
			base := t.TempDir()
			t.Setenv("GOENV", "off")
			t.Setenv("GOTMPDIR", "")
			t.Setenv("TMPDIR", base)
			if selection == "configured" {
				temporary := filepath.Join(base, "go-temporary")
				if err := os.Mkdir(temporary, 0700); err != nil {
					t.Fatal(err)
				}
				t.Setenv("GOTMPDIR", temporary)
			}
			directory := consumerJob(t)
			if filepath.Dir(directory) != filepath.Join(base, "jobs") {
				t.Fatal("consumer build ignored selected temporary directory", directory)
			}
		})
	}
}

func TestIndependentConsumers(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	common, err := os.ReadFile("testdata/common.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"direct", "framework"} {
		t.Run(variant, func(t *testing.T) {
			fixture, err := os.ReadFile(filepath.Join("testdata", variant, "main.go"))
			if err != nil {
				t.Fatal(err)
			}
			for _, data := range [][]byte{fixture, common} {
				parsed, err := parser.ParseFile(token.NewFileSet(), "fixture.go", data, parser.ImportsOnly)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range parsed.Imports {
					name, err := strconv.Unquote(entry.Path.Value)
					if err != nil || strings.Contains(name, "/internal/") || variant == "direct" && strings.Contains(name, "/framework/") {
						t.Fatal("consumer bypassed public boundary", name)
					}
				}
			}
			directory := consumerJob(t)
			module := fmt.Sprintf("module example.org/temporal-%s-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\nreplace go.temporal.io/sdk => %q\n", variant, root, filepath.Join(root, "third_party/temporal-sdk"))
			for name, data := range map[string][]byte{"main.go": fixture, "common.go": common, "go.mod": []byte(module), "go.sum": sums} {
				if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			run := func(offline bool, args ...string) []byte {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()
				command := exec.CommandContext(ctx, "go", args...)
				command.Dir = directory
				command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
				if offline {
					command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
				}
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("%q: %v\n%s", args, err, output)
				}
				return output
			}
			run(false, "mod", "tidy")
			run(true, "mod", "tidy", "-diff")
			selected := run(true, "list", "-mod=readonly", "-m", "-json", "go.temporal.io/sdk")
			var sdk struct {
				Version string
				Replace *struct{ Path string }
			}
			if json.Unmarshal(selected, &sdk) != nil || sdk.Version != "v1.49.0" || sdk.Replace == nil || sdk.Replace.Path != filepath.Join(root, "third_party/temporal-sdk") {
				t.Fatal("wrong independent SDK selection")
			}
			deps := run(true, "list", "-mod=readonly", "-deps", ".")
			for _, name := range strings.Fields(string(deps)) {
				if variant == "direct" && strings.HasPrefix(name, "github.com/frost-leo/fathomry/framework/") ||
					strings.HasPrefix(name, "github.com/frost-leo/fathomry/internal/database/") ||
					strings.HasPrefix(name, "github.com/frost-leo/fathomry/adapters/telemetry/") {
					t.Fatal("unrelated runtime provider acquired", name)
				}
			}
			binary := filepath.Join(directory, "consumer")
			run(true, "build", "-mod=readonly", "-race", "-o", binary, ".")
			info, err := buildinfo.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, dep := range info.Deps {
				if dep.Path == "go.temporal.io/sdk" {
					found = dep.Version == "v1.49.0" && dep.Replace != nil && dep.Replace.Path == filepath.Join(root, "third_party/temporal-sdk")
				}
			}
			if !found {
				t.Fatal("built consumer did not retain exact repaired SDK")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			command := exec.CommandContext(ctx, binary)
			command.Dir = directory
			output, err := command.CombinedOutput()
			if err != nil || string(output) != "temporal "+variant+" consumer passed\n" {
				t.Fatalf("consumer: %v\n%s", err, output)
			}
		})
	}
}
