/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	"context"
	"debug/buildinfo"
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

// This fixture is compiled in a genuinely independent module. No Internal or
// native SDK import is available to its test source, even though the adapter
// intentionally depends on its selected Internal integration.
func TestIndependentConsumer(t *testing.T) {
	for _, fixture := range []struct{ directory, packageName string }{{"consumer", "consumer"}} {
		t.Run(fixture.directory, func(t *testing.T) { testIndependentConsumer(t, fixture.directory, fixture.packageName) })
	}
}

func testIndependentConsumer(t *testing.T, fixture, packageName string) {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := consumerJob(t)
	fixtureRoot := filepath.Join("testdata", fixture)
	goFiles := 0
	err = filepath.WalkDir(fixtureRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(fixtureRoot, path)
		if err != nil {
			return err
		}
		target := filepath.Join(directory, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected consumer fixture type: %s", relative)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".go" {
			goFiles++
			syntax, err := parser.ParseFile(token.NewFileSet(), path, data, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, item := range syntax.Imports {
				imported, err := strconv.Unquote(item.Path.Value)
				if err != nil || strings.Contains(imported, "/internal/") || strings.Contains(imported, "larksuite") {
					return fmt.Errorf("consumer bypasses public API: %s", imported)
				}
			}
		}
		return os.WriteFile(target, data, 0600)
	})
	if err != nil || goFiles == 0 {
		t.Fatal("missing or invalid consumer", err)
	}
	module := fmt.Sprintf("module example.org/larkconsumer\n\ngo 1.27.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))
	if err = os.WriteFile(filepath.Join(directory, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "go.sum"), sums, 0600); err != nil {
		t.Fatal(err)
	}
	run := func(offline bool, args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
		if offline {
			command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
		}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("consumer go %v: %v\n%s", args, err, output)
		}
		return output
	}
	run(false, "mod", "tidy")
	run(true, "mod", "tidy", "-diff")
	binary := filepath.Join(directory, "consumer.test")
	run(true, "test", "-c", "-race", "-mod=readonly", "-o", binary, ".")
	metadata, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, dependency := range metadata.Deps {
		if dependency.Path == "github.com/larksuite/oapi-sdk-go/v3" {
			found = dependency.Version == "v3.12.1" && dependency.Replace == nil && dependency.Sum == "h1:5Zu2z+meHR/ETyTSWOB6gyRN0FIiImXdq/ReQGlGYH8="
		}
	}
	if !found {
		t.Fatal("consumer selected a different native SDK")
	}
	dependencies := string(run(true, "list", "-deps", "-test", "-mod=readonly", "."))
	for _, dependency := range strings.Fields(dependencies) {
		if strings.Contains(dependency, "notification/mail") || strings.Contains(dependency, "wneessen") {
			t.Fatal("Lark imported unrelated notification provider")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.v", "-test.count=1")
	command.Dir = directory
	output, err := command.CombinedOutput()
	t.Logf("independent race executable:\n%s", output)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []struct{ source, want string }{
		{"package consumer\nimport p \"github.com/frost-leo/fathomry/adapters/notification/lark/v1\"\nvar _ = (*p.Client).Close\n", "has no field or method Close"},
		{"package consumer\nimport p \"github.com/frost-leo/fathomry/adapters/notification/lark/v1\"\nvar _ = p.Owner(p.Handle{})\n", "cannot convert"},
		{"package consumer\nimport _ \"github.com/frost-leo/fathomry/internal/notification/lark/v3\"\n", "use of internal package"},
	} {
		invalidSource := strings.Replace(invalid.source, "package consumer", "package "+packageName, 1)
		if err = os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(invalidSource), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(ctx, "go", "test", "-c", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), ".")
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
		output, err := command.CombinedOutput()
		if err == nil || !strings.Contains(string(output), invalid.want) {
			t.Fatalf("authority negative control: %v\n%s", err, output)
		}
	}
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
	if err = os.MkdirAll(jobs, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(jobs, "build.")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	return directory
}
