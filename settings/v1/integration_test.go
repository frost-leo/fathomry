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

package settings_test

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

func TestIndependentModule(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	header, err := os.ReadFile(filepath.Join(root, ".github/LICENSE_HEADER"))
	if err != nil {
		t.Fatal(err)
	}
	notice := "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(header)), "\n", "\n * ") + "\n */\n"
	notice = strings.ReplaceAll(notice, "\n * \n", "\n *\n")
	write := func(name, content string) {
		t.Helper()
		path := filepath.Join(directory, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", fmt.Sprintf("module example.org/settings-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root)))
	for _, name := range []string{"consumer_test.go", "component/preferences.go"} {
		data, err := os.ReadFile(filepath.Join("testdata/consumer", name))
		if err != nil {
			t.Fatal(err)
		}
		write(name, string(data))
	}
	run := func(args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("independent consumer exceeded its execution budget")
		}
		return output, err
	}
	for _, args := range [][]string{{"mod", "tidy"}, {"mod", "tidy", "-diff"}, {"test", "-count=1", "-v", "./..."}} {
		output, err := run(args...)
		if err != nil {
			t.Fatalf("independent command %v failed: %v\n%s", args, err, output)
		}
		t.Logf("%v\n%s", args, output)
	}
	output, err := run("list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/frost-leo/fathomry/settings/v1")
	want := "github.com/frost-leo/fathomry/failure/v1\ngithub.com/frost-leo/fathomry/settings/v1"
	if err != nil || strings.TrimSpace(string(output)) != want {
		t.Fatalf("settings acquired a non-foundation dependency: %v\n%s", err, output)
	}

	for _, handle := range []string{"Snapshot", "Store"} {
		for _, pointer := range []bool{false, true} {
			star := ""
			if pointer {
				star = "*"
			}
			write("conversion_test.go", notice+fmt.Sprintf("package consumer\nimport s \"github.com/frost-leo/fathomry/settings/v1\"\nfunc forbidden(value %[1]ss.%[2]s[struct{ Value string `json:\"a\"` }]) %[1]ss.%[2]s[struct{ Value string `json:\"b\"` }] { return (%[1]ss.%[2]s[struct{ Value string `json:\"b\"` }])(value) }\n", star, handle))
			output, err := run("test", "-run", "^$", ".")
			if err == nil || !strings.Contains(string(output), "cannot convert") {
				t.Fatalf("tag-only %s conversion pointer=%v crossed the type boundary: %v\n%s", handle, pointer, err, output)
			}
		}
	}
	write("conversion_test.go", notice+"package consumer\nimport s \"github.com/frost-leo/fathomry/settings/v1\"\ntype Alias = struct{ Value string `json:\"a\"` }\nfunc sameSnapshot(value s.Snapshot[Alias]) s.Snapshot[struct{ Value string `json:\"a\"` }] { return value }\nfunc sameStore(value s.Store[Alias]) s.Store[struct{ Value string `json:\"a\"` }] { return value }\n")
	if output, err := run("test", "-run", "^$", "."); err != nil {
		t.Fatalf("genuine aliases were rejected: %v\n%s", err, output)
	}
	write("reader_test.go", notice+"package consumer\nimport s \"github.com/frost-leo/fathomry/settings/v1\"\nfunc forbidden(reader s.Reader, snapshot s.Snapshot[int]) error { return reader.Publish(snapshot) }\n")
	output, err = run("test", "-run", "^$", ".")
	if err == nil || !strings.Contains(string(output), "has no field or method Publish") {
		t.Fatalf("reader acquired write authority or failed for another reason: %v\n%s", err, output)
	}
	if err := os.Remove(filepath.Join(directory, "reader_test.go")); err != nil {
		t.Fatal(err)
	}
	if output, err := run("test", "-count=1", "./..."); err != nil {
		t.Fatalf("consumer failed after compile-boundary controls: %v\n%s", err, output)
	}
}
