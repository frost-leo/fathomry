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

package i18n_test

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
	write := func(name string, data []byte) {
		t.Helper()
		path := filepath.Join(directory, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", []byte(fmt.Sprintf("module example.org/i18n-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))))
	for _, name := range []string{"consumer_test.go", "component/component.go"} {
		data, err := os.ReadFile(filepath.Join("testdata/consumer", name))
		if err != nil {
			t.Fatal(err)
		}
		write(name, data)
	}
	names, err := filepath.Glob("testdata/resources/*.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		write(filepath.Join("component/messages", filepath.Base(name)), data)
	}
	run := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("independent i18n consumer exceeded its budget")
		}
		return output, err
	}
	for _, args := range [][]string{{"mod", "tidy"}, {"mod", "tidy", "-diff"}, {"test", "-count=1", "-v", "./..."}} {
		output, err := run(args...)
		if err != nil {
			t.Fatalf("consumer %v failed: %v\n%s", args, err, output)
		}
		t.Logf("%v\n%s", args, output)
	}
	output, err := run("list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/frost-leo/fathomry/i18n/v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range strings.Fields(string(output)) {
		if !strings.HasPrefix(name, "golang.org/x/text/") && name != "github.com/frost-leo/fathomry/i18n/v1" &&
			name != "github.com/frost-leo/fathomry/failure/v1" && name != "github.com/frost-leo/fathomry/settings/v1" {
			t.Fatalf("unexpected dependency %s", name)
		}
	}
	output, err = run("list", "-m", "-f", "{{.Version}}", "golang.org/x/text")
	if err != nil || strings.TrimSpace(string(output)) != "v0.41.0" {
		t.Fatal("qualified language engine changed")
	}
}
