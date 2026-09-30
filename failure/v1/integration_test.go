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
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", fmt.Sprintf("module example.org/failure-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root)))
	consumer, err := os.ReadFile("testdata/consumer/consumer_test.go")
	if err != nil {
		t.Fatal(err)
	}
	write("consumer_test.go", string(consumer))
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
	for _, args := range [][]string{{"mod", "tidy"}, {"mod", "tidy", "-diff"}, {"test", "-count=1", "-v", "."}} {
		output, err := run(args...)
		if err != nil {
			t.Fatalf("independent command %v failed: %v\n%s", args, err, output)
		}
		t.Logf("%v\n%s", args, output)
	}
	output, err := run("list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/frost-leo/fathomry/failure/v1")
	if err != nil || strings.TrimSpace(string(output)) != "github.com/frost-leo/fathomry/failure/v1" {
		t.Fatalf("failure acquired a non-stdlib production dependency: %v\n%s", err, output)
	}
	for _, pointer := range []bool{false, true} {
		star := ""
		if pointer {
			star = "*"
		}
		write("conversion_test.go", notice+fmt.Sprintf("package consumer\nimport f \"github.com/frost-leo/fathomry/failure/v1\"\nfunc forbidden(value %[1]sf.Detailed[struct{ Value string `json:\"a\"` }]) %[1]sf.Detailed[struct{ Value string `json:\"b\"` }] { return (%[1]sf.Detailed[struct{ Value string `json:\"b\"` }])(value) }\n", star))
		output, err := run("test", "-run", "^$", ".")
		if err == nil || !strings.Contains(string(output), "cannot convert") {
			t.Fatalf("tag-only conversion pointer=%v did not fail at the intended boundary: %v\n%s", pointer, err, output)
		}
	}
	write("conversion_test.go", notice+"package consumer\nimport f \"github.com/frost-leo/fathomry/failure/v1\"\ntype DetailsAlias = struct{ Value string `json:\"a\"` }\nfunc allowed(value f.Detailed[DetailsAlias]) f.Detailed[struct{ Value string `json:\"a\"` }] { return value }\n")
	if output, err := run("test", "-run", "^$", "."); err != nil {
		t.Fatalf("genuine type alias was rejected: %v\n%s", err, output)
	}
	for _, literal := range []string{"0x100000000", "0x8000000100000001"} {
		write("width_test.go", notice+"package consumer\nimport f \"github.com/frost-leo/fathomry/failure/v1\"\nconst tooWide f.Code = "+literal+"\n")
		output, err := run("test", "-run", "^$", ".")
		if err == nil || !strings.Contains(string(output), "overflows") {
			t.Fatalf("oversize numeric constant did not fail at its width boundary: %v\n%s", err, output)
		}
	}
	write("width_test.go", notice+"package consumer\nimport f \"github.com/frost-leo/fathomry/failure/v1\"\nconst composed f.Code = f.ErrorPrefix | f.Code(0x449)<<16 | 1\nvar exact uint32 = uint32(composed)\n")
	if output, err := run("test", "-run", "^$", "."); err != nil {
		t.Fatalf("32-bit constant composition was rejected: %v\n%s", err, output)
	}
}
