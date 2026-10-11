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

package iceberg_test

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

func TestIndependentIcebergConsumer(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("testdata/consumer/main.go")
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range syntax.Imports {
		path, _ := strconv.Unquote(item.Path.Value)
		if strings.Contains(path, "/internal/") {
			t.Fatal("consumer imports internal authority")
		}
	}
	job := consumerJob(t)
	module := fmt.Sprintf("module example.org/iceberg-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\nreplace github.com/apache/iceberg-go => %q\n", root, filepath.Join(root, "third_party/iceberg-go"))
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"go.mod": []byte(module), "go.sum": sums, "main.go": fixture} {
		if err = os.WriteFile(filepath.Join(job, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.CopyFS(filepath.Join(job, "data"), os.DirFS(filepath.Join(root, "internal/tableformat/iceberg/v0/testdata/format2-v060"))); err != nil {
		t.Fatal(err)
	}
	run := func(offline bool, args ...string) []byte {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = job
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-p=1 -buildvcs=false")
		if offline {
			cmd.Env = append(cmd.Env, "GOPROXY=off")
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("consumer %v: %v\n%s", args, err, out)
		}
		return out
	}
	run(false, "mod", "tidy")
	run(true, "mod", "tidy", "-diff")
	binary := filepath.Join(job, "consumer")
	run(true, "build", "-mod=readonly", "-race", "-o", binary, ".")
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, dep := range info.Deps {
		if dep.Path == "github.com/apache/iceberg-go" {
			found = dep.Version == "v0.7.0" && dep.Replace != nil && dep.Replace.Path == filepath.Join(root, "third_party/iceberg-go")
		}
	}
	if !found {
		t.Fatal("consumer did not select maintained v0.7.0")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	out, err := command.CombinedOutput()
	if err != nil || string(out) != "iceberg independent direct and Fixed/Follow passed\n" {
		t.Fatalf("independent execution: %v\n%s", err, out)
	}
}
