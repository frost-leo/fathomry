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

package project_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/cli"
	"golang.org/x/mod/modfile"
)

type poisonReader struct{}

func (poisonReader) Read([]byte) (int, error) { panic("new must not read stdin") }

func sourceRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func arguments(target, source, module string) []string {
	return []string{"new", target, "--module", module, "--fathomry-source", source}
}

func invoke(ctx context.Context, args []string, output io.Writer) (int, error, string) {
	var diagnostic bytes.Buffer
	status, err := cli.Run(ctx, args, cli.Streams{Stdin: poisonReader{}, Stdout: output, Stderr: &diagnostic})
	return status, err, diagnostic.String()
}

func absent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected target: %s: %v", path, err)
	}
}

func generated(t *testing.T, path string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{"go.mod": true, "main.go": true, "README.md": true, ".gitignore": true}
	if len(entries) != len(expected) {
		t.Fatalf("expected exactly four files, found %v", entries)
	}
	files := map[string]string{}
	for _, entry := range entries {
		if !expected[entry.Name()] || !entry.Type().IsRegular() {
			t.Fatalf("unexpected entry: %s", entry.Name())
		}
		data, err := os.ReadFile(filepath.Join(path, entry.Name()))
		if err != nil || len(data) == 0 {
			t.Fatalf("incomplete file %s: %v", entry.Name(), err)
		}
		files[entry.Name()] = string(data)
	}
	return files
}

func TestPublicCommandEarlyPaths(t *testing.T) {
	root := sourceRoot(t)
	for _, test := range []struct {
		name   string
		args   func(string) []string
		status int
	}{
		{"new-help", func(target string) []string { return []string{"new", target, "--fathomry-source=/missing", "--help"} }, 0},
		{"help-new", func(string) []string { return []string{"help", "new"} }, 0},
		{"missing-input", func(string) []string { return []string{"new"} }, 2},
		{"missing-module", func(target string) []string { return []string{"new", target, "--fathomry-source", root} }, 2},
		{"missing-source", func(target string) []string { return []string{"new", target, "--module=example.org/app"} }, 2},
		{"empty-source", func(target string) []string { return arguments(target, "", "example.org/app") }, 2},
		{"invalid-module", func(target string) []string { return arguments(target, root, "example.org/bad name") }, 2},
		{"source-collision", func(target string) []string { return arguments(target, root, "github.com/frost-leo/fathomry") }, 2},
		{"cli-collision", func(target string) []string { return arguments(target, root, "github.com/frost-leo/fathomry/cli") }, 2},
		{"extra-operand", func(target string) []string { return append(arguments(target, root, "example.org/app"), "extra") }, 2},
		{"unknown-option", func(target string) []string { return append(arguments(target, root, "example.org/app"), "--unknown") }, 2},
		{"create-is-not-alias", func(target string) []string { return []string{"create", target} }, 2},
		{"init-is-not-alias", func(target string) []string { return []string{"init", target} }, 2},
		{"bad-source", func(target string) []string {
			return arguments(target, "/missing/SOURCE-CANARY\x1b[31m\n", "example.org/app")
		}, 1},
		{"wrong-source", func(target string) []string { return arguments(target, t.TempDir(), "example.org/app") }, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "app")
			var output bytes.Buffer
			status, err, diagnostic := invoke(context.Background(), test.args(target), &output)
			if status != test.status || (err == nil) != (status == 0) {
				t.Fatalf("status=%d err=%v output=%q diagnostic=%q", status, err, output.String(), diagnostic)
			}
			if strings.Contains(diagnostic, "SOURCE-CANARY") || strings.ContainsAny(diagnostic, "\x1b\r") {
				t.Fatalf("unsafe diagnostic: %q", diagnostic)
			}
			absent(t, target)
		})
	}
	target := filepath.Join(t.TempDir(), "canceled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, err, _ := invoke(ctx, arguments(target, "/missing", "example.org/app"), io.Discard)
	if status != 130 || !errors.Is(err, context.Canceled) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-cancel inspected source: %d %v", status, err)
	}
	absent(t, target)
}

type outputFunc func([]byte) (int, error)

func (write outputFunc) Write(data []byte) (int, error) { return write(data) }

func TestPublicDeliveryFailureKeepsCompleteProject(t *testing.T) {
	sentinel := errors.New("output failure")
	for _, mode := range []string{"failed-output", "short-output", "late-cancel", "cancel-and-output"} {
		t.Run(mode, func(t *testing.T) {
			root := sourceRoot(t)
			parent := t.TempDir()
			control, target := filepath.Join(parent, "control"), filepath.Join(parent, "app")
			status, err, _ := invoke(context.Background(), arguments(control, root, "example.org/app"), io.Discard)
			if status != 0 || err != nil {
				t.Fatal("control generation failed", status, err)
			}
			expected := generated(t, control)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writes := 0
			output := outputFunc(func(data []byte) (int, error) {
				writes++
				if mode == "late-cancel" || mode == "cancel-and-output" {
					cancel()
				}
				switch mode {
				case "failed-output", "cancel-and-output":
					return 0, sentinel
				case "short-output":
					return len(data) - 1, nil
				default:
					return len(data), nil
				}
			})
			status, err, diagnostic := invoke(ctx, arguments(target, root, "example.org/app"), output)
			if writes != 1 || status == 0 || err == nil {
				t.Fatalf("delivery: %d %v writes=%d", status, err, writes)
			}
			if mode == "late-cancel" {
				if status != 130 || !errors.Is(err, context.Canceled) {
					t.Fatal("late cancellation lost", status, err)
				}
			} else if status != 1 {
				t.Fatal("independent failure must outrank cancellation", status, err)
			}
			if (mode == "failed-output" || mode == "cancel-and-output") && !errors.Is(err, sentinel) {
				t.Fatal("output cause lost", err)
			}
			if mode == "short-output" && !errors.Is(err, io.ErrShortWrite) {
				t.Fatal("short output lost", err)
			}
			if strings.Contains(diagnostic, "not created") || strings.Contains(diagnostic, "rollback") {
				t.Fatalf("false effect claim: %q", diagnostic)
			}
			actual := generated(t, target)
			for name, data := range expected {
				if actual[name] != data {
					t.Errorf("completed %s differs after failed delivery", name)
				}
			}
			status, err, _ = invoke(context.Background(), arguments(target, root, "example.org/retry"), io.Discard)
			if status != 1 || !errors.Is(err, os.ErrExist) {
				t.Fatal("existing target retry accepted", status, err)
			}
			for name, data := range generated(t, target) {
				if data != actual[name] {
					t.Errorf("retry overwrote %s", name)
				}
			}
		})
	}
}

func TestPublicInvocationsIsolateLanguageAndModule(t *testing.T) {
	root, parent := sourceRoot(t), t.TempDir()
	const count = 16
	var group sync.WaitGroup
	for index := range count {
		group.Go(func() {
			target := filepath.Join(parent, fmt.Sprintf("app%d", index))
			module := fmt.Sprintf("example.org/project%d", index)
			language := "en"
			if index%2 == 1 {
				language = "zh-CN"
			}
			args := append(arguments(target, root, module), "--lang="+language)
			var output bytes.Buffer
			status, err, diagnostic := invoke(context.Background(), args, &output)
			if status != 0 || err != nil || diagnostic != "" {
				t.Errorf("%d: %d %v %q", index, status, err, diagnostic)
				return
			}
			expected := "Project files created."
			if language == "zh-CN" {
				expected = "项目文件已创建。"
			}
			if !strings.HasPrefix(output.String(), expected) {
				t.Errorf("%d: wrong language: %q", index, output.String())
			}
		})
	}
	group.Wait()
	var baseline map[string]string
	for index := range count {
		files := generated(t, filepath.Join(parent, fmt.Sprintf("app%d", index)))
		metadata, err := modfile.Parse("go.mod", []byte(files["go.mod"]), nil)
		if err != nil || metadata.Module.Mod.Path != fmt.Sprintf("example.org/project%d", index) {
			t.Fatalf("cross-invocation module leak: %d %v", index, err)
		}
		if err := metadata.AddModuleStmt("example.org/normalized"); err != nil {
			t.Fatal(err)
		}
		normalized, err := metadata.Format()
		if err != nil {
			t.Fatal(err)
		}
		files["go.mod"] = string(normalized)
		if baseline == nil {
			baseline = files
		}
		for name, data := range files {
			if baseline[name] != data {
				t.Errorf("language changed generated %s", name)
			}
		}
	}
}

func TestPublicSameTargetCompetition(t *testing.T) {
	root := sourceRoot(t)
	target := filepath.Join(t.TempDir(), "shared")
	var group sync.WaitGroup
	results := make(chan int, 8)
	start := make(chan struct{})
	for index := range 8 {
		group.Go(func() {
			<-start
			status, err, _ := invoke(context.Background(), arguments(target, root, fmt.Sprintf("example.org/project%d", index)), io.Discard)
			if status != 0 && (status != 1 || !errors.Is(err, os.ErrExist)) {
				t.Errorf("wrong concurrent refusal: %d %v", status, err)
			}
			results <- status
		})
	}
	close(start)
	group.Wait()
	close(results)
	winners := 0
	for status := range results {
		if status == 0 {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("winners=%d", winners)
	}
	generated(t, target)
}

func TestFailedDiagnosticRetainsSourceAndWriterCauses(t *testing.T) {
	target := filepath.Join(t.TempDir(), "app")
	sentinel := errors.New("diagnostic failure")
	writes := 0
	status, err := cli.Run(context.Background(), arguments(target, "/missing/SOURCE-CANARY", "example.org/app"), cli.Streams{
		Stdin: poisonReader{}, Stdout: io.Discard,
		Stderr: outputFunc(func(data []byte) (int, error) {
			writes++
			if bytes.Contains(data, []byte("SOURCE-CANARY")) {
				t.Fatal("raw source error leaked")
			}
			return 0, sentinel
		}),
	})
	if status != 1 || !errors.Is(err, os.ErrNotExist) || !errors.Is(err, sentinel) || writes != 1 {
		t.Fatalf("diagnostic: %d %v writes=%d", status, err, writes)
	}
	absent(t, target)
}
