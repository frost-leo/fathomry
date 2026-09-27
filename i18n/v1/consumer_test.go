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
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestIndependentModule(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("race-built independent consumer targets Linux/amd64")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	module := fmt.Sprintf("module example.org/independent-i18n\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))
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
	if err := os.Mkdir(filepath.Join(directory, "resources"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"en", "zh-CN", "ru", "ar", "custom"} {
		data, err := os.ReadFile("testdata/resources/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "resources", name+".json"), data, 0600); err != nil {
			t.Fatal(err)
		}
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
			t.Fatal("consumer deadline")
		}
		return output, err
	}
	for _, args := range [][]string{{"mod", "tidy"}, {"mod", "tidy", "-diff"}, {"test", "-race", "-count=1", "-mod=readonly", "-v", "."}} {
		output, err := run(args...)
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
		t.Logf("%v\n%s", args, output)
	}
	output, err := run("list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/frost-leo/fathomry/i18n/v1")
	if err != nil {
		t.Fatal(err, string(output))
	}
	actual := strings.Fields(string(output))
	expected := []string{
		"github.com/frost-leo/fathomry/failure/v1", "github.com/frost-leo/fathomry/i18n/v1",
		"golang.org/x/text/internal/tag", "golang.org/x/text/internal/language", "golang.org/x/text/internal/language/compact", "golang.org/x/text/language",
		"golang.org/x/text/internal/catmsg", "golang.org/x/text/internal/stringset", "golang.org/x/text/internal/number", "golang.org/x/text/internal",
		"golang.org/x/text/message/catalog", "golang.org/x/text/feature/plural",
	}
	slices.Sort(actual)
	slices.Sort(expected)
	if !slices.Equal(actual, expected) {
		t.Fatalf("unexpected production graph: %v", actual)
	}
	t.Logf("exact production graph: %v", actual)
	output, err = run("list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/frost-leo/fathomry/failure/v1")
	if err != nil || strings.TrimSpace(string(output)) != "github.com/frost-leo/fathomry/failure/v1" {
		t.Fatal("failure dependency direction", err, string(output))
	}
	data, err := os.ReadFile(filepath.Join(directory, "go.mod"))
	if err != nil || strings.Count(string(data), "replace ") != 1 || !strings.Contains(string(data), "replace github.com/frost-leo/fathomry => ") {
		t.Fatal("unexpected replacements")
	}
	forbidden := "package consumer\nimport _ \"github.com/frost-leo/fathomry/internal/fault\"\n"
	if err := os.WriteFile(filepath.Join(directory, "forbidden_test.go"), []byte(forbidden), 0600); err != nil {
		t.Fatal(err)
	}
	output, err = run("test", "-mod=readonly", ".")
	if err == nil || !strings.Contains(string(output), "use of internal package github.com/frost-leo/fathomry/internal/fault not allowed") {
		t.Fatalf("private import accepted/wrong rejection: %v %s", err, output)
	}
}
