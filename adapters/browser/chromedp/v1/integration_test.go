/*
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

package chromedp

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

func TestIndependentConsumerExplicitNativeReplacement(t *testing.T) {
	for _, fixture := range []struct{ name, path, output string }{
		{"direct", "testdata/consumer/main.go", "browser public consumer passed\n"},
		{"framework", "testdata/framework/main.go", "browser framework consumer passed\n"},
	} {
		t.Run(fixture.name, func(t *testing.T) { independentConsumer(t, fixture.path, fixture.output, false) })
	}
}

func TestIndependentConsumerPublishedNativeModule(t *testing.T) {
	for _, fixture := range []struct{ name, path, output string }{
		{"direct", "testdata/consumer/main.go", "browser public consumer passed\n"},
		{"framework", "testdata/framework/main.go", "browser framework consumer passed\n"},
	} {
		t.Run(fixture.name, func(t *testing.T) { independentConsumer(t, fixture.path, fixture.output, true) })
	}
}

func independentConsumer(t *testing.T, fixturePath, expectedOutput string, published bool) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range syntax.Imports {
		path, err := strconv.Unquote(entry.Path.Value)
		if err != nil || strings.Contains(path, "/internal/") {
			t.Fatal("consumer bypasses public boundary")
		}
	}
	directory := consumerJob(t)
	replacementPath := filepath.Join(root, "third_party/chromedp")
	replacementVersion := ""
	const publishedSum = "h1:ZDOiaPN5NKu6WkluMA9CouyL90r0ShfMS6/icP8qNrk="
	if published {
		replacementPath = "github.com/frost-leo/fathomry/third_party/chromedp"
		replacementVersion = "v0.0.0-20261013175902-b928a75059b9"
	}
	module := fmt.Sprintf("module example.org/browser-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\nreplace github.com/chromedp/chromedp => %q %s\n", root, replacementPath, replacementVersion)
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"main.go": fixture, "go.mod": []byte(module), "go.sum": sums} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(offline bool, args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
		if offline {
			command.Env = append(command.Env, "GOPROXY=off")
		}
		return command.CombinedOutput()
	}
	if out, err := run(false, "mod", "tidy"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if out, err := run(true, "mod", "tidy", "-diff"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	binary := filepath.Join(directory, "consumer")
	if out, err := run(true, "build", "-p=1", "-race", "-mod=readonly", "-o", binary, "."); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, module := range info.Deps {
		if module.Path == "github.com/chromedp/chromedp" {
			found = module.Version == "v0.21.0" && module.Replace != nil && module.Replace.Path == replacementPath
			if found {
				t.Logf("actual SDK replacement version=%q published=%t", module.Replace.Version, published)
				if published {
					found = module.Replace.Version == replacementVersion && module.Replace.Sum == publishedSum
				} else {
					found = module.Replace.Version == "" || module.Replace.Version == "(devel)"
				}
			}
		}
	}
	if !found {
		t.Fatal("explicit SDK replacement not consumed")
	}
	if out, err := run(true, "list", "-mod=readonly", "-deps", "."); err != nil {
		t.Fatal(err)
	} else {
		for _, dependency := range strings.Fields(string(out)) {
			if strings.Contains(dependency, "/internal/httpclient/") || strings.Contains(dependency, "/internal/tableformat/") {
				t.Fatal("unselected provider linked", dependency)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = append(os.Environ(), "TMPDIR="+directory)
	if published {
		command.Env = append(command.Env, "FATHOMRY_TEST_SDK_REPLACEMENT=module")
	} else {
		command.Env = append(command.Env, "FATHOMRY_TEST_SDK_REPLACEMENT=local")
	}
	if out, err := command.CombinedOutput(); err != nil || string(out) != expectedOutput {
		t.Fatalf("%v\n%s", err, out)
	}
	// This negative control is not a distribution certificate.
	if out, err := run(true, "mod", "edit", "-dropreplace=github.com/chromedp/chromedp"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if out, err := run(false, "build", "-p=1", "-mod=mod", "-o", filepath.Join(directory, "unpatched"), "."); err == nil || !strings.Contains(string(out), "undefined:") {
		t.Fatalf("root replace was inherited or control failed: %v\n%s", err, out)
	}
}

func consumerJob(t *testing.T) string {
	t.Helper()
	output, err := exec.Command("go", "env", "GOTMPDIR").Output()
	if err != nil {
		t.Fatal(err)
	}
	base := strings.TrimSpace(string(output))
	if base == "" {
		base = filepath.Join(os.TempDir(), "fathomry")
	}
	jobs := filepath.Join(filepath.Dir(base), "jobs")
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
