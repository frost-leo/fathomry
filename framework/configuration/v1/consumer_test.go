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

package configuration_test

import (
	"context"
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

func TestIndependentConsumer(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	source, err := os.ReadFile("testdata/consumer/consumer_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"consumer_test.go": source,
		"go.mod":           []byte(fmt.Sprintf("module example.org/configuration-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
	} {
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
			command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
		}
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("independent command exceeded its bound")
		}
		return output, err
	}
	// Prepare the consumer's complete graph before enforcing offline checks.
	if output, err := run(false, "mod", "tidy"); err != nil {
		t.Fatalf("consumer dependency preparation: %v\n%s", err, output)
	}
	for _, args := range [][]string{{"mod", "tidy", "-diff"}, {"test", "-mod=readonly", "-race", "-count=1", "./..."}} {
		if output, err := run(true, args...); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
	}
	output, err := run(true, "list", "-deps", "github.com/frost-leo/fathomry/framework/configuration/v1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), "github.com/frost-leo/fathomry/adapters/configsource/v1") {
		t.Fatal("Framework scenario bypassed the public preparation capability")
	}
	if strings.Contains(string(source), "github.com/frost-leo/fathomry/adapters/") {
		t.Fatal("Framework consumer requires Adapter assembly")
	}
	notice, err := os.ReadFile(filepath.Join(root, ".github/LICENSE_HEADER"))
	if err != nil {
		t.Fatal(err)
	}
	header := "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(notice)), "\n", "\n * ") + "\n */\n"
	header = strings.ReplaceAll(header, "\n * \n", "\n *\n")
	for _, kind := range []string{"State", "Accepted", "Watcher"} {
		forbidden := header + fmt.Sprintf("package consumer\nimport c \"github.com/frost-leo/fathomry/framework/configuration/v1\"\nfunc forbidden(value c.%[1]s[struct{Value string `json:\"a\"`}]) c.%[1]s[struct{Value string `json:\"b\"`}] { return (c.%[1]s[struct{Value string `json:\"b\"`}])(value) }\n", kind)
		if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(forbidden), 0600); err != nil {
			t.Fatal(err)
		}
		output, err := run(true, "test", "-mod=readonly", "./...")
		if err == nil || !strings.Contains(string(output), "cannot convert") {
			t.Fatalf("generic authority seal not enforced: %v\n%s", err, output)
		}
	}
	for _, declaration := range []string{
		"var _ = c.Bootstrap[struct{}]", "var _ c.Startup[struct{}]", "var _ c.BootstrapOptions[struct{}]",
		"var _ c.BootstrapDefaults", "var _ c.SourceMode", "var _ c.Layer",
		"var _ = c.Declaration[struct{}]{Layers:nil}", "var _ = c.ViperOptions{Paths:nil}", "var _ = c.NacosOptions{Keys:nil}",
		"var _ = c.Dependencies{Source:nil}", "var _ = c.Dependencies{Runtime:nil}", "var _ = c.Dependencies{Evidence:nil}",
		"var _ = c.Declaration[struct{}]{Source: nil}", "var _ = c.Declaration[struct{}]{Environment: nil}",
	} {
		if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(header+"package consumer\nimport c \"github.com/frost-leo/fathomry/framework/configuration/v1\"\n"+declaration+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if output, err := run(true, "test", "-mod=readonly", "./..."); err == nil || !strings.Contains(string(output), "undefined:") && !strings.Contains(string(output), "unknown field") {
			t.Fatalf("withdrawn configuration contract remains usable: %s: %v\n%s", declaration, err, output)
		}
	}
}

func TestConfigurationDoesNotBypassAdaptersOrChooseProcessInputs(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			path, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path == "os" || strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") {
				t.Fatal("configuration bypassed its explicit Adapter boundary or read process inputs", entry.Name(), path)
			}
		}
	}
}
