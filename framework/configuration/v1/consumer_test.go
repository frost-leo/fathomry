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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// This is deliberately a different module, not merely an external test package.
func TestIndependentConfigurationConsumers(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	// These cached versions are the existing repository's transitive test graph,
	// not new runtime dependencies or external-module replacement workarounds.
	module := fmt.Sprintf("module example.org/configuration-consumer\n\ngo 1.27.0\nrequire (\n github.com/frost-leo/fathomry v0.0.0\n github.com/BurntSushi/toml v1.6.0\n github.com/rogpeppe/go-internal v1.14.1\n gopkg.in/yaml.v2 v2.4.0\n)\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))
	write := func(name string, data []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", []byte(module))
	files, err := filepath.Glob("testdata/consumer/*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatal("consumer sources unavailable", err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		write(filepath.Base(file), data)
	}
	run := func(args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("consumer timed out: %s", output)
		}
		return output, err
	}
	commands := [][]string{{"mod", "tidy"}, {"mod", "tidy", "-diff"}}
	test := []string{"test", "-count=1", "-timeout=2m", "-v", "."}
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" && os.Getenv("CGO_ENABLED") != "0" {
		test = append([]string{"test", "-race"}, test[1:]...)
	}
	commands = append(commands, test)
	for _, args := range commands {
		output, err := run(args...)
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
		t.Logf("%v\n%s", args, output)
	}
	for _, entry := range []string{
		"github.com/frost-leo/fathomry/adapters/v1",
		"github.com/frost-leo/fathomry/framework/v1",
		"github.com/frost-leo/fathomry/framework/configuration/v1",
		"github.com/frost-leo/fathomry/adapters/configsource/viper/v1",
	} {
		output, err := run("list", "-deps", entry)
		if err != nil {
			t.Fatal(err, string(output))
		}
		for _, forbidden := range []string{"nacos", "temporal", "database", "redis", "cli", "telemetry", "notification", "sqlengine", "tableformat"} {
			for _, line := range strings.Fields(string(output)) {
				if strings.Contains(line, "/"+forbidden) {
					t.Fatalf("unexpected dependency for %s: %s", entry, line)
				}
			}
		}
		t.Logf("isolated production graph: %s (%d packages)", entry, len(strings.Fields(string(output))))
	}
	for _, wrapper := range []string{"Snapshot", "Schema", "State"} {
		for _, pointer := range []bool{false, true} {
			star := ""
			if pointer {
				star = "*"
			}
			source := fmt.Sprintf("package consumer\nimport c \"github.com/frost-leo/fathomry/framework/configuration/v1\"\nfunc forbidden(value %[1]sc.%[2]s[struct{Value string `json:\"a\"`}]) %[1]sc.%[2]s[struct{Value string `json:\"b\"`}] { return (%[1]sc.%[2]s[struct{Value string `json:\"b\"`}])(value) }\n", star, wrapper)
			write("conversion_test.go", []byte(source))
			output, err := run("test", "-run", "^$", ".")
			if err == nil || !strings.Contains(string(output), "cannot convert") {
				t.Fatalf("tag-only %s pointer=%v conversion did not reject correctly: %v %s", wrapper, pointer, err, output)
			}
		}
	}
	write("conversion_test.go", []byte("package consumer\nimport c \"github.com/frost-leo/fathomry/framework/configuration/v1\"\ntype alias=struct{ Value string `json:\"a\"` }\nfunc allowed(value c.Snapshot[alias]) c.Snapshot[struct{Value string `json:\"a\"`}] { return value }\n"))
	if output, err := run("test", "-run", "^$", "."); err != nil {
		t.Fatal("identical alias rejected", err, string(output))
	}
	write("forbidden_test.go", []byte("package consumer\nimport _ \"github.com/frost-leo/fathomry/internal/resource\"\n"))
	output, err := run("test", "-run", "^$", ".")
	if err == nil || !strings.Contains(string(output), "use of internal package") {
		t.Fatal("private import control failed", err, string(output))
	}
}
