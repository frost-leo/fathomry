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

package configsource_test

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

func TestIndependentConsumers(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range []string{"viper", "nacos"} {
		t.Run(provider, func(t *testing.T) {
			directory := t.TempDir()
			source, err := os.ReadFile(filepath.Join("..", provider, "v1", "testdata", "consumer", "consumer_test.go"))
			if err != nil {
				t.Fatal(err)
			}
			module := fmt.Sprintf("module example.org/configsource-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))
			if provider == "nacos" {
				// Lumberjack v2's unversioned test imports need explicit fixture
				// versions for reproducible module discovery.
				module += "\nrequire (\n github.com/BurntSushi/toml v1.6.0\n gopkg.in/yaml.v2 v2.4.0\n)\n"
			}
			for name, data := range map[string][]byte{
				"consumer_test.go": source,
				"go.mod":           []byte(module),
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
					t.Fatal("independent check exceeded its bound")
				}
				return output, err
			}
			// Dependency tests may select versions absent from the parent cache.
			if output, err := run(false, "mod", "tidy"); err != nil {
				t.Fatalf("consumer dependency preparation: %v\n%s", err, output)
			}
			for _, args := range [][]string{{"mod", "tidy", "-diff"}, {"test", "-mod=readonly", "-race", "-count=1", "./..."}} {
				if output, err := run(true, args...); err != nil {
					t.Fatalf("%v: %v\n%s", args, err, output)
				}
			}
			output, err := run(true, "list", "-deps", "github.com/frost-leo/fathomry/adapters/configsource/"+provider+"/v1")
			if err != nil {
				t.Fatal(err)
			}
			other := "nacos"
			if provider == "nacos" {
				other = "viper"
			}
			if strings.Contains(string(output), "fathomry/internal/configsource/"+other+"/") || strings.Contains(string(output), "fathomry/framework/") {
				t.Fatal("provider selection imported unrelated capability")
			}
			output, err = run(true, "list", "-deps", "github.com/frost-leo/fathomry/adapters/configsource/v1")
			if err != nil || strings.Contains(string(output), "fathomry/internal/") {
				t.Fatal("public preparation depends on Internal", err)
			}
			notice, err := os.ReadFile(filepath.Join(root, ".github", "LICENSE_HEADER"))
			if err != nil {
				t.Fatal(err)
			}
			header := "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(notice)), "\n", "\n * ") + "\n */\n"
			header = strings.ReplaceAll(header, "\n * \n", "\n *\n")
			negative := header + "package consumer\nimport c \"github.com/frost-leo/fathomry/adapters/configsource/v1\"\nfunc forbidden(value c.Prepared[struct{Value string `json:\"a\"`}]) c.Prepared[struct{Value string `json:\"b\"`}] { return (c.Prepared[struct{Value string `json:\"b\"`}])(value) }\n"
			if provider == "nacos" {
				negative = header + "package consumer\nimport (\"context\"; n \"github.com/frost-leo/fathomry/adapters/configsource/nacos/v1\")\nfunc forbidden(value n.Handle) { value.Close(context.Background()) }\n"
			}
			if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(negative), 0600); err != nil {
				t.Fatal(err)
			}
			output, err = run(true, "test", "-mod=readonly", "./...")
			if err == nil {
				t.Fatal("forbidden conversion or authority compiled")
			}
			expected := "cannot convert"
			if provider == "nacos" {
				expected = "value.Close undefined"
			}
			if !strings.Contains(string(output), expected) {
				t.Fatalf("wrong negative failure: %s", output)
			}
		})
	}
}
