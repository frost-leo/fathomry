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

package source_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIndependentModuleUsesPublicFoundation(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	quoted, _ := json.Marshal(filepath.ToSlash(root))
	module := "module example.org/consumer\n\ngo 1.26.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => " + string(quoted) + "\n"
	program := `package consumer
import (
	"context"
	"errors"
	"testing"
	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/source"
)
type config struct{}
func TestPublicComposition(t *testing.T) {
	identity := failure.MustDefine(failure.Definition{Code:"consumer.example.failed", Component:"consumer", Version:1})
	if !errors.Is(identity.New(failure.Attribution{}, context.Canceled), context.Canceled) { t.Fatal("cause inspection") }
	settings, err := source.Prepare(source.Schema[config]{Format:1}, source.Input{
		Identity:source.Identity{Provider:"example.local", Name:"one"}, Format:1,
	})
	if err != nil { t.Fatal(err) }
	selected := source.Select(settings, func(context.Context, config) (source.Resource[func() string], error) {
		return source.Resource[func() string]{Acquired:true, Capability:func() string { return "one" },
			Release:func(context.Context) source.ReleaseResult { return source.ReleaseResult{Released:true, Quiescent:true} },
		}, nil
	})
	assembly, err := source.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil { t.Fatal(err) }
	value, _, err := source.Bind(assembly, selected)
	if err != nil || value() != "one" { t.Fatal("public binding") }
	if err := assembly.Close(context.Background()); err != nil { t.Fatal(err) }
}
`
	header, err := os.ReadFile(filepath.Join(root, ".github", "LICENSE_HEADER"))
	if err != nil {
		t.Fatal(err)
	}
	program = "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(header)), "\n", "\n * ") + "\n */\n" + program
	for name, content := range map[string]string{"go.mod": module, "consumer_test.go": program} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = directory
	command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("external consumer: %v\n%s", err, output)
	}

	command = exec.Command("go", "list", "-deps", "github.com/frost-leo/fathomry/failure")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.Contains(strings.Split(dependency, "/")[0], ".") && dependency != "github.com/frost-leo/fathomry/failure" {
			t.Fatalf("error foundation depends on consumer or Provider: %s", dependency)
		}
	}
}
