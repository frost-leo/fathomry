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

package resource_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestGenericTagIdentity(t *testing.T) {
	type before = struct {
		Primary string `json:"primary"`
		Backup  string `json:"backup"`
	}
	type after = struct {
		Primary string `json:"backup"`
		Backup  string `json:"primary"`
	}
	for _, pair := range [][2]reflect.Type{
		{reflect.TypeFor[resource.Prepared[before]](), reflect.TypeFor[resource.Prepared[after]]()},
		{reflect.TypeFor[resource.Selection[before]](), reflect.TypeFor[resource.Selection[after]]()},
	} {
		if pair[0].ConvertibleTo(pair[1]) || reflect.PointerTo(pair[0]).ConvertibleTo(reflect.PointerTo(pair[1])) {
			t.Error("tag-only type conversion can reinterpret a sealed token")
		}
	}
}

func TestGenericConversionsCompileBoundary(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	write := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	header, err := os.ReadFile(filepath.Join(root, ".github/LICENSE_HEADER"))
	if err != nil {
		t.Fatal(err)
	}
	notice := "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(header)), "\n", "\n * ") + "\n */\n"
	write("go.mod", "module github.com/frost-leo/fathomry/typecheck\n\ngo 1.26.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => "+fmt.Sprintf("%q", filepath.ToSlash(root))+"\n")
	declarations := "package typecheck\nimport \"github.com/frost-leo/fathomry/internal/resource\"\n" +
		"type before = struct { Limit int `json:\"limit\"` }\n" +
		"type after = struct { Limit int `json:\"different\"` }\n" +
		"type namedBefore before\ntype namedAfter before\ntype alias = before\n" +
		"type primary = struct { Primary string `json:\"primary\"`; Backup string `json:\"backup\"` }\n" +
		"type swapped = struct { Primary string `json:\"backup\"`; Backup string `json:\"primary\"` }\n"
	run := func() ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./...")
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("type conversion fixture exceeded its deadline")
		}
		return output, err
	}
	valid := "func valid() {\n" +
		" _ = resource.Prepared[alias](resource.Prepared[before]{})\n" +
		" _ = resource.Selection[alias](resource.Selection[before]{})\n" +
		" type preparedCopy resource.Prepared[before]\n" +
		" _ = resource.Prepared[before](preparedCopy(resource.Prepared[before]{}))\n" +
		" _ = after(before{Limit:7})\n" +
		"}\n"
	write("fixture.go", notice+declarations+valid)
	if output, err := run(); err != nil {
		t.Fatalf("valid conversion control did not compile: %v\n%s", err, output)
	}
	for _, fixture := range []struct{ name, expression string }{
		{"prepared-tags", "resource.Prepared[after](resource.Prepared[before]{})"},
		{"prepared-swapped-tags", "resource.Prepared[swapped](resource.Prepared[primary]{})"},
		{"prepared-named", "resource.Prepared[namedAfter](resource.Prepared[namedBefore]{})"},
		{"prepared-pointer", "(*resource.Prepared[after])(new(resource.Prepared[before]))"},
		{"selection-tags", "resource.Selection[after](resource.Selection[before]{})"},
		{"selection-named", "resource.Selection[namedAfter](resource.Selection[namedBefore]{})"},
		{"selection-pointer", "(*resource.Selection[after])(new(resource.Selection[before]))"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			write("fixture.go", notice+declarations+valid+"func forbidden() { _ = "+fixture.expression+" }\n")
			output, err := run()
			var failure *exec.ExitError
			if !errors.As(err, &failure) || failure.ExitCode() != 1 ||
				!strings.Contains(string(output), "fixture.go:") || !strings.Contains(string(output), "cannot convert") {
				t.Fatalf("missing intended conversion rejection: %v\n%s", err, output)
			}
		})
	}
}

func TestPreparedAnonymousSettingsRetainValidationAndIdentity(t *testing.T) {
	type config = struct {
		Primary string            `json:"primary"`
		Backup  string            `json:"backup"`
		Labels  map[string]string `json:"labels"`
	}
	validations := 0
	prepared, err := resource.Prepare(resource.Schema[config]{
		Format:   1,
		Defaults: config{Primary: "primary", Backup: "backup", Labels: map[string]string{"key": "original"}},
		Validate: func(value config) error {
			validations++
			if value.Primary != "primary" || value.Backup != "backup" || value.Labels["key"] != "original" {
				return errors.New("unexpected settings")
			}
			value.Labels["key"] = "validator"
			return nil
		},
	}, resource.Input{Identity: resource.Identity{Provider: "fixture.types", Name: "typed"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	description := prepared.Description()
	selected := resource.Select(prepared, func(_ context.Context, value config) (resource.Resource[config], error) {
		if value.Primary != "primary" || value.Backup != "backup" || value.Labels["key"] != "original" {
			t.Error("factory settings differ from independently validated values")
		}
		return resource.Resource[config]{Acquired: true, Capability: value, Release: complete}, nil
	})
	for range 2 {
		assembly := assemble(t, "typed", selected)
		capability, info, err := resource.Bind(assembly, selected)
		if err != nil || !reflect.DeepEqual(info.Configuration, description) {
			t.Fatal("binding changed the preparation identity")
		}
		capability.Labels["key"] = "consumer"
		info.Configuration.Provenance[0].Fields[0] = "consumer"
		borrowed := resource.Borrow("alias", assembly, selected)
		borrower := assemble(t, "borrower", borrowed)
		shared, sharedInfo, err := resource.Bind(borrower, borrowed)
		if err != nil || shared.Labels["key"] != "consumer" || !reflect.DeepEqual(sharedInfo.Configuration, description) {
			t.Fatal("typed borrowing lost the authoritative resource or immutable metadata")
		}
		if err := borrower.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := assembly.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if validations != 1 || !reflect.DeepEqual(prepared.Description(), description) {
		t.Fatal("reuse changed validation or provenance")
	}
}
