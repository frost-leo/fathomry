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

package create

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"go/parser"
	"go/token"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

func validOptions() options {
	return options{Name: "demo", Module: "example.com/acme/demo",
		FrameworkVersion: "v0.0.0-20261031064010-94997fff92da"}
}

func prepared(t testing.TB) plan {
	t.Helper()
	value, err := prepare(context.Background(), validOptions())
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestInputProfile(t *testing.T) {
	for _, name := range []string{"a", "demo-name", strings.Repeat("a", 63), "a--b", "a-"} {
		value := validOptions()
		value.Name = name
		if err := validate(value); err != nil {
			t.Fatal(name, err)
		}
	}
	for _, name := range []string{"", ".", "..", "../x", "a/b", "a\\b", "-a", "A", "a b", "a\nb", "a\x00b", "订单", "con", "nul", "com1", "lpt1", strings.Repeat("a", 64)} {
		value := validOptions()
		value.Name = name
		if err := validate(value); !errors.Is(err, ErrInput) {
			t.Fatalf("name admitted %q: %v", name, err)
		}
	}
	modules := []string{"", "demo", "../x", "/x", "https://example.com/app", "Example.com/app",
		"example.com/a//b", "example.com/a/../b", "example.com/app/v1", "example.com/CON",
		"example.com/vendor", "example.com/vendor/app", "example.com/app/vendor/child",
		"google.golang.org/grpc/codes", "golang.org/x/text/language", "golang.org/x/text/date",
		"golang.org/x/sys/windows", "golang.org/x/sys/windows/registry",
		"github.com/frost-leo/fathomry/internal/conformance",
		"example.com/app\nrequire evil", strings.Repeat("x", 1025),
		"github.com/frost-leo/fathomry", "github.com/fsnotify/fsnotify", "github.com/go-viper/mapstructure/v2",
		"github.com/inconshreveable/mousetrap", "github.com/nacos-group/nacos-sdk-go/v2",
		"github.com/pelletier/go-toml/v2", "github.com/sagikazarmark/locafero", "github.com/sourcegraph/conc",
		"github.com/spf13/afero", "github.com/spf13/cast", "github.com/spf13/cobra", "github.com/spf13/pflag",
		"github.com/spf13/viper", "github.com/subosito/gotenv", "go.uber.org/multierr", "go.uber.org/zap",
		"go.yaml.in/yaml/v3", "golang.org/x/net", "golang.org/x/sys", "golang.org/x/text",
		"google.golang.org/genproto/googleapis/rpc", "google.golang.org/grpc", "google.golang.org/protobuf",
		"gopkg.in/natefinch/lumberjack.v2"}
	for _, path := range modules {
		value := validOptions()
		value.Module = path
		if err := validate(value); !errors.Is(err, ErrInput) {
			t.Fatalf("module admitted %q: %v", path, err)
		}
	}
	for _, path := range []string{"github.com/frost-leo", "github.com/frost-leo/fathomry/app", "example.com/acme/demo/v2",
		"github.com/frost-leo/fathomry/framework", "golang.org/x/text/language/app",
		"example.com/internal", "example.com/testdata", "example.com/vendor-app", "vendor.example.com/app",
		"github.com/jackc/pgx/v5/pgconn", "github.com/redis/go-redis/v9/internal/pool",
		"github.com/stretchr/testify/assert", "github.com/BurntSushi/toml", "gopkg.in/yaml.v2"} {
		value := validOptions()
		value.Module = path
		if err := validate(value); err != nil {
			t.Fatal("valid distinct module refused", path, err)
		}
	}
	for _, version := range []string{"", "latest", "develop", "b28e37d", "(devel)", "v1", "v1.2", "v0.0.0", "v2.0.0+incompatible",
		"v0.0.0-20261028081050-b28e37d55e53", validOptions().FrameworkVersion + "+dirty", strings.Repeat("x", 129)} {
		value := validOptions()
		value.FrameworkVersion = version
		if err := validate(value); !errors.Is(err, ErrInput) {
			t.Fatalf("version admitted %q: %v", version, err)
		}
	}
	if platform("linux") != nil {
		t.Fatal("qualified platform rejected")
	}
	for _, goos := range []string{"windows", "darwin", "js", "wasip1", ""} {
		if !errors.Is(platform(goos), ErrPlatform) {
			t.Fatal("unqualified platform admitted", goos)
		}
	}
}

func TestMirroredClosedTemplates(t *testing.T) {
	want := []string{
		"templates/.env.example.tmpl", "templates/.env.tmpl",
		"templates/.gitignore.tmpl", "templates/README.md.tmpl",
		"templates/config/base.yaml.tmpl", "templates/config/environment/development.yaml.tmpl", "templates/config/environment/production.yaml.tmpl",
		"templates/go.mod.tmpl", "templates/internal/bootstrap/bootstrap.go.tmpl", "templates/internal/bootstrap/bootstrap_test.go.tmpl",
		"templates/internal/setting/application.go.tmpl", "templates/internal/setting/config.go.tmpl", "templates/internal/setting/config_test.go.tmpl",
		"templates/main.go.tmpl", "templates/main_test.go.tmpl",
	}
	var actual []string
	if err := fs.WalkDir(templates, "templates", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			actual = append(actual, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(actual, want) {
		t.Fatal("embedded template inventory differs", actual)
	}
	value := prepared(t)
	targets := []string{"go.mod", "README.md", ".gitignore", "main.go", "main_test.go", ".env", ".env.example",
		"config/base.yaml", "config/environment/development.yaml", "config/environment/production.yaml",
		"internal/bootstrap/bootstrap.go", "internal/bootstrap/bootstrap_test.go", "internal/setting/config.go", "internal/setting/application.go", "internal/setting/config_test.go"}
	if len(value.files) != len(targets) {
		t.Fatal("wrong file count")
	}
	for index, file := range value.files {
		if file.path != targets[index] || len(file.data) == 0 {
			t.Fatal("wrong target/content", index, file.path)
		}
	}
	raw, err := templates.ReadFile("templates/.gitignore.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	parts := bytes.SplitN(raw, []byte("# Upstream body below is preserved verbatim.\n\n"), 2)
	if len(parts) != 2 {
		t.Fatal("upstream provenance missing")
	}
	digest := sha256.Sum256(parts[1])
	if hex.EncodeToString(digest[:]) != "16c7d7ad8c23ef27ccc8fe3c605752024a4d353f7e0b745defad7e6b0430b719" {
		t.Fatal("GitHub ignore template changed without qualification")
	}
	independent := prepared(t)
	value.files[0].data[0] = 'X'
	if independent.files[0].data[0] == 'X' || prepared(t).files[0].data[0] == 'X' {
		t.Fatal("rendered snapshots alias")
	}
}

func TestGeneratedApplicationUsesFrameworkContracts(t *testing.T) {
	for _, file := range prepared(t).files {
		if !strings.HasSuffix(file.path, ".go") {
			continue
		}
		source, err := parser.ParseFile(token.NewFileSet(), file.path, file.data, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range source.Imports {
			path, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/configsource/") {
				t.Errorf("generated application bypasses its Framework configuration contract: %s: %s", file.path, path)
			}
			if strings.HasPrefix(file.path, "internal/setting/") && !strings.HasSuffix(file.path, "_test.go") {
				for _, runtime := range []string{"os", "io/fs", "net", "net/http", "database/sql"} {
					if path == runtime {
						t.Errorf("setting owns runtime I/O: %s: %s", file.path, path)
					}
				}
			}
		}
	}
}

func templateMap(t testing.TB) fstest.MapFS {
	t.Helper()
	result := fstest.MapFS{}
	err := fs.WalkDir(templates, "templates", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			data, err := templates.ReadFile(path)
			if err != nil {
				return err
			}
			result[path] = &fstest.MapFile{Data: bytes.Clone(data)}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRenderRejectsBeforeCreation(t *testing.T) {
	for _, test := range []struct {
		name, path string
		data       []byte
	}{
		{"missing", "templates/go.mod.tmpl", nil},
		{"bad_template", "templates/README.md.tmpl", []byte("{{")},
		{"bad_go", "templates/main.go.tmpl", []byte("not Go")},
		{"wrong_module", "templates/go.mod.tmpl", []byte("module example.com/wrong\ngo 1.27.0\n")},
		{"hidden_toolchain", "templates/go.mod.tmpl", []byte("module {{.Module}}\ngo 1.27.0\nrequire github.com/frost-leo/fathomry {{.FrameworkVersion}}\ntoolchain go1.28.0\n")},
		{"hidden_ignore", "templates/go.mod.tmpl", []byte("module {{.Module}}\ngo 1.27.0\nrequire github.com/frost-leo/fathomry {{.FrameworkVersion}}\nignore ./cmd\n")},
		{"too_large", "templates/README.md.tmpl", bytes.Repeat([]byte("x"), maxRenderedBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			sources := templateMap(t)
			if test.data == nil {
				delete(sources, test.path)
			} else {
				sources[test.path] = &fstest.MapFile{Data: test.data}
			}
			if _, err := render(context.Background(), validOptions(), sources); !errors.Is(err, ErrRender) {
				t.Fatal("invalid render accepted", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepare(ctx, validOptions()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	buffer := new(renderBuffer)
	if _, err := buffer.Write(make([]byte, maxRenderedBytes)); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Write([]byte("x")); !errors.Is(err, ErrRender) {
		t.Fatal("render limit weakened", err)
	}
}

func FuzzPrepare(f *testing.F) {
	value := validOptions()
	f.Add(value.Name, value.Module, value.FrameworkVersion)
	f.Add("../escape", "github.com/frost-leo/fathomry", "latest")
	f.Fuzz(func(t *testing.T, name, path, version string) {
		value := options{Name: name, Module: path, FrameworkVersion: version}
		result, err := prepare(context.Background(), value)
		if err == nil && (validate(value) != nil || len(result.files) != len(templateFiles)) {
			t.Fatal("invalid plan admitted")
		}
	})
}
