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

package presentation_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/i18n"
	"github.com/frost-leo/fathomry/version"
	"github.com/frost-leo/fathomry/version/presentation"
)

func prepared(t testing.TB) *i18n.Catalog {
	t.Helper()
	catalog, err := i18n.Prepare(presentation.Resources())
	if err != nil || len(catalog.Snapshot().Stale) != 0 {
		t.Fatal("version resources are invalid or stale", err)
	}
	return catalog
}

func fixture(t testing.TB, variant string) version.Build {
	t.Helper()
	build, err := version.FromBuildInfo(&debug.BuildInfo{
		GoVersion: "go1.27.0",
		Settings:  []debug.BuildSetting{{Key: "GOOS", Value: "linux"}, {Key: "GOARCH", Value: "amd64"}},
	}, version.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if variant != "unstamped" {
		tree := version.Clean
		if variant == "dirty" {
			tree = version.Dirty
		}
		build, err = version.WithDeclaration(build, version.Declaration{
			Release: "v1.2.3+test", GitRevision: strings.Repeat("a", 40), Tree: tree, BuildTime: "1970-01-01T00:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return build
}

func TestSummaryResourcesFallbackAndMachineIsolation(t *testing.T) {
	var golden struct {
		Samples []struct{ Locale, Variant, Text string }
	}
	data, err := os.ReadFile("testdata/golden.json")
	if err != nil || json.Unmarshal(data, &golden) != nil {
		t.Fatal("cannot read golden messages", err)
	}
	catalog := prepared(t)
	for _, item := range golden.Samples {
		build := fixture(t, item.Variant)
		before := build.Snapshot()
		result, err := presentation.Summary(catalog, item.Locale, build)
		if err != nil || result.Text != item.Text || result.ResourceLocale != item.Locale || result.Fallback != i18n.NoFallback {
			t.Fatal("wrong summary or language", result, err)
		}
		if item.Locale == "en" {
			fallback, err := presentation.Summary(catalog, "de", build)
			if err != nil || fallback.Text != item.Text || fallback.ResourceLocale != "en" || fallback.Fallback != i18n.UnsupportedLocale {
				t.Fatal("fallback metadata lost", fallback, err)
			}
		}
		if _, err := presentation.Summary(nil, item.Locale, build); !errors.Is(err, i18n.InvalidCatalog) {
			t.Fatal("invalid catalog accepted")
		}
		if _, err := presentation.Summary(catalog, "en_!", build); !errors.Is(err, i18n.InvalidLocale) {
			t.Fatal("invalid locale accepted")
		}
		if !reflect.DeepEqual(build.Snapshot(), before) {
			t.Fatal("presentation changed machine facts")
		}
	}
	if result, err := presentation.Summary(catalog, "en", version.Build{}); err != nil || !strings.Contains(result.Text, "?") {
		t.Fatal("missing scalar state lost", err)
	}
	resource := presentation.Resources()
	resource[0].Data[0] = '!'
	if bytes.Equal(resource[0].Data, presentation.Resources()[0].Data) {
		t.Fatal("resources alias package state")
	}
	var readers sync.WaitGroup
	build := fixture(t, "clean")
	for range 16 {
		readers.Go(func() {
			for range 20 {
				if _, err := presentation.Summary(catalog, "zh-Hans", build); err != nil {
					t.Error(err)
				}
			}
		})
	}
	readers.Wait()
}

type formatterTrap struct{}

func (formatterTrap) String() string               { panic("formatter invoked") }
func (formatterTrap) MarshalJSON() ([]byte, error) { panic("marshal invoked") }

func TestFailureAndFormatterBoundaries(t *testing.T) {
	catalog := prepared(t)
	_, original := version.Parse("invalid-private-canary")
	for _, code := range []failure.Code{
		version.InvalidVersion, version.Unordered, version.InvalidTimestamp, version.InvalidRequest,
		version.InvalidMetadata, version.LimitExceeded, version.InvalidDeclaration,
		version.MissingDeclaration, version.Conflict, version.SerializationUnsupported,
	} {
		english, err := presentation.Error(catalog, "en", code)
		if err != nil || english.Text == "" {
			t.Fatal(err)
		}
		chinese, err := presentation.Error(catalog, "zh-Hans", code)
		if err != nil || chinese.Text == english.Text || chinese.Fallback != i18n.NoFallback {
			t.Fatal("missing error translation", err)
		}
		fallback, err := presentation.Error(catalog, "de", code)
		if err != nil || fallback.Text != english.Text || fallback.Fallback != i18n.UnsupportedLocale {
			t.Fatal("error fallback failed", err)
		}
	}
	if _, err := presentation.Error(catalog, "en", "example.external"); !errors.Is(err, i18n.MessageNotFound) {
		t.Fatal("foreign failure rendered as a version failure")
	}
	if _, err := presentation.Error(nil, "en", version.InvalidVersion); !errors.Is(err, i18n.InvalidCatalog) {
		t.Fatal("render failure hidden")
	}
	if !errors.Is(original, version.InvalidVersion) || original.Error() != string(version.InvalidVersion) {
		t.Fatal("localization changed failure identity")
	}
	_, err := catalog.Render("en", "fathomry.version.summary.unstamped",
		i18n.Arguments{"Go": formatterTrap{}, "GOOS": "linux", "GOARCH": "amd64"})
	if !errors.Is(err, i18n.InvalidArguments) {
		t.Fatal("formatter object accepted")
	}
}

func TestMessagesAndGoldenTextStayInLicensedResources(t *testing.T) {
	header, err := os.ReadFile("../../.github/LICENSE_HEADER")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := make(map[string]bool)
	for _, resource := range presentation.Resources() {
		var contents struct {
			Messages []struct{ Forms map[string]string }
		}
		if err := json.Unmarshal(resource.Data, &contents); err != nil {
			t.Fatal(err)
		}
		for _, message := range contents.Messages {
			for _, text := range message.Forms {
				forbidden[text] = true
			}
		}
	}
	err = filepath.WalkDir("..", func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, ".json") {
			var resource struct{ License []string }
			if err := json.Unmarshal(data, &resource); err != nil {
				return err
			}
			if strings.Join(resource.License, "\n") != strings.TrimSpace(string(header)) {
				t.Errorf("resource license missing: %s", path)
			}
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		expected := "/**\n"
		for _, line := range strings.Split(strings.TrimRight(string(header), "\n"), "\n") {
			expected += " *"
			if line != "" {
				expected += " " + line
			}
			expected += "\n"
		}
		expected += " */\n"
		if !bytes.HasPrefix(data, []byte(expected)) {
			t.Errorf("Go license missing: %s", path)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err == nil && forbidden[value] {
				t.Errorf("localized text in Go: %s", path)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
