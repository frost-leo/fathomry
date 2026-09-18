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
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/i18n"
)

func TestAggregateResourceLimits(t *testing.T) {
	var large []i18n.Resource
	for index := 0; index < 9; index++ {
		large = append(large, i18n.Resource{Name: fmt.Sprintf("%d.json", index), Data: make([]byte, i18n.MaxFileBytes)})
	}
	_, err := i18n.Prepare(large)
	requireCode(t, err, i18n.LimitExceeded)
	source := resources(t)[0]
	for _, resource := range resources(t) {
		if strings.HasSuffix(resource.Name, "catalog.en.json") {
			source = resource
		}
	}
	tooMany := editResource(t, source, func(value map[string]any) {
		original := message(value, "demo.welcome")
		list := make([]any, i18n.MaxMessages+1)
		for index := range list {
			copy := make(map[string]any, len(original))
			for key, entry := range original {
				copy[key] = entry
			}
			copy["id"] = fmt.Sprintf("generated.message%d", index)
			list[index] = copy
		}
		value["messages"] = list
	})
	if len(tooMany.Data) > i18n.MaxFileBytes {
		t.Fatal("message-count control hit byte limit first")
	}
	_, err = i18n.Prepare([]i18n.Resource{tooMany})
	requireCode(t, err, i18n.LimitExceeded)
	tooMany = editResource(t, source, func(value map[string]any) {
		params := message(value, "demo.welcome")["parameters"].(map[string]any)
		for index := 0; index < i18n.MaxParameters; index++ {
			params[fmt.Sprintf("Param%d", index)] = params["Name"]
		}
	})
	_, err = i18n.Prepare([]i18n.Resource{tooMany})
	requireCode(t, err, i18n.LimitExceeded)
	translation := resources(t)[0]
	for _, resource := range resources(t) {
		if strings.HasSuffix(resource.Name, "catalog.zh-Hant.json") {
			translation = resource
		}
	}
	many := []i18n.Resource{source}
	for _, locale := range []string{"ar", "bg", "bs", "ca", "cs", "cy", "da", "de", "el", "es", "et", "eu", "fa", "fi", "fr", "ga", "he", "hi", "hr", "hu", "id", "is", "it", "ja", "ko", "lt", "lv", "nl", "pl", "pt", "ro", "ru"} {
		input := editResource(t, translation, func(value map[string]any) { value["locale"] = locale })
		input.Name = locale + ".json"
		many = append(many, input)
	}
	_, err = i18n.Prepare(many)
	requireCode(t, err, i18n.LimitExceeded)
}

func TestExplicitLocaleAndScriptRefusal(t *testing.T) {
	t.Setenv("LANG", "zh_CN.UTF-8")
	t.Setenv("LC_ALL", "ru_RU.UTF-8")
	inputs := resources(t)
	inputs = slices.DeleteFunc(inputs, func(value i18n.Resource) bool { return strings.HasSuffix(value.Name, "catalog.zh-Hans.json") })
	catalog := prepared(t, inputs)
	english, err := catalog.Render("en", "demo.source-only", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := catalog.Render("", "demo.source-only", nil)
	if err != nil || result != english {
		t.Fatal("ambient locale affected rendering")
	}
	result, err = catalog.Render("zh-Hans", "demo.source-only", nil)
	if err != nil || result.MatchedLocale != "en" || result.Fallback != i18n.UnsupportedLocale {
		t.Fatal("weak cross-script match accepted", result, err)
	}
}

func TestAuthoredMessagesStayInLicensedResources(t *testing.T) {
	header, err := os.ReadFile("../.github/LICENSE_HEADER")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := make(map[string]bool)
	for _, resource := range resources(t) {
		var file struct {
			Messages []struct{ Forms map[string]string }
		}
		if err := json.Unmarshal(resource.Data, &file); err != nil {
			t.Fatal(err)
		}
		for _, message := range file.Messages {
			for _, text := range message.Forms {
				forbidden[text] = true
			}
		}
	}
	for _, item := range fixture[struct{ Cases []renderCase }](t, "cases.json").Cases {
		forbidden[item.Text] = true
	}
	presentation, err := os.ReadFile("../failure/testdata/presentation.json")
	if err != nil {
		t.Fatal(err)
	}
	var existing struct{ Messages map[string]string }
	if err := json.Unmarshal(presentation, &existing); err != nil {
		t.Fatal(err)
	}
	for _, text := range existing.Messages {
		forbidden[text] = true
	}
	for _, root := range []string{".", "../failure"} {
		err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.HasSuffix(path, ".json") {
				var envelope struct{ License []string }
				if err := json.Unmarshal(data, &envelope); err != nil {
					return err
				}
				if strings.Join(envelope.License, "\n") != strings.TrimRight(string(header), "\n") {
					t.Errorf("missing resource license: %s", path)
				}
			}
			if strings.HasSuffix(path, ".go") {
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
					t.Errorf("missing Go license: %s", path)
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
						t.Errorf("authored message literal in Go: %s", path)
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkRender(b *testing.B) {
	catalog := prepared(b, resources(b))
	args := i18n.Arguments{"Count": i18n.Number("2"), "Owner": ""}
	for _, locale := range []string{"en", "zh-Hans", "ru"} {
		b.Run(locale, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := catalog.Render(locale, "demo.files", args); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
