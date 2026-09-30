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

package i18n

import (
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/frost-leo/fathomry/failure/v1"
)

//go:embed testdata/resources/*.json testdata/other/*.json
var fixtureResources embed.FS

const fixtureCode failure.Code = 0xA4410001

func fixtureDefinition() failure.Definition {
	return failure.Definition{Code: fixtureCode, Identifier: "example.source.failed", Module: "example", Component: "source", Revision: 1, Message: "The source failed.",
		Details: failure.Contract{ID: "example.source.details_contract", Version: 1}}
}
func fixtureComponent() Component {
	return Component{Module: "example", Name: "source", BaseLocale: "en", Resources: fixtureResources, Directory: "testdata/resources", Definitions: []failure.Definition{fixtureDefinition()}}
}
func mustCatalog(t testing.TB, components ...Component) *Catalog {
	t.Helper()
	catalog, err := Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
func copyFiles(t testing.TB, names ...string) fstest.MapFS {
	t.Helper()
	result := fstest.MapFS{}
	for _, name := range names {
		data, err := fixtureResources.ReadFile("testdata/resources/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		result[name+".json"] = &fstest.MapFile{Data: data}
	}
	return result
}

func TestCatalog(t *testing.T) {
	t.Run("foundation_code_explanations", func(t *testing.T) {
		catalog := mustCatalog(t, CoreComponents()...)
		owners, err := catalog.Components()
		if err != nil || len(owners) != 3 {
			t.Fatal("foundation registration incomplete")
		}
		declarations := append(append(failure.Definitions(), CoreComponents()[1].Definitions...), Definitions()...)
		for _, definition := range declarations {
			english, found, err := catalog.Explain(definition.Code, "en")
			if err != nil || !found || english.Message.Text != definition.Message {
				t.Fatal("baseline and numeric definition diverged")
			}
			chinese, found, err := catalog.Explain(definition.Code, "zh-CN")
			if err != nil || !found || chinese.Message.Locale != "zh-CN" || chinese.Message.Text == english.Message.Text || chinese.Definition != definition {
				t.Fatal("cross-language code lookup failed")
			}
		}
		coverage, err := catalog.Coverage("zh-CN")
		if err != nil {
			t.Fatal(err)
		}
		for _, owner := range coverage {
			if len(owner.Missing) != 0 {
				t.Fatal("missing foundation translation")
			}
		}
	})
	t.Run("discovery_coverage_and_ownership", func(t *testing.T) {
		component := fixtureComponent()
		component.Resources = copyFiles(t, "en", "zh-CN")
		component.Directory = "."
		before := mustCatalog(t, component)
		missing, _ := before.Coverage("fr")
		if len(missing) != 1 || len(missing[0].Missing) != 3 {
			t.Fatal("fallback counted as a translation")
		}
		component.Resources = copyFiles(t, "en", "zh-CN", "fr")
		after := mustCatalog(t, component)
		owners, _ := after.Components()
		if len(owners) != 1 || len(owners[0].Locales) != 3 {
			t.Fatal("new language needed an engine switch")
		}
		coverage, _ := after.Coverage("fr")
		if len(coverage[0].Missing) != 1 || coverage[0].Missing[0] != "example.source.items" {
			t.Fatal("partial coverage lost")
		}
		selected, err := after.Resolve("example.source.items", "fr")
		if err != nil || selected.info.Fallback != MissingTranslation || selected.info.Locale != "en" {
			t.Fatal("missing translation fallback not explicit")
		}
		exact, err := after.Lookup("example.source.items", "fr")
		if err != nil || !exact.MessageExists || exact.TranslationExists {
			t.Fatal("exact query secretly fell back")
		}
		other := Component{Module: "example", Name: "other", BaseLocale: "fr", Resources: fixtureResources, Directory: "testdata/other"}
		isolated := mustCatalog(t, Component{Module: component.Module, Name: component.Name, BaseLocale: "en", Resources: copyFiles(t, "en", "zh-CN"), Directory: ".", Definitions: component.Definitions}, other)
		selected, err = isolated.Resolve("example.source.failed", "fr")
		if err != nil || selected.info.Fallback != UnsupportedLocale || selected.info.Locale != "en" {
			t.Fatal("another owner's French leaked into matching")
		}
		selected, err = isolated.Resolve("example.other.notice", "de")
		if err != nil || selected.info.Locale != "fr" {
			t.Fatal("non-English base ignored")
		}
		owners[0].Locales[0] = "mutated"
		entries, _ := after.Inspect()
		entries[0].Forms[0].Pattern = "mutated"
		selected, _ = after.Resolve("example.source.failed", "en-GB-u-nu-thai")
		if selected.info.Locale != "en" || selected.info.Matched == "en" {
			t.Fatal("synthetic match became a resource key")
		}
	})
	t.Run("concurrent_inspection_and_rendering", func(t *testing.T) {
		catalog := mustCatalog(t, fixtureComponent())
		var group sync.WaitGroup
		for range 8 {
			group.Go(func() {
				for range 100 {
					selection, err := catalog.Resolve("example.source.details", "zh-CN")
					if err != nil {
						t.Error(err)
						return
					}
					rendered, err := selection.Render([]Argument{{Name: "label", Value: "public"}}, nil)
					if err != nil || rendered.Locale != "zh-CN" {
						t.Error("rendering changed")
						return
					}
					owners, _ := catalog.Components()
					owners[0].Messages[0] = "mutated"
					entries, _ := catalog.Inspect()
					entries[0].Forms[0].Pattern = "mutated"
				}
			})
		}
		group.Wait()
	})
	t.Run("invalid_and_unknown_queries", func(t *testing.T) {
		catalog := mustCatalog(t, fixtureComponent())
		if _, found, err := catalog.Explain(0xA7FFFFFF, "en"); err != nil || found {
			t.Fatal("unknown code not absent")
		}
		if _, _, err := catalog.Explain(0, "en"); !errors.Is(err, failure.ErrCode) {
			t.Fatal("invalid code accepted")
		}
		for _, locale := range []string{"", "und", "en_US", "en-x-private", "en,zh-CN", strings.Repeat("a", 129)} {
			if _, err := catalog.Resolve("example.source.failed", locale); !errors.Is(err, ErrLocale) {
				t.Fatal("bad locale accepted")
			}
		}
		if _, err := catalog.Resolve("example.source.unknown", "en"); !errors.Is(err, ErrMessage) {
			t.Fatal("unknown message resolved")
		}
		for _, catalog := range []*Catalog{nil, {}} {
			if _, err := catalog.Inspect(); !errors.Is(err, ErrCatalog) {
				t.Fatal("invalid catalog inspected")
			}
			if _, err := catalog.Components(); !errors.Is(err, ErrCatalog) {
				t.Fatal("invalid catalog listed")
			}
			if _, err := catalog.Coverage("en"); !errors.Is(err, ErrCatalog) {
				t.Fatal("invalid coverage")
			}
			if _, err := catalog.Lookup("", "en"); !errors.Is(err, ErrCatalog) {
				t.Fatal("invalid lookup")
			}
			if _, err := catalog.Resolve("", "en"); !errors.Is(err, ErrCatalog) {
				t.Fatal("invalid resolution")
			}
			if _, _, err := catalog.Explain(fixtureCode, "en"); !errors.Is(err, ErrCatalog) {
				t.Fatal("invalid explanation")
			}
		}
	})
}

func TestResourceAdmission(t *testing.T) {
	for name, mutate := range map[string]func(fstest.MapFS){
		"duplicate_member":   func(files fstest.MapFS) { files["en.json"].Data = []byte(`{"schema":"a","schema":"b"}`) },
		"malformed_utf":      func(files fstest.MapFS) { files["en.json"].Data = []byte{'{', '"', 255, '"', ':', '1', '}'} },
		"unpaired_surrogate": func(files fstest.MapFS) { files["en.json"].Data = []byte(`{"bad":"\ud800"}`) },
		"unknown_field": func(files fstest.MapFS) {
			files["en.json"].Data = []byte(strings.Replace(string(files["en.json"].Data), `"schema":`, `"unknown":true,"schema":`, 1))
		},
		"stale_translation": func(files fstest.MapFS) {
			files["en.json"].Data = []byte(strings.Replace(string(files["en.json"].Data), "The source failed.", "The source changed.", 1))
		},
		"duplicate_locale": func(files fstest.MapFS) {
			files["EN.json"] = &fstest.MapFile{Data: append([]byte(nil), files["en.json"].Data...)}
		},
		"missing_baseline":  func(files fstest.MapFS) { delete(files, "en.json") },
		"oversize_document": func(files fstest.MapFS) { files["en.json"].Data = []byte(strings.Repeat(" ", MaxDocumentBytes+1)) },
	} {
		t.Run(name, func(t *testing.T) {
			files := copyFiles(t, "en", "zh-CN")
			mutate(files)
			component := fixtureComponent()
			component.Resources = files
			component.Directory = "."
			if catalog, err := Prepare(component); err == nil || catalog != nil {
				t.Fatal("invalid/partial resource catalog published")
			}
		})
	}
	t.Run("native_syntax_and_io_causes", func(t *testing.T) {
		files := fstest.MapFS{"en.json": {Data: []byte(`{"schema":]}`)}}
		component := fixtureComponent()
		component.Resources = files
		component.Directory = "."
		_, err := Prepare(component)
		var syntax *json.SyntaxError
		if !errors.Is(err, ErrResource) || !errors.As(err, &syntax) {
			t.Fatal("native JSON cause lost")
		}
		component.Resources = fstest.MapFS{}
		component.Directory = "absent"
		_, err = Prepare(component)
		if !errors.Is(err, fs.ErrNotExist) || !errors.Is(err, ErrResource) {
			t.Fatal("native FS cause lost")
		}
	})
	t.Run("binding_and_definition_admission", func(t *testing.T) {
		component := fixtureComponent()
		component.Definitions[0].Message = "Different baseline."
		if _, err := Prepare(component); !errors.Is(err, ErrBinding) {
			t.Fatal("two baseline truths admitted")
		}
		component = fixtureComponent()
		component.Bindings = []Binding{{Code: fixtureCode, Message: "example.source.details", MessageContract: "v1"}}
		if _, err := Prepare(component); !errors.Is(err, ErrBinding) {
			t.Fatal("nil projector admitted")
		}
		component.Bindings[0].Project = func(error) (Input, error) { return Input{}, nil }
		if _, err := Prepare(component); !errors.Is(err, ErrBinding) {
			t.Fatal("wrong detail contract admitted")
		}
		component.Bindings[0].Details = fixtureDefinition().Details
		component.Bindings[0].MessageContract = "v2"
		if _, err := Prepare(component); !errors.Is(err, ErrBinding) {
			t.Fatal("stale message contract admitted")
		}
		if _, err := Prepare(); !errors.Is(err, ErrResource) {
			t.Fatal("empty catalog accepted")
		}
		if _, err := Prepare(fixtureComponent(), fixtureComponent()); !errors.Is(err, ErrResource) {
			t.Fatal("duplicate owner accepted")
		}
	})
}

func FuzzResource(f *testing.F) {
	data, _ := fixtureResources.ReadFile("testdata/resources/en.json")
	f.Add(data)
	f.Add([]byte(`{"schema":"bad","schema":"again"}`))
	f.Add([]byte(`{"bad":"\ud800"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxDocumentBytes+1 {
			return
		}
		component := Component{Module: "example", Name: "source", BaseLocale: "en", Resources: fstest.MapFS{"en.json": {Data: data}}, Directory: "."}
		catalog, err := Prepare(component)
		if err != nil {
			if catalog != nil {
				t.Fatal("partial catalog")
			}
			return
		}
		entries, err := catalog.Inspect()
		if err != nil || len(entries) == 0 {
			t.Fatal("invalid accepted catalog")
		}
	})
}
