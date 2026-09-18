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
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/i18n"
)

//go:embed testdata/catalog.*.json testdata/cases.json testdata/adversarial.json
var fixtures embed.FS

type fixtureArgument struct {
	Type  string
	Value json.RawMessage
}

type renderCase struct {
	Locale    string
	ID        string
	Arguments map[string]fixtureArgument
	Text      string
	Matched   string
	Resource  string
	Fallback  i18n.Fallback
}

type adversarialCases struct {
	Templates         []string
	PositiveTemplates []string
	InvalidNumbers    []string
	InvalidLocales    []string
	SourceEdits       map[string]string
	TranslationFix    string
	Retry             map[string]string
}

func fixture[T any](t testing.TB, name string) T {
	t.Helper()
	data, err := fixtures.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var result T
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func resources(t testing.TB) []i18n.Resource {
	t.Helper()
	paths, err := fs.Glob(fixtures, "testdata/catalog.*.json")
	if err != nil {
		t.Fatal(err)
	}
	var result []i18n.Resource
	for _, path := range paths {
		data, err := fixtures.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, i18n.Resource{Name: path, Data: data})
	}
	return result
}

func prepared(t testing.TB, resources []i18n.Resource) *i18n.Catalog {
	t.Helper()
	catalog, err := i18n.Prepare(resources)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func arguments(t testing.TB, values map[string]fixtureArgument) i18n.Arguments {
	t.Helper()
	result := make(i18n.Arguments)
	for name, argument := range values {
		switch argument.Type {
		case "string", "number":
			var value string
			if err := json.Unmarshal(argument.Value, &value); err != nil {
				t.Fatal(err)
			}
			if argument.Type == "number" {
				result[name] = i18n.Number(value)
			} else {
				result[name] = value
			}
		case "integer":
			var value int64
			if err := json.Unmarshal(argument.Value, &value); err != nil {
				t.Fatal(err)
			}
			result[name] = value
		case "boolean":
			var value bool
			if err := json.Unmarshal(argument.Value, &value); err != nil {
				t.Fatal(err)
			}
			result[name] = value
		default:
			t.Fatal("unknown fixture argument type")
		}
	}
	return result
}

func editResource(t testing.TB, resource i18n.Resource, edit func(map[string]any)) i18n.Resource {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(resource.Data, &object); err != nil {
		t.Fatal(err)
	}
	edit(object)
	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return i18n.Resource{Name: resource.Name, Data: data}
}

func editLocale(t testing.TB, inputs []i18n.Resource, locale string, edit func(map[string]any)) []i18n.Resource {
	t.Helper()
	result := slices.Clone(inputs)
	for index, resource := range result {
		var object struct{ Locale string }
		if err := json.Unmarshal(resource.Data, &object); err != nil {
			t.Fatal(err)
		}
		if object.Locale == locale {
			result[index] = editResource(t, resource, edit)
			return result
		}
	}
	t.Fatal("fixture locale not found")
	return nil
}

func message(object map[string]any, id string) map[string]any {
	for _, item := range object["messages"].([]any) {
		value := item.(map[string]any)
		if value["id"] == id {
			return value
		}
	}
	panic("fixture message not found")
}

func requireCode(t testing.TB, err error, code failure.Code) {
	t.Helper()
	if !errors.Is(err, code) {
		t.Fatalf("got %v, want %v", err, code)
	}
	for _, verb := range []string{"%s", "%v", "%+v", "%#v", "%9999.9999s"} {
		if fmt.Sprintf(verb, err) != string(code) {
			t.Fatal("unsafe error formatting")
		}
	}
	var occurrence failure.Error
	if !errors.As(err, &occurrence) || occurrence.Unwrap() != nil {
		t.Fatal("missing safe public failure")
	}
}

func TestExternalResourcesAndLocalePolicy(t *testing.T) {
	inputs := resources(t)
	catalog := prepared(t, inputs)
	cases := fixture[struct{ Cases []renderCase }](t, "cases.json").Cases
	for index, item := range cases {
		t.Run(fmt.Sprintf("%02d-%s-%s", index, item.Locale, item.ID), func(t *testing.T) {
			result, err := catalog.Render(item.Locale, item.ID, arguments(t, item.Arguments))
			if err != nil {
				t.Fatal(err)
			}
			if result.Text != item.Text || result.MatchedLocale != item.Matched ||
				result.ResourceLocale != item.Resource || result.Fallback != item.Fallback ||
				result.Snapshot != catalog.Snapshot().ID {
				t.Fatalf("unexpected rendering metadata or text: %#v", result)
			}
		})
	}
	slices.Reverse(inputs)
	reordered := prepared(t, inputs)
	if !reflect.DeepEqual(catalog.Snapshot(), reordered.Snapshot()) {
		t.Fatal("input order changed snapshot")
	}
	for _, item := range cases {
		first, firstErr := catalog.Render(item.Locale, item.ID, arguments(t, item.Arguments))
		second, secondErr := reordered.Render(item.Locale, item.ID, arguments(t, item.Arguments))
		if firstErr != nil || secondErr != nil || first != second {
			t.Fatal("input order changed matching")
		}
	}
}

func TestResourceRefusalIsAtomic(t *testing.T) {
	inputs := resources(t)
	source := slices.IndexFunc(inputs, func(value i18n.Resource) bool { return strings.HasSuffix(value.Name, "catalog.en.json") })
	tests := []struct {
		name string
		edit func(map[string]any)
		code failure.Code
	}{
		{"profile", func(value map[string]any) { value["profile"] = "fathomry.i18n/v2" }, i18n.UnsupportedProfile},
		{"unknown", func(value map[string]any) { value["extra"] = true }, i18n.InvalidResource},
		{"case", func(value map[string]any) { value["Locale"] = value["locale"]; delete(value, "locale") }, i18n.InvalidResource},
		{"null", func(value map[string]any) { value["license"] = nil }, i18n.InvalidResource},
		{"empty", func(value map[string]any) { value["messages"] = []any{} }, i18n.InvalidResource},
		{"duplicate-id", func(value map[string]any) {
			value["messages"] = append(value["messages"].([]any), value["messages"].([]any)[0])
		}, i18n.Duplicate},
		{"id", func(value map[string]any) { message(value, "demo.welcome")["id"] = "invalid" }, i18n.InvalidResource},
		{"description", func(value map[string]any) { delete(message(value, "demo.welcome"), "description") }, i18n.InvalidResource},
		{"missing-other", func(value map[string]any) { delete(message(value, "demo.welcome")["forms"].(map[string]any), "other") }, i18n.InvalidResource},
		{"missing-one", func(value map[string]any) { delete(message(value, "demo.files")["forms"].(map[string]any), "one") }, i18n.InvalidResource},
		{"count-type", func(value map[string]any) {
			message(value, "demo.files")["parameters"].(map[string]any)["Count"].(map[string]any)["type"] = "integer"
		}, i18n.InvalidResource},
		{"unknown-form", func(value map[string]any) {
			forms := message(value, "demo.files")["forms"].(map[string]any)
			forms["ordinal"] = forms["one"]
		}, i18n.InvalidResource},
		{"unknown-parameter-field", func(value map[string]any) {
			message(value, "demo.welcome")["parameters"].(map[string]any)["Name"].(map[string]any)["other"] = true
		}, i18n.InvalidResource},
		{"unknown-parameter-type", func(value map[string]any) {
			message(value, "demo.welcome")["parameters"].(map[string]any)["Name"].(map[string]any)["type"] = "object"
		}, i18n.InvalidResource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bad := slices.Clone(inputs)
			bad[source] = editResource(t, bad[source], test.edit)
			catalog, err := i18n.Prepare(bad)
			requireCode(t, err, test.code)
			if catalog != nil {
				t.Fatal("partially published invalid catalog")
			}
		})
	}
	for index, template := range fixture[adversarialCases](t, "adversarial.json").Templates {
		t.Run(fmt.Sprintf("template-%d", index), func(t *testing.T) {
			bad := editLocale(t, inputs, "en", func(value map[string]any) {
				message(value, "demo.welcome")["forms"].(map[string]any)["other"] = template
			})
			catalog, err := i18n.Prepare(bad)
			if err == nil || catalog != nil {
				t.Fatal("invalid template published")
			}
		})
	}
	for _, template := range fixture[adversarialCases](t, "adversarial.json").PositiveTemplates {
		valid := editLocale(t, inputs, "en", func(value map[string]any) {
			message(value, "demo.welcome")["forms"].(map[string]any)["other"] = template
		})
		result, err := prepared(t, valid).Render("en", "demo.welcome", i18n.Arguments{"Name": "x"})
		if err != nil || result.Text != "x" {
			t.Fatal("valid comment/trim syntax rejected", err)
		}
	}
	for _, data := range [][]byte{
		[]byte(`{"profile":"a","profile":"b"}`),
		[]byte(`{"forms":{"other":"x","\u006fther":"y"}}`),
		[]byte(`{"x":"\ud800"}`),
		[]byte(`{"x":"\udfff"}`),
		{0xff}, []byte("{} {}"), []byte("{"), []byte("null"),
	} {
		catalog, err := i18n.Prepare([]i18n.Resource{{Name: "invalid.json", Data: data}})
		if err == nil || catalog != nil {
			t.Fatal("invalid JSON published")
		}
	}
	duplicate := append(slices.Clone(inputs), inputs[0])
	_, err := i18n.Prepare(duplicate)
	requireCode(t, err, i18n.Duplicate)
	copyFile := inputs[source]
	copyFile.Name = "copy.json"
	duplicate = append(slices.Clone(inputs), copyFile)
	_, err = i18n.Prepare(duplicate)
	requireCode(t, err, i18n.Duplicate)
	_, err = i18n.Prepare(nil)
	requireCode(t, err, i18n.InvalidResource)
	_, err = i18n.Prepare(slices.Delete(slices.Clone(inputs), source, source+1))
	requireCode(t, err, i18n.InvalidResource)
}

type callbackValue struct{ called *bool }

func (value callbackValue) String() string { *value.called = true; panic("callback executed") }
func (value callbackValue) Error() string  { *value.called = true; panic("callback executed") }

func TestArgumentsLocalesAndFailurePrivacy(t *testing.T) {
	catalog := prepared(t, resources(t))
	type label string
	type alias = string
	_, err := catalog.Render("en", "demo.welcome", i18n.Arguments{"Name": label("")})
	requireCode(t, err, i18n.InvalidArguments)
	aliased, err := catalog.Render("en", "demo.welcome", i18n.Arguments{"Name": alias("")})
	plain, plainErr := catalog.Render("en", "demo.welcome", i18n.Arguments{"Name": ""})
	if err != nil || plainErr != nil || aliased != plain {
		t.Fatal("true alias changed the dynamic type contract")
	}
	called := false
	for _, value := range []any{nil, 1, 1.0, []byte{1}, []string{}, map[string]string{}, func() { called = true }, callbackValue{&called}} {
		result, err := catalog.Render("en", "demo.welcome", i18n.Arguments{"Name": value})
		requireCode(t, err, i18n.InvalidArguments)
		if result != (i18n.Result{}) {
			t.Fatal("partial result on invalid argument")
		}
	}
	if called {
		t.Fatal("unapproved callback executed")
	}
	for _, args := range []i18n.Arguments{nil, {}, {"Extra": ""}, {"Name": "", "Extra": ""}, {"Name": string([]byte{0xff})}} {
		_, err := catalog.Render("en", "demo.welcome", args)
		requireCode(t, err, i18n.InvalidArguments)
	}
	for _, number := range fixture[adversarialCases](t, "adversarial.json").InvalidNumbers {
		_, err := catalog.Render("en", "demo.files", i18n.Arguments{"Count": i18n.Number(number), "Owner": ""})
		requireCode(t, err, i18n.InvalidArguments)
	}
	for _, locale := range fixture[adversarialCases](t, "adversarial.json").InvalidLocales {
		_, err := catalog.Render(locale, "demo.source-only", nil)
		requireCode(t, err, i18n.InvalidLocale)
	}
	_, err = catalog.Render(strings.Repeat("a", i18n.MaxLocaleBytes+1), "demo.source-only", nil)
	requireCode(t, err, i18n.InvalidLocale)
	_, err = catalog.Render("en", "unknown.id", nil)
	requireCode(t, err, i18n.MessageNotFound)
	_, err = catalog.Render("en", "", nil)
	requireCode(t, err, i18n.MessageNotFound)
	_, err = (&i18n.Catalog{}).Render("en", "demo.source-only", nil)
	requireCode(t, err, i18n.InvalidCatalog)
	_, err = (*i18n.Catalog)(nil).Render("en", "demo.source-only", nil)
	requireCode(t, err, i18n.InvalidCatalog)
	if !reflect.DeepEqual((*i18n.Catalog)(nil).Snapshot(), i18n.Snapshot{}) {
		t.Fatal("nil snapshot")
	}
	result, err := catalog.Render("en", "demo.files", i18n.Arguments{"Count": i18n.Number("999999999.999999"), "Owner": ""})
	if err != nil || result.ResourceLocale != "en" {
		t.Fatal("bounded maximum number rejected", err)
	}
}

func TestBoundsBeforePublicationAndOutput(t *testing.T) {
	source := resources(t)[0]
	tests := []struct {
		name      string
		resources []i18n.Resource
	}{
		{"file", []i18n.Resource{{Name: "large.json", Data: make([]byte, i18n.MaxFileBytes+1)}}},
		{"files", make([]i18n.Resource, i18n.MaxFiles+1)},
		{"depth", []i18n.Resource{{Name: "deep.json", Data: []byte(strings.Repeat("[", i18n.MaxJSONDepth+2) + "0" + strings.Repeat("]", i18n.MaxJSONDepth+2))}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog, err := i18n.Prepare(test.resources)
			requireCode(t, err, i18n.LimitExceeded)
			if catalog != nil {
				t.Fatal("partial publication")
			}
		})
	}
	for _, name := range []string{"", "../escape.json", "/absolute.json", ".", "bad\nname", strings.Repeat("a", 257)} {
		source.Name = name
		_, err := i18n.Prepare([]i18n.Resource{source})
		requireCode(t, err, i18n.InvalidResource)
	}
	catalog := prepared(t, resources(t))
	value := strings.Repeat("x", i18n.MaxOutputBytes/32)
	result, err := catalog.Render("en", "demo.expansion", i18n.Arguments{"Name": value})
	if err != nil || len(result.Text) != i18n.MaxOutputBytes {
		t.Fatal("exact output boundary", err)
	}
	result, err = catalog.Render("en", "demo.expansion", i18n.Arguments{"Name": value + "x"})
	requireCode(t, err, i18n.LimitExceeded)
	if result != (i18n.Result{}) {
		t.Fatal("partial oversized output")
	}
	_, err = catalog.Render("en", "demo.opaque", i18n.Arguments{"Name": strings.Repeat("x", i18n.MaxArgumentBytes+1)})
	requireCode(t, err, i18n.LimitExceeded)
	bad := editLocale(t, resources(t), "en", func(value map[string]any) {
		message(value, "demo.welcome")["forms"].(map[string]any)["other"] = strings.Repeat("x", i18n.MaxTemplateBytes+1)
	})
	_, err = i18n.Prepare(bad)
	requireCode(t, err, i18n.LimitExceeded)
	bad = editLocale(t, resources(t), "en", func(value map[string]any) {
		template := message(value, "demo.opaque")["forms"].(map[string]any)["other"].(string)
		message(value, "demo.opaque")["forms"].(map[string]any)["other"] = strings.Repeat(template, i18n.MaxTemplateNodes+1)
	})
	_, err = i18n.Prepare(bad)
	requireCode(t, err, i18n.LimitExceeded)
}

func TestMissingPluralVariantHasExplicitEnglishFallback(t *testing.T) {
	inputs := editLocale(t, resources(t), "ru", func(value map[string]any) { delete(message(value, "demo.files")["forms"].(map[string]any), "few") })
	catalog := prepared(t, inputs)
	args := i18n.Arguments{"Count": i18n.Number("2"), "Owner": ""}
	english, err := catalog.Render("en", "demo.files", args)
	if err != nil {
		t.Fatal(err)
	}
	result, err := catalog.Render("ru", "demo.files", args)
	if err != nil || result.Text != english.Text || result.ResourceLocale != "en" || result.MatchedLocale != "ru" || result.Fallback != i18n.TranslationFailure {
		t.Fatal("sparse plural silently succeeded", result, err)
	}
}

func TestOwnedSnapshotsAndConcurrentFirstUse(t *testing.T) {
	inputs := resources(t)
	catalog := prepared(t, inputs)
	snapshot := catalog.Snapshot()
	for _, resource := range inputs {
		clear(resource.Data)
	}
	inputs[0].Name = "changed"
	for index := range snapshot.Resources {
		snapshot.Resources[index] = i18n.ResourceDigest{}
	}
	for index := range snapshot.Sources {
		snapshot.Sources[index] = i18n.SourceRevision{}
	}
	if reflect.DeepEqual(snapshot, catalog.Snapshot()) {
		t.Fatal("metadata not owned")
	}
	cases := fixture[struct{ Cases []renderCase }](t, "cases.json").Cases
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		group.Go(func() {
			for repeat := 0; repeat < 20; repeat++ {
				for _, item := range cases {
					args := arguments(t, item.Arguments)
					result, err := catalog.Render(item.Locale, item.ID, args)
					if err != nil || result.Text != item.Text {
						t.Errorf("concurrent render failed: %v", err)
						return
					}
					clear(args)
					metadata := catalog.Snapshot()
					metadata.Resources[0] = i18n.ResourceDigest{}
				}
			}
		})
	}
	group.Wait()
}

func TestJSONEscapesAndCanonicalLocaleDuplicates(t *testing.T) {
	inputs := resources(t)
	source := slices.IndexFunc(inputs, func(value i18n.Resource) bool { return strings.HasSuffix(value.Name, "catalog.en.json") })
	inputs[source] = editResource(t, inputs[source], func(value map[string]any) { value["license"] = []string{string([]rune{0x1f30d})} })
	inputs[source].Data = bytes.ReplaceAll(inputs[source].Data, []byte(string([]rune{0x1f30d})), []byte(`\ud83c\udf0d`))
	if _, err := i18n.Prepare(inputs); err != nil {
		t.Fatal(err)
	}
	duplicate := editResource(t, inputs[source], func(value map[string]any) { value["locale"] = "EN" })
	duplicate.Name = "duplicate.json"
	_, err := i18n.Prepare(append(inputs, duplicate))
	requireCode(t, err, i18n.Duplicate)
}
