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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	"golang.org/x/text/feature/plural"
	"golang.org/x/text/language"
)

func fixtures(t testing.TB) []Source {
	t.Helper()
	var sources []Source
	for _, locale := range []string{"en", "zh-CN", "ru", "ar"} {
		data, err := os.ReadFile("testdata/resources/" + locale + ".json")
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, Source{Name: "inventory." + locale, Data: data})
	}
	return sources
}

func preparedFixture(t testing.TB) *Catalog {
	t.Helper()
	catalog, err := Prepare(fixtures(t)...)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func selected(t testing.TB, catalog *Catalog, id, locale string) Selection {
	t.Helper()
	selection, err := catalog.Resolve("example.inventory:"+id, locale)
	if err != nil {
		t.Fatal(err)
	}
	return selection
}

func rendered(t testing.TB, selection Selection, args []Argument, count *uint64) Rendered {
	t.Helper()
	result, err := selection.Render(args, count)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func sourceJSON(t testing.TB, name string, document any) Source {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return Source{Name: name, Data: data}
}

func sourceDocument(t testing.TB, source Source) map[string]any {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(source.Data, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

func message(id, pattern string, args map[string]any, cardinal bool) map[string]any {
	return map[string]any{"id": "test:" + id, "contract": "v1", "context": "Bounded test definition.", "args": args, "cardinal": cardinal, "forms": map[string]any{"other": pattern}}
}

func document(messages ...any) map[string]any {
	return map[string]any{"schema": Schema, "profile": Profile, "owner": "test", "locale": "en", "messages": messages}
}

func expectRejected(t testing.TB, condition failure.Condition, sources ...Source) {
	t.Helper()
	catalog, err := Prepare(sources...)
	current, ok := failure.Inspect(err)
	if catalog != nil || !ok || !errors.Is(err, condition) || current.Diagnostic().CauseCount != 0 {
		t.Fatalf("wanted %s without publication, published=%v error=%v", condition, catalog != nil, err)
	}
}

func TestExactLookupAndSelection(t *testing.T) {
	catalog := preparedFixture(t)
	for _, test := range []struct {
		id, locale           string
		message, translation bool
	}{
		{"identity", "en", true, true}, {"identity", "zh-CN", true, false},
		{"missing", "en", false, false}, {"Identity", "en", false, false},
		{"identity", "en-Latn", true, true}, {"identity", "en-u-nu-arab", true, false},
	} {
		result, err := catalog.Lookup("example.inventory:"+test.id, test.locale)
		if err != nil || result.MessageExists != test.message || result.TranslationExists != test.translation {
			t.Fatalf("%+v: %+v %v", test, result, err)
		}
	}
	if result := rendered(t, selected(t, catalog, "identity", "en"), nil, nil); result.Text != "example.inventory:identity" {
		t.Fatal(result)
	}
	for _, invalid := range []string{"", "en_US", " en", "und", "en-x-private", "en-fonipa", "en-a-test", "en-INVALID", strings.Repeat("a", 129)} {
		if _, err := catalog.Lookup("example.inventory:identity", invalid); !errors.Is(err, ErrLocale) {
			t.Fatalf("%q: %v", invalid, err)
		}
		if invalid != "" {
			if _, err := catalog.Resolve("example.inventory:identity", invalid); !errors.Is(err, ErrLocale) {
				t.Fatal(invalid, err)
			}
		}
	}
	if _, err := catalog.Resolve("missing", "en"); !errors.Is(err, ErrMessage) {
		t.Fatal(err)
	}
	if _, err := catalog.Lookup(strings.Repeat("x", 257), "en"); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	for _, test := range []struct {
		request, actual string
		fallback        Fallback
	}{
		{"", "en", NoFallback}, {"EN-latn", "en", NoFallback}, {"en-US-u-nu-arab", "en", NoFallback},
		{"zh-CN", "zh-CN", NoFallback}, {"zh-TW", "en", UnsupportedLocale}, {"ja", "en", UnsupportedLocale},
	} {
		info, err := selected(t, catalog, "items", test.request).Metadata()
		if err != nil || info.Requested != test.request || info.Resource.Locale != test.actual || info.Fallback != test.fallback {
			t.Fatalf("%+v: %+v %v", test, info, err)
		}
		if test.request == "en-US-u-nu-arab" && (info.Matched == info.Resource.Locale || info.Candidate != "en" || info.Canonical != test.request) {
			t.Fatal("matcher provenance conflated", info)
		}
	}
	count := uint64(21)
	selection := selected(t, catalog, "english", "ru")
	info, _ := selection.Metadata()
	result := rendered(t, selection, nil, &count)
	if info.Candidate != "ru" || info.Resource.Locale != "en" || info.Fallback != MissingTranslation || result.Category != Other || result.Text != "21 source items" {
		t.Fatal(info, result)
	}
}

func TestPreparationIsAtomicAndStrict(t *testing.T) {
	sources := fixtures(t)
	for _, data := range []string{
		"", "null", "[]", "{} {}", "\ufeff{}", "{\"schema\":null}",
		"{\"a\":1}", "{\"a\":\"x\",\"a\":\"y\"}", "{\"id\":\"x\",\"\\u0069d\":\"y\"}",
		"{\"a\":{\"x\":true,\"x\":false}}", "{\"a\":\"\\ud800\"}", "{\"a\":\"\\udc00\"}",
		"{\"a\":\"\\ud800x\"}", string([]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}),
	} {
		expectRejected(t, ErrResource, Source{Name: "invalid", Data: []byte(data)})
	}
	if !validEscapes([]byte(`{"a":"\ud83d\ude00","b":"\\ud800"}`)) {
		t.Fatal("valid surrogate/escaped literal refused")
	}
	expectRejected(t, ErrResource)
	expectRejected(t, ErrResource, sources[1])
	expectRejected(t, ErrResource, sources[0], sources[0])
	duplicate := sources[0]
	duplicate.Name = "other"
	expectRejected(t, ErrResource, sources[0], duplicate)
	for _, mutate := range []func(map[string]any){
		func(d map[string]any) { d["Schema"] = d["schema"] },
		func(d map[string]any) { d["schema"] = "future" },
		func(d map[string]any) { d["profile"] = "other" },
		func(d map[string]any) { d["owner"] = "wrong" },
		func(d map[string]any) { d["messages"] = []any{} },
		func(d map[string]any) { d["messages"].([]any)[0].(map[string]any)["cardinal"] = nil },
		func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["forms"] = map[string]any{"one": "one"}
		},
		func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["forms"] = map[string]any{"other": ""}
		},
		func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["forms"] = map[string]any{"other": "{unknown}"}
		},
		func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["forms"] = map[string]any{"other": "{count}"}
		},
		func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["forms"] = map[string]any{"other": "{place:%s}"}
		},
		func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["forms"] = map[string]any{"other": "{place} }"}
		},
		func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["args"].(map[string]any)["count"] = map[string]any{"kind": "uint64", "meaning": "conflict"}
		},
		func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["args"].(map[string]any)["place"].(map[string]any)["kind"] = "object"
		},
	} {
		doc := sourceDocument(t, sources[0])
		mutate(doc)
		expectRejected(t, ErrResource, sourceJSON(t, "mutated", doc))
	}
	for _, field := range []string{"context", "contract"} {
		doc := sourceDocument(t, sources[0])
		doc["messages"].([]any)[0].(map[string]any)[field] = "changed"
		expectRejected(t, ErrResource, sourceJSON(t, "changed", doc), sources[1])
	}
	doc := sourceDocument(t, sources[1])
	doc["messages"].([]any)[0].(map[string]any)["source"] = strings.Repeat("a", 64)
	expectRejected(t, ErrResource, sources[0], sourceJSON(t, "stale", doc))
	for _, field := range []string{"context", "args", "cardinal"} {
		doc := sourceDocument(t, sources[1])
		doc["messages"].([]any)[0].(map[string]any)[field] = "not permitted"
		expectRejected(t, ErrResource, sources[0], sourceJSON(t, "extra", doc))
	}
}

func TestCanonicalCollisionsAndProvenance(t *testing.T) {
	sources := fixtures(t)
	doc := sourceDocument(t, sources[0])
	doc["locale"] = "en-Latn"
	expectRejected(t, ErrResource, sources[0], sourceJSON(t, "alias", doc))
	doc = sourceDocument(t, sources[1])
	doc["locale"] = "iw"
	hebrew := sourceJSON(t, "hebrew.alias", doc)
	doc["locale"] = "he"
	expectRejected(t, ErrResource, sources[0], hebrew, sourceJSON(t, "hebrew", doc))
	catalog, err := Prepare(sources[0], hebrew)
	if err != nil {
		t.Fatal(err)
	}
	found, err := catalog.Lookup("example.inventory:items", "he")
	if err != nil || !found.TranslationExists || found.Definition.Locale != "he" || found.Definition.SourceName != hebrew.Name {
		t.Fatal(found, err)
	}
	all := preparedFixture(t)
	before, _ := all.Inspect()
	slices.Reverse(sources)
	reordered, err := Prepare(sources...)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := reordered.Inspect()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("input-order dependent composition")
	}
	original := fixtures(t)[0]
	spaced := original
	spaced.Name = "renamed"
	spaced.Data = append([]byte(" \n"), original.Data...)
	baseline, err := Prepare(original)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := Prepare(spaced)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := baseline.Inspect()
	right, _ := changed.Inspect()
	for index := range left {
		if left[index].SourceDigest != right[index].SourceDigest || left[index].EntryDigest != right[index].EntryDigest || left[index].DocumentDigest == right[index].DocumentDigest || left[index].SourceName == right[index].SourceName {
			t.Fatal("wrong digest axes")
		}
	}
}

func TestOwnedInspectionAndSharedLanguage(t *testing.T) {
	for _, mode := range []string{"inspection", "lookup", "selection"} {
		for _, field := range []string{"arguments", "forms"} {
			t.Run(mode+"/"+field, func(t *testing.T) {
				sources := fixtures(t)
				catalog, err := Prepare(sources...)
				if err != nil {
					t.Fatal(err)
				}
				expected, _ := catalog.Inspect()
				frozen, err := json.Marshal(expected)
				if err != nil {
					t.Fatal(err)
				}
				for _, source := range sources {
					clear(source.Data)
				}
				var actual Definition
				switch mode {
				case "inspection":
					entries, _ := catalog.Inspect()
					for _, definition := range entries {
						if definition.ID == "example.inventory:items" && definition.Locale == "en" {
							actual = definition
						}
					}
				case "lookup":
					result, _ := catalog.Lookup("example.inventory:items", "en")
					actual = result.Definition
				case "selection":
					info, _ := selected(t, catalog, "items", "en").Metadata()
					actual = info.Resource
				}
				if field == "arguments" {
					actual.Arguments[0].Meaning = "MUTATED"
					actual.Arguments[0].Name = "changed"
				} else {
					actual.Forms[0].Pattern = "MUTATED"
				}
				fresh, _ := catalog.Inspect()
				encoded, err := json.Marshal(fresh)
				if err != nil || !bytes.Equal(frozen, encoded) {
					t.Fatal("nested alias", mode, field, err)
				}
				number := uint64(1)
				if got := rendered(t, selected(t, catalog, "items", "en"), []Argument{{Name: "place", Value: "shelf"}}, &number); got.Text != "1 item at shelf" {
					t.Fatal(mode, field, got)
				}
			})
		}
	}
	catalog := preparedFixture(t)
	expected, _ := catalog.Inspect()
	previousID, previousLocale := "", ""
	for _, definition := range expected {
		if definition.ID < previousID || definition.ID == previousID && definition.Locale != "en" && previousLocale != "en" && definition.Locale < previousLocale {
			t.Fatal("unsorted")
		}
		previousID, previousLocale = definition.ID, definition.Locale
		if definition.Schema != Schema || definition.Profile != Profile || definition.Engine != Engine || definition.LanguageData != LanguageData || definition.PluralData != PluralData || len(definition.EntryDigest) != 64 || len(definition.SourceDigest) != 64 || len(definition.DocumentDigest) != 64 {
			t.Fatal("incomplete metadata")
		}
	}
	var group sync.WaitGroup
	for worker := range 24 {
		group.Go(func() {
			locale := []string{"en", "zh-CN", "ru", "ar"}[worker%4]
			for iteration := range 100 {
				selection, err := catalog.Resolve("example.inventory:items", locale)
				if err != nil {
					t.Error(err)
					return
				}
				number := uint64(iteration)
				result, err := selection.Render([]Argument{{Name: "place", Value: "shelf"}}, &number)
				info, _ := selection.Metadata()
				if err != nil || !strings.Contains(result.Text, strconv.FormatUint(number, 10)) || info.Resource.Locale != locale {
					t.Error(locale, result, err)
				}
				snapshot, _ := catalog.Inspect()
				snapshot[0].Forms[0].Pattern = "caller"
			}
		})
	}
	group.Wait()
}

func oracle(locale string, number uint64) Form {
	switch locale {
	case "en":
		if number == 1 {
			return One
		}
	case "ru":
		if number%10 == 1 && number%100 != 11 {
			return One
		}
		if number%10 >= 2 && number%10 <= 4 && !(number%100 >= 12 && number%100 <= 14) {
			return Few
		}
		return Many
	case "ar":
		switch number {
		case 0:
			return Zero
		case 1:
			return One
		case 2:
			return Two
		}
		if number%100 >= 3 && number%100 <= 10 {
			return Few
		}
		if number%100 >= 11 && number%100 <= 99 {
			return Many
		}
	}
	return Other
}

func TestFullWidthCardinalsAndScalarRejection(t *testing.T) {
	catalog := preparedFixture(t)
	numbers := []uint64{0, 1, 2, 3, 10, 11, 12, 21, 101, 1000001, 10000000, 10000001, 1 << 53, (1 << 53) + 1, (1 << 63) + 1, math.MaxUint64}
	for number := range uint64(1000) {
		numbers = append(numbers, number)
	}
	for _, locale := range []string{"en", "zh-CN", "ru", "ar"} {
		selection := selected(t, catalog, "items", locale)
		for _, number := range numbers {
			result := rendered(t, selection, []Argument{{Name: "place", Value: "shelf"}}, &number)
			if result.Category != oracle(locale, number) || !strings.Contains(result.Text, strconv.FormatUint(number, 10)) {
				t.Fatalf("%s %d: %+v", locale, number, result)
			}
			if locale == "ar" && (result.Variant != Other || result.FormFallback != (result.Category != Other)) {
				t.Fatal(result)
			}
		}
	}
	selection := selected(t, catalog, "items", "en")
	for _, args := range [][]Argument{
		nil, {{Name: "place", Value: "x"}, {Name: "place", Value: "y"}},
		{{Name: "count", Value: uint64(2)}}, {{Name: "place", Value: int64(2)}},
	} {
		count := uint64(0)
		if got, err := selection.Render(args, &count); !errors.Is(err, ErrArguments) || got != (Rendered{}) {
			t.Fatal(got, err)
		}
	}
	if _, err := selection.Render([]Argument{{Name: "place", Value: "x"}}, nil); !errors.Is(err, ErrArguments) {
		t.Fatal(err)
	}
	zero := uint64(0)
	if _, err := selected(t, catalog, "identity", "en").Render(nil, &zero); !errors.Is(err, ErrArguments) {
		t.Fatal(err)
	}
	type namedString string
	for _, value := range []any{nil, 1, float64(1), json.Number("1"), namedString("x"), []byte("x"), new(string), hostileScalar{}, errors.New("private")} {
		if got, err := selection.Render([]Argument{{Name: "place", Value: value}}, &zero); !errors.Is(err, ErrArguments) || got != (Rendered{}) {
			t.Fatalf("%T: %v", value, err)
		}
	}
	if _, err := selection.Render([]Argument{{Name: "place", Value: string([]byte{0xff})}}, &zero); !errors.Is(err, ErrArguments) {
		t.Fatal(err)
	}
	args := []Argument{{Name: "s", Value: "unchanged"}, {Name: "i", Value: int64(math.MinInt64)}, {Name: "u", Value: uint64(math.MaxUint64)}, {Name: "b", Value: true}}
	result := rendered(t, selected(t, catalog, "scalars", "en-US-u-nu-arab"), args, nil)
	if result.Text != "{unchanged} -9223372036854775808 18446744073709551615 true 100%" {
		t.Fatal(result)
	}
}

type hostileScalar struct{}

func (hostileScalar) String() string         { panic("String called") }
func (hostileScalar) Error() string          { panic("Error called") }
func (hostileScalar) Format(fmt.State, rune) { panic("Format called") }

func TestInvalidHandlesAndSafeFailures(t *testing.T) {
	for _, catalog := range []*Catalog{nil, {}} {
		if _, err := catalog.Inspect(); !errors.Is(err, ErrCatalog) {
			t.Fatal(err)
		}
		if _, err := catalog.Lookup("secret", "en"); !errors.Is(err, ErrCatalog) {
			t.Fatal(err)
		}
		if _, err := catalog.Resolve("secret", "en"); !errors.Is(err, ErrCatalog) {
			t.Fatal(err)
		}
	}
	if _, err := (Selection{}).Render(nil, nil); !errors.Is(err, ErrCatalog) {
		t.Fatal(err)
	}
	if _, err := (Selection{}).Metadata(); !errors.Is(err, ErrCatalog) {
		t.Fatal(err)
	}
	_, err := Prepare(Source{Name: "secret/path", Data: []byte("secret payload")})
	if strings.Contains(fmt.Sprintf("%+v", err), "secret") {
		t.Fatal("input leaked")
	}
}

func TestCardinalCaptureSurvivesNativeMutation(t *testing.T) {
	if LanguageData != "CLDR "+language.CLDRVersion || PluralData != "CLDR "+plural.CLDRVersion {
		t.Fatal("engine/data qualification metadata is stale")
	}
	originalPointer, originalValue := plural.Cardinal, *plural.Cardinal
	defer func() { *originalPointer = originalValue; plural.Cardinal = originalPointer }()
	*plural.Cardinal = *plural.Ordinal
	plural.Cardinal = plural.Ordinal
	if cardinalForm(language.English, 2) != Other || cardinalForm(language.Russian, 21) != One {
		t.Fatal("native mutation changed captured rules")
	}
}

func TestPreparationIgnoresMutableNativeEnglish(t *testing.T) {
	original := language.English
	defer func() { language.English = original }()
	language.English = language.Russian
	catalog, err := Prepare(fixtures(t)[0])
	language.English = original
	if err != nil {
		t.Fatal(err)
	}
	for _, locale := range []string{"en", "ru"} {
		selection := selected(t, catalog, "identity", locale)
		info, err := selection.Metadata()
		expected := NoFallback
		if locale == "ru" {
			expected = UnsupportedLocale
		}
		if err != nil || info.Candidate != "en" || info.Resource.Locale != "en" || info.Fallback != expected {
			t.Fatalf("native global fabricated candidate: %+v %v", info, err)
		}
	}
}

func TestReservedSubtagsAndPrivateUseSequences(t *testing.T) {
	source := fixtures(t)[0]
	english, err := Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	found, _ := english.Lookup("example.inventory:identity", "en")
	for _, locale := range []string{"qaa", "en-Qaaa", "en-QM", "sq-XK", "en-US", "x-private", "en-x-private"} {
		privateSequence := strings.Contains(locale, "x-private")
		translation := sourceJSON(t, "translation", map[string]any{
			"schema": Schema, "profile": Profile, "owner": "example.inventory", "locale": locale,
			"messages": []any{map[string]any{"id": found.Definition.ID, "source": found.Definition.SourceDigest, "forms": map[string]string{"other": found.Definition.ID}}},
		})
		catalog, resourceErr := Prepare(source, translation)
		_, lookupErr := english.Lookup(found.Definition.ID, locale)
		_, requestErr := english.Resolve(found.Definition.ID, locale)
		if privateSequence {
			if catalog != nil || !errors.Is(resourceErr, ErrLocale) || !errors.Is(lookupErr, ErrLocale) || !errors.Is(requestErr, ErrLocale) {
				t.Fatal("private-use sequence accepted", locale, resourceErr, lookupErr, requestErr)
			}
			continue
		}
		if resourceErr != nil || lookupErr != nil || requestErr != nil {
			t.Fatal(locale, resourceErr, lookupErr, requestErr)
		}
		exact, err := catalog.Lookup(found.Definition.ID, locale)
		if err != nil || !exact.TranslationExists || exact.Definition.Locale != locale {
			t.Fatal(locale, exact, err)
		}
	}
}

func FuzzStrictResource(f *testing.F) {
	for _, source := range fixtures(f) {
		f.Add(source.Data)
	}
	for _, input := range []string{`{"x":true,"\u0078":false}`, `{"a":"\ud800"}`, "null"} {
		f.Add([]byte(input))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxDocumentBytes+1 {
			t.Skip()
		}
		catalog, err := Prepare(Source{Name: "fuzz", Data: data})
		if err != nil {
			if catalog != nil {
				t.Fatal("partial publication")
			}
			if current, ok := failure.Inspect(err); !ok || current.Diagnostic().CauseCount != 0 {
				t.Fatal("unowned failure")
			}
			return
		}
		definitions, err := catalog.Inspect()
		if err != nil || len(definitions) == 0 {
			t.Fatal("empty publication")
		}
		for _, definition := range definitions {
			found, err := catalog.Lookup(definition.ID, definition.Locale)
			if err != nil || !found.TranslationExists || !reflect.DeepEqual(found.Definition, definition) {
				t.Fatal("inspection lost entry")
			}
		}
	})
}

func FuzzIntegerCardinal(f *testing.F) {
	for _, number := range []uint64{0, 1, 2, 21, 1000001, 10000001, (1 << 53) + 1, math.MaxUint64} {
		f.Add(number)
	}
	f.Fuzz(func(t *testing.T, number uint64) {
		for _, locale := range []string{"en", "zh-CN", "ru", "ar"} {
			tag, err := parseLocale(locale, true)
			if err != nil || cardinalForm(tag, number) != oracle(locale, number) {
				t.Fatalf("%s %d", locale, number)
			}
		}
	})
}

func TestStrictDecoderBounds(t *testing.T) {
	for _, source := range []Source{
		{Name: "tooBig", Data: bytes.Repeat([]byte(" "), MaxDocumentBytes+1)},
	} {
		expectRejected(t, ErrLimit, source)
	}
	source := sourceJSON(t, "one", document(message("id", "x", map[string]any{}, false)))
	many := make([]Source, MaxSources+1)
	expectRejected(t, ErrLimit, many...)
	for index := range 9 {
		many[index] = Source{Name: fmt.Sprintf("s%d", index), Data: bytes.Repeat([]byte(" "), MaxDocumentBytes)}
	}
	expectRejected(t, ErrLimit, many[:9]...)
	for _, input := range []string{
		strings.Repeat("[", MaxDepth+1) + strings.Repeat("]", MaxDepth+1),
		"{\"" + strings.Repeat("k", MaxKeyBytes+1) + "\":true}",
		"{\"x\":\"" + strings.Repeat("x", MaxTokenBytes+1) + "\"}",
	} {
		expectRejected(t, ErrLimit, Source{Name: "bounded", Data: []byte(input)})
	}
	nodes := MaxNodes
	if _, err := decode(source.Data, &nodes); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}
