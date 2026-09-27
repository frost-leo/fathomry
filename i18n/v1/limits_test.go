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
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/text/feature/plural"
	"golang.org/x/text/language"
)

func catalogWithPattern(t testing.TB, pattern string, args map[string]any) *Catalog {
	t.Helper()
	catalog, err := Prepare(sourceJSON(t, "pattern", document(message("text", pattern, args, false))))
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestRenderCapacityBeforeExpansion(t *testing.T) {
	argument := map[string]any{"x": map[string]any{"kind": "string", "meaning": "Bounded data."}}
	for _, references := range []int{16, 17, 64} {
		catalog := catalogWithPattern(t, strings.Repeat("{x}", references), argument)
		selection, err := catalog.Resolve("test:text", "en")
		if err != nil {
			t.Fatal(err)
		}
		args := []Argument{{Name: "x", Value: strings.Repeat("x", MaxStringBytes)}}
		result, err := selection.Render(args, nil)
		if references == 16 {
			if err != nil || len(result.Text) != MaxOutputBytes {
				t.Fatal(result, err)
			}
		} else if !isLimit(err) || result != (Rendered{}) {
			t.Fatal("over-limit text materialized", err)
		}
	}
	catalog := catalogWithPattern(t, "{x}", argument)
	selection, _ := catalog.Resolve("test:text", "en")
	if _, err := selection.Render([]Argument{{Name: "x", Value: strings.Repeat("x", MaxStringBytes+1)}}, nil); !isLimit(err) {
		t.Fatal(err)
	}
	expectRejected(t, ErrLimit, sourceJSON(t, "references", document(message("text", strings.Repeat("{x}", 65), argument, false))))
	for _, count := range []int{4, 5} {
		arguments := map[string]any{}
		supplied := []Argument{}
		pattern := ""
		for index := range count {
			name := fmt.Sprintf("a%d", index)
			arguments[name] = map[string]any{"kind": "string", "meaning": "Data."}
			supplied = append(supplied, Argument{Name: name, Value: strings.Repeat("x", MaxStringBytes)})
			pattern += "{" + name + "}"
		}
		selection, _ := catalogWithPattern(t, pattern, arguments).Resolve("test:text", "en")
		result, err := selection.Render(supplied, nil)
		if count == 4 && (err != nil || len(result.Text) != MaxArgumentBytes) || count == 5 && (!isLimit(err) || result != (Rendered{})) {
			t.Fatal(count, err)
		}
	}
}

func isLimit(err error) bool {
	return errors.Is(err, ErrLimit)
}

func TestCatalogAdmissionBoundaries(t *testing.T) {
	var sources []Source
	for index := range MaxSources {
		sources = append(sources, sourceJSON(t, fmt.Sprintf("s%d", index), document(message(fmt.Sprintf("id%d", index), "x", map[string]any{}, false))))
	}
	if _, err := Prepare(sources...); err != nil {
		t.Fatal("at document limit", err)
	}
	for _, count := range []int{MaxMessages, MaxMessages + 1} {
		sources = nil
		for start := 0; start < count; start += 32 {
			var messages []any
			for index := start; index < start+32 && index < count; index++ {
				messages = append(messages, message(fmt.Sprintf("id%d", index), "x", map[string]any{}, false))
			}
			sources = append(sources, sourceJSON(t, fmt.Sprintf("s%d", start), document(messages...)))
		}
		catalog, err := Prepare(sources...)
		if count == MaxMessages {
			if err != nil {
				t.Fatal(err)
			}
			snapshot, _ := catalog.Inspect()
			encoded, _ := json.Marshal(snapshot)
			charge := 0
			for _, definition := range snapshot {
				charge += inspectionCharge(definition)
			}
			if len(snapshot) != MaxMessages || len(encoded) > charge || charge > MaxInspectionBytes {
				t.Fatal("incomplete/unbudgeted inspection", len(encoded), charge)
			}
		} else if catalog != nil || !isLimit(err) {
			t.Fatal("message admission", err)
		}
	}
	// Each form is individually within limits; retained aggregate segments are not.
	sources = nil
	args := map[string]any{"x": map[string]any{"kind": "string", "meaning": "Data."}}
	for start := 0; start < 129; start += 16 {
		var messages []any
		for index := start; index < start+16 && index < 129; index++ {
			messages = append(messages, message(fmt.Sprintf("id%d", index), strings.Repeat("a{x}", 64), args, false))
		}
		sources = append(sources, sourceJSON(t, fmt.Sprintf("s%d", start), document(messages...)))
	}
	expectRejected(t, ErrLimit, sources...)
	// A bounded raw catalog may still exceed the complete inspection envelope.
	sources = nil
	for start := 0; start < 512; start += 16 {
		var messages []any
		for index := start; index < start+16; index++ {
			messages = append(messages, message(fmt.Sprintf("id%d", index), strings.Repeat("x", 820), map[string]any{}, false))
		}
		sources = append(sources, sourceJSON(t, fmt.Sprintf("s%0127d", start), document(messages...)))
	}
	raw := 0
	for _, source := range sources {
		raw += len(source.Data)
		if len(source.Data) > MaxDocumentBytes {
			t.Fatal("wrong boundary fixture")
		}
	}
	if raw > MaxInputBytes {
		t.Fatal("not inspection boundary", raw)
	}
	expectRejected(t, ErrLimit, sources...)
}

func TestEscapingInspectionEnvelope(t *testing.T) {
	args := map[string]any{"x": map[string]any{"kind": "string", "meaning": strings.Repeat("\x01", 256)}}
	msg := message("escaping", strings.Repeat("\x01", 8192-3)+"{x}", args, false)
	msg["context"] = strings.Repeat("\x01", 1024)
	source := sourceJSON(t, "escaping", document(msg))
	if len(source.Data) > MaxDocumentBytes {
		t.Fatal("fixture oversize")
	}
	catalog, err := Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := catalog.Inspect()
	data, err := json.Marshal(snapshot)
	if err != nil || len(data) > inspectionCharge(snapshot[0]) {
		t.Fatal("escaping undercharged", len(data))
	}
	t.Logf("encoded=%d admitted=%d", len(data), inspectionCharge(snapshot[0]))
}

func TestRejectedExpansionAllocation(t *testing.T) {
	catalog := catalogWithPattern(t, strings.Repeat("{x}", 64), map[string]any{"x": map[string]any{"kind": "string", "meaning": "Data."}})
	selection, _ := catalog.Resolve("test:text", "en")
	args := []Argument{{Name: "x", Value: strings.Repeat("x", 4096)}}
	for range 10 {
		_, _ = selection.Render(args, nil)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 1000 {
		result, err := selection.Render(args, nil)
		if !isLimit(err) || result != (Rendered{}) {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	bytesPerCall := (after.TotalAlloc - before.TotalAlloc) / 1000
	if bytesPerCall > 8192 {
		t.Fatalf("rejected 256KiB expansion allocated %d bytes/call", bytesPerCall)
	}
	t.Logf("rejected expansion allocated %d bytes/call", bytesPerCall)
}

func TestRepresentativeMatchesPinnedNative(t *testing.T) {
	if strconvIntSize() != 64 {
		t.Skip("wide native int comparison requires 64-bit; uint64 oracle tests run on 386")
	}
	bases := language.Supported.BaseLanguages()
	if len(bases) < 100 {
		t.Fatal("vacuous native language coverage")
	}
	count := 0
	for _, base := range bases {
		tag, err := language.BCP47.Parse(base.String())
		if err != nil {
			continue
		}
		for _, number := range []uint64{0, 1, 2, 11, 21, 800, 1000000, 1000001, 9999999, 10000000, 10000001, 1000000001, 9007199254740993, 9223372036854775807} {
			want := nativeForm(cardinalRules.MatchPlural(tag, int(number), 0, 0, 0, 0))
			if got := cardinalForm(tag, number); got != want {
				t.Fatalf("%s %d: %s != %s", tag, number, got, want)
			}
			count++
		}
	}
	t.Logf("native equivalence comparisons=%d", count)
}

func strconvIntSize() int { return 32 << (^uint(0) >> 63) }
func nativeForm(form plural.Form) Form {
	switch form {
	case plural.Zero:
		return Zero
	case plural.One:
		return One
	case plural.Two:
		return Two
	case plural.Few:
		return Few
	case plural.Many:
		return Many
	default:
		return Other
	}
}

func BenchmarkRejectedExpansion(b *testing.B) {
	catalog := catalogWithPattern(b, strings.Repeat("{x}", 64), map[string]any{"x": map[string]any{"kind": "string", "meaning": "Data."}})
	selection, _ := catalog.Resolve("test:text", "en")
	args := []Argument{{Name: "x", Value: strings.Repeat("x", 4096)}}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = selection.Render(args, nil)
	}
}

func BenchmarkCompleteInspection(b *testing.B) {
	catalog := preparedFixture(b)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = catalog.Inspect()
	}
}

func TestLocaleAndEntryAdmission(t *testing.T) {
	locales := []string{"en", "ar", "de", "es", "fr", "ru", "zh-CN", "ja", "ko", "he", "it", "nl", "pl", "pt", "ro", "sv", "tr", "uk", "vi", "th", "hi", "id", "ms", "fi", "da", "no", "cs", "el", "hu", "bg", "fa", "ur"}
	var englishMessages []any
	for index := range 34 {
		englishMessages = append(englishMessages, message(fmt.Sprintf("id%d", index), "x", map[string]any{}, false))
	}
	english := sourceJSON(t, "english", document(englishMessages...))
	sourceCatalog, err := Prepare(english)
	if err != nil {
		t.Fatal(err)
	}
	definitions, _ := sourceCatalog.Inspect()
	var translated []any
	for _, definition := range definitions {
		translated = append(translated, map[string]any{"id": definition.ID, "source": definition.SourceDigest, "forms": map[string]any{"other": "x"}})
	}
	for _, entries := range []int{32, 33} {
		sources := []Source{english}
		for index, locale := range locales[1:] {
			// 34 English + 30*32 + 30 = 1024; then 34 + 31*33 = 1057.
			count := entries
			if entries == 32 && index == 30 {
				count = 30
			}
			sources = append(sources, sourceJSON(t, fmt.Sprintf("locale%d", index), map[string]any{"schema": Schema, "profile": Profile, "owner": "test", "locale": locale, "messages": translated[:count]}))
		}
		catalog, err := Prepare(sources...)
		if entries == 32 {
			if err != nil {
				t.Fatal("inclusive locale/entry boundary", err)
			}
			actual, _ := catalog.Inspect()
			if len(actual) != MaxEntries || len(catalog.locales) != MaxLocales {
				t.Fatal("wrong positive boundary")
			}
		} else if err == nil || catalog != nil {
			t.Fatal("entry boundary accepted")
		}
	}
}

func TestRawAndNodeInclusiveBoundaries(t *testing.T) {
	var sources []Source
	for index := range 8 {
		source := sourceJSON(t, fmt.Sprintf("s%d", index), document(message(fmt.Sprintf("id%d", index), " ", map[string]any{}, false)))
		source.Data = append(source.Data, []byte(strings.Repeat(" ", MaxDocumentBytes-len(source.Data)))...)
		sources = append(sources, source)
	}
	catalog, err := Prepare(sources...)
	if err != nil {
		t.Fatal("inclusive input boundary", err)
	}
	selection, _ := catalog.Resolve("test:id0", "en")
	if result := rendered(t, selection, nil, nil); result.Text != " " {
		t.Fatal("whitespace is authored content")
	}
	expectRejected(t, ErrLimit, append(sources, Source{Name: "overflow", Data: []byte(" ")})...)
	nodes := MaxNodes - 1
	if _, err := decode([]byte("{}"), &nodes); err != nil || nodes != MaxNodes {
		t.Fatal("inclusive node boundary", err)
	}
	if _, err := decode([]byte("{}"), &nodes); !isLimit(err) {
		t.Fatal("aggregate nodes", err)
	}
}
