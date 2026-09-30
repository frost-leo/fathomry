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

package settings

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
)

type readPreference struct {
	Language  string   `json:"language"`
	Fallbacks []string `json:"fallbacks"`
}
type readEmbedded struct {
	Promoted int `json:"promoted"`
}
type readKey string
type readRoot struct {
	Application *readPreference    `json:"application,omitempty"`
	Map         map[readKey]string `json:"map"`
	List        []int              `json:"list"`
	Array       [2]int             `json:"array"`
	Empty       any                `json:"empty"`
	Dynamic     any                `json:"dynamic"`
	Nil         *readPreference    `json:"nil"`
	Unsupported map[int]int        `json:"unsupported"`
	Duplicate   any                `json:"duplicate"`
	Untagged    string
	Hidden      string `json:"-"`
	OptionOnly  string `json:",omitempty"`
	private     string
	readEmbedded
	Named readEmbedded `json:"named"`
}

func TestRead(t *testing.T) {
	duplicateType := reflect.StructOf([]reflect.StructField{
		{Name: "First", Type: reflect.TypeFor[int](), Tag: `json:"same"`},
		{Name: "Second", Type: reflect.TypeFor[int](), Tag: `json:"same"`},
		{Name: "Unique", Type: reflect.TypeFor[int](), Tag: `json:"unique"`},
	})
	root := readRoot{
		Duplicate:   reflect.New(duplicateType).Elem().Interface(),
		Application: &readPreference{Language: "en", Fallbacks: []string{"zh-CN"}},
		Map:         map[readKey]string{"a/b": "slash", "~": "tilde", "~1": "order", "": "empty-key", "日本語": "unicode"},
		List:        []int{0, 2}, Array: [2]int{3, 4}, Dynamic: map[string]int{"count": 7},
		readEmbedded: readEmbedded{Promoted: 8}, Named: readEmbedded{Promoted: 9},
	}
	// This fixture is immutable; production mutable data requires an isolating copier.
	view := mustSnapshot(t, root, identity[readRoot]).View()
	t.Run("named_edges_and_escaping", func(t *testing.T) {
		for path, want := range map[string]string{
			"/application/language": "en", "/map/a~1b": "slash", "/map/~0": "tilde",
			"/map/~01": "order", "/map/": "empty-key", "/map/日本語": "unicode",
		} {
			got, found, err := Read(view, path, identity[string])
			if err != nil || !found || got != want {
				t.Fatalf("selection %q failed", path)
			}
		}
		for path, want := range map[string]int{"/list/0": 0, "/array/1": 4, "/dynamic/count": 7, "/named/promoted": 9, "/duplicate/unique": 0} {
			got, found, err := Read(view, path, identity[int])
			if err != nil || !found || got != want {
				t.Fatalf("selection %q failed", path)
			}
		}
	})
	t.Run("missing_is_not_zero_or_error", func(t *testing.T) {
		for _, path := range []string{"/absent", "/application/missing", "/map/missing", "/list/2", "/array/9", "/nil/language",
			"/Untagged", "/Hidden", "/private", "/promoted", "/OptionOnly"} {
			calls := 0
			_, found, err := Read(view, path, func(value string) string { calls++; return value })
			if err != nil || found || calls != 0 {
				t.Fatalf("missing selection %q was not missing", path)
			}
		}
		value, found, err := Read(view, "/empty", identity[any])
		if err != nil || !found || value != nil {
			t.Fatal("nil interface was absent")
		}
		pointer, found, err := Read(view, "/nil", identity[*readPreference])
		if err != nil || !found || pointer != nil {
			t.Fatal("nil pointer was absent")
		}
		privateType := reflect.StructOf([]reflect.StructField{
			{Name: "private", PkgPath: "settings", Type: reflect.TypeFor[string](), Tag: `json:"secret"`},
		})
		privateView := mustSnapshot(t, reflect.New(privateType).Elem().Interface(), identity[any]).View()
		if _, found, err := Read(privateView, "/secret", identity[string]); found || err != nil {
			t.Fatal("tagged private field became a readable edge")
		}
	})
	t.Run("type_and_syntax_rejections_do_not_call_clone", func(t *testing.T) {
		cases := []struct {
			path string
			code error
		}{
			{"/application", ErrType}, {"/empty", ErrType}, {"/dynamic", ErrType}, {"/list/0/extra", ErrType},
			{"/unsupported/key", ErrType}, {"/duplicate/same", ErrPath},
			{"application", ErrPath}, {"#/application", ErrPath}, {"/map/~", ErrPath}, {"/map/~2", ErrPath},
			{"/missing/~2", ErrPath}, {"/map/\xff", ErrPath}, {"/list/01", ErrPath}, {"/list/-", ErrPath},
			{"/list/+1", ErrPath}, {"/list/", ErrPath}, {"/list/18446744073709551616", ErrPath},
			{"/" + strings.Repeat("x", MaxPathBytes), ErrPath}, {strings.Repeat("/next", MaxPathSegments+1), ErrPath},
		}
		for _, item := range cases {
			_, found, err := Read(view, item.path, func(value int) int { t.Fatal("clone ran on rejection"); return value })
			if found || !errors.Is(err, item.code) {
				t.Fatalf("wrong rejection for %q: %v", item.path, err)
			}
		}
		if _, _, err := Read[int](View{}, "", identity[int]); !errors.Is(err, ErrSnapshot) {
			t.Fatal("zero view accepted")
		}
		if _, _, err := Read[int](view, "", nil); !errors.Is(err, ErrCopy) {
			t.Fatal("nil copy accepted")
		}
	})
	t.Run("selected_copy_not_root_copy", func(t *testing.T) {
		rootCalls, sectionCalls := 0, 0
		snapshot := mustSnapshot(t, root, func(value readRoot) readRoot {
			rootCalls++
			preference := *value.Application
			preference.Fallbacks = slices.Clone(preference.Fallbacks)
			value.Application = &preference
			return value
		})
		clone := func(value *readPreference) *readPreference {
			sectionCalls++
			result := *value
			result.Fallbacks = slices.Clone(value.Fallbacks)
			return &result
		}
		first, found, err := Read(snapshot.View(), "/application", clone)
		if err != nil || !found {
			t.Fatal("section missing")
		}
		first.Language, first.Fallbacks[0] = "changed", "changed"
		second, _, _ := Read(snapshot.View(), "/application", clone)
		if second.Language != "en" || second.Fallbacks[0] != "zh-CN" || rootCalls != 1 || sectionCalls != 2 {
			t.Fatal("wrong copy scope or aliased section")
		}
	})
	t.Run("bounded_cycles_and_boundary_paths", func(t *testing.T) {
		var cycle any
		cycle = &cycle
		cyclic := mustSnapshot(t, cycle, identity[any]).View()
		if _, _, err := Read(cyclic, "/next", identity[int]); !errors.Is(err, ErrPath) {
			t.Fatal("cycle not bounded")
		}
		type node struct {
			Next  *node `json:"next"`
			Value int   `json:"value"`
		}
		linked := &node{Value: 9}
		linked.Next = linked
		linkedView := mustSnapshot(t, linked, identity[*node]).View()
		got, found, err := Read(linkedView, strings.Repeat("/next", MaxPathSegments-1)+"/value", identity[int])
		if err != nil || !found || got != 9 {
			t.Fatal("exact depth boundary rejected")
		}
		key := strings.Repeat("x", MaxPathBytes-1)
		longest := mustSnapshot(t, map[string]int{key: 1}, identity[map[string]int]).View()
		if _, found, err := Read(longest, "/"+key, identity[int]); err != nil || !found {
			t.Fatal("exact byte boundary rejected")
		}
	})
	t.Run("exact_declared_leaf_type_and_root", func(t *testing.T) {
		got, found, err := Read(view, "", identity[readRoot])
		if err != nil || !found || !reflect.DeepEqual(got, root) {
			t.Fatal("empty path was not root")
		}
		dynamic, found, err := Read(view, "/dynamic", identity[any])
		if err != nil || !found || !reflect.DeepEqual(dynamic, root.Dynamic) {
			t.Fatal("declared interface leaf lost")
		}
		if _, _, err := Read(view, "/dynamic", identity[map[string]int]); !errors.Is(err, ErrType) {
			t.Fatal("interface leaf silently unwrapped")
		}
		mapView := mustSnapshot(t, map[string]any{"nil": nil}, identity[map[string]any]).View()
		if got, found, err := Read(mapView, "/nil", identity[any]); err != nil || !found || got != nil {
			t.Fatal("map nil interface extraction failed")
		}
	})
}

func FuzzRead(f *testing.F) {
	for _, seed := range []string{"", "/value", "/unknown", "/value/~2", "/~01", "/list/00", "/list/0", "\xff", strings.Repeat("/", 65)} {
		f.Add(seed)
	}
	snapshot, err := New(struct {
		Value int   `json:"value"`
		List  []int `json:"list"`
	}{Value: 7, List: []int{0, 1}},
		func(value struct {
			Value int   `json:"value"`
			List  []int `json:"list"`
		}) struct {
			Value int   `json:"value"`
			List  []int `json:"list"`
		} {
			value.List = slices.Clone(value.List)
			return value
		})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, path string) {
		calls := 0
		value, found, err := Read(snapshot.View(), path, func(value int) int { calls++; return value })
		if err != nil || !found {
			if calls != 0 || found || value != 0 {
				t.Fatal("rejected selection invoked copy or returned data")
			}
		} else if calls != 1 || value != 7 && value != 0 && value != 1 {
			t.Fatal("invalid selected result")
		}
	})
}

var benchmarkPreference readPreference

func BenchmarkRead(b *testing.B) {
	type config struct {
		Preference readPreference `json:"preference"`
		Payload    []byte
	}
	snapshot := mustSnapshot(b, config{Preference: readPreference{Language: "en", Fallbacks: []string{"zh-CN"}}, Payload: make([]byte, 128<<10)},
		func(value config) config {
			value.Preference.Fallbacks = slices.Clone(value.Preference.Fallbacks)
			value.Payload = slices.Clone(value.Payload)
			return value
		})
	b.Run("whole_project", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			value, err := snapshot.ValueCopy()
			if err != nil {
				b.Fatal(err)
			}
			benchmarkPreference = value.Preference
		}
	})
	b.Run("selected_subtree", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			value, found, err := Read(snapshot.View(), "/preference", func(value readPreference) readPreference {
				value.Fallbacks = slices.Clone(value.Fallbacks)
				return value
			})
			if err != nil || !found {
				b.Fatal("section not found")
			}
			benchmarkPreference = value
		}
	})
}
