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

package version_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/version"
)

func requireCode(t testing.TB, err error, code failure.Code) {
	t.Helper()
	var occurrence failure.Error
	if !errors.Is(err, code) || !errors.As(err, &occurrence) || occurrence.Code() != code ||
		err.Error() != string(code) || errors.Unwrap(err) != nil {
		t.Fatalf("wanted safe failure %s, got %v", code, err)
	}
}

func parse(t testing.TB, text string) version.Version {
	t.Helper()
	result, err := version.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestVersionIdentityAndPrecedence(t *testing.T) {
	ordered := []string{
		"v1.0.0-alpha", "v1.0.0-alpha.1", "v1.0.0-alpha.beta", "v1.0.0-beta",
		"v1.0.0-beta.2", "v1.0.0-beta.11", "v1.0.0-rc.1", "v1.0.0", "v2.0.0",
		"v999999999999999999999999999999999.0.0",
	}
	for index, text := range ordered {
		value := parse(t, text)
		if value.String() != text || value.State() != version.Release || !value.Equal(parse(t, text)) {
			t.Fatal("identity lost", text)
		}
		if index > 0 {
			before := parse(t, ordered[index-1])
			if order, err := before.Compare(value); err != nil || order != -1 {
				t.Fatal("incorrect forward precedence", order, err)
			}
			if order, err := value.Compare(before); err != nil || order != 1 {
				t.Fatal("incorrect reverse precedence", order, err)
			}
		}
	}
	left, right := parse(t, "v1.2.3+linux.01"), parse(t, "v1.2.3+windows.01")
	if left.Equal(right) || left == right {
		t.Fatal("build metadata was discarded")
	}
	if order, err := left.Compare(right); err != nil || order != 0 {
		t.Fatal("build metadata changed precedence")
	}
	for _, text := range []string{"", "(devel)"} {
		value := parse(t, text)
		want := version.Unknown
		if text != "" {
			want = version.Development
		}
		if value.State() != want || value.String() != text {
			t.Fatal("missing/development state lost")
		}
		_, err := value.Compare(value)
		requireCode(t, err, version.Unordered)
		_, err = left.Compare(value)
		requireCode(t, err, version.Unordered)
	}
	if parse(t, "") != (version.Version{}) {
		t.Fatal("zero is not unknown")
	}
	for _, text := range []string{
		"1.2.3", "v1", "v1.2", "v01.2.3", "v1.02.3", "v1.2.03", "v1.2.3-01",
		"v1.2.3-", "v1.2.3+", "v1.2.3-a..b", "v1.2.3+build..x", "v1.2.3\n",
		" v1.2.3", "unknown", "devel", "v1.2.3-中文", "v1.2.3\x00", strings.Repeat("v", 129),
	} {
		value, err := version.Parse(text)
		requireCode(t, err, version.InvalidVersion)
		if value != (version.Version{}) {
			t.Fatal("failed parse returned a partial version")
		}
	}
	boundary := "v1.2.3+" + strings.Repeat("a", version.MaxVersionBytes-7)
	if parse(t, boundary).String() != boundary {
		t.Fatal("maximum valid version rejected")
	}
	_, err := version.Parse(boundary + "a")
	requireCode(t, err, version.InvalidVersion)
}

func TestTimestampsAndSerializationBoundary(t *testing.T) {
	for _, text := range []string{"", "0001-01-01T00:00:00Z", "1970-01-01T00:00:00Z", "2024-02-29T23:59:59Z", "9999-12-31T23:59:59Z"} {
		value, err := version.ParseTimestamp(text)
		instant, present := value.Time()
		if err != nil || present != (text != "") || value.String() != text {
			t.Fatal("timestamp presence/identity lost", err)
		}
		if text == "1970-01-01T00:00:00Z" && instant.Unix() != 0 {
			t.Fatal("epoch zero is not preserved")
		}
	}
	for _, text := range []string{"0000-01-01T00:00:00Z", "2026-02-29T00:00:00Z", "2026-09-19T00:00:60Z",
		"2026-09-19T00:00:00+00:00", "2026-09-19T00:00:00.1Z", "2026-09-19t00:00:00z", "0"} {
		_, err := version.ParseTimestamp(text)
		requireCode(t, err, version.InvalidTimestamp)
	}
	value := parse(t, "v1.2.3")
	before := value
	for _, item := range []any{value, version.Build{}, version.Snapshot{}, version.Source{}, version.Claim{}, version.Request{}, version.Declaration{}, version.Fact{}, version.Module{}, version.Replacement{}, version.Setting{}, version.Timestamp{}} {
		if _, err := json.Marshal(item); !errors.Is(err, version.SerializationUnsupported) {
			t.Fatal("invented JSON protocol", err)
		}
	}
	if err := json.Unmarshal([]byte("{}"), &value); !errors.Is(err, version.SerializationUnsupported) || value != before {
		t.Fatal("reconstruction mutated the value")
	}
}

func TestRetainedStringsOwnBoundedStorage(t *testing.T) {
	t.Run("development", func(t *testing.T) {
		storage := strings.Repeat("x", 1<<20) + "(devel)"
		input := storage[len(storage)-len("(devel)"):]
		value := parse(t, input)
		if unsafe.StringData(value.String()) == unsafe.StringData(input) {
			t.Fatal("development value retains caller backing storage")
		}
	})
	for _, tree := range []version.TreeState{version.Clean, version.Dirty} {
		t.Run(string(tree), func(t *testing.T) {
			storage := strings.Repeat("x", 1<<20) + string(tree)
			input := storage[len(storage)-len(tree):]
			claim := declaration()
			claim.Tree = version.TreeState(input)
			build, err := version.WithDeclaration(version.Build{}, claim)
			if err != nil {
				t.Fatal(err)
			}
			if unsafe.StringData(string(build.Snapshot().Declaration.Tree)) == unsafe.StringData(input) {
				t.Fatal("tree value retains caller backing storage")
			}
		})
	}
}

func FuzzVersion(f *testing.F) {
	for _, text := range []string{"", "(devel)", "v1.2.3", "v1.2.3-rc.1+linux", "v1.2", "v1.2.3-01"} {
		f.Add(text)
	}
	f.Fuzz(func(t *testing.T, text string) {
		value, err := version.Parse(text)
		if err != nil {
			requireCode(t, err, version.InvalidVersion)
			return
		}
		if value.String() != text || value != parse(t, value.String()) {
			t.Fatal("parse identity changed")
		}
		if value.State() == version.Release {
			if order, err := value.Compare(value); err != nil || order != 0 {
				t.Fatal("reflexive precedence failed")
			}
		}
	})
}
