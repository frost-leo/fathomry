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

package catalog

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/frost-leo/fathomry/failure/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"strings"
	"testing"
)

func TestDocumentEncodingDoesNotShrinkTheProfile(t *testing.T) {
	value := View{Documents: []StoredDocument{{Canonical: bytes.Repeat([]byte("<"), declaration.MaxSpecBytes)}, {Canonical: bytes.Repeat([]byte(">"), declaration.MaxModeBytes)}}}
	if err := viewBound(value); err != nil {
		t.Fatal("JSON transport shrank admitted document bounds", err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var copied View
	if err := json.Unmarshal(raw, &copied); err != nil || !bytes.Equal(copied.Documents[0].Canonical, value.Documents[0].Canonical) {
		t.Fatal("document JSON lost byte identity", err)
	}
}

func TestReadRequestAndCursorBounds(t *testing.T) {
	parent := declaration.Identity{Namespace: "example"}
	for _, bad := range []declaration.Identity{{}, {Namespace: "example", Version: "1"}, {Namespace: "example", Code: "x", Mode: "mode"}, {Namespace: "bad\n"}, {Namespace: "example", Code: "x", Version: "1", Mode: "mode", Revision: "1"}} {
		if _, err := listKind(bad); !errors.Is(err, ErrInput) {
			t.Fatal("invalid list scope admitted")
		}
	}
	request := ListRequest{Parent: parent, Locale: "zh-cn"}
	hash := cursorBinding("public", true, request, "zh-CN", Definition)
	cursor := writeCursor(hash, "opaque ")
	for _, label := range []string{strings.Repeat("<", 128), strings.Repeat("\"", 128), strings.Repeat("界", 42)} {
		encoded := writeCursor(hash, label)
		if got, err := readCursor(encoded, hash); err != nil || got != label {
			t.Fatal("admitted label cannot be paged", err)
		}
	}
	if got, err := readCursor(cursor, hash); err != nil || got != "opaque " {
		t.Fatal("cursor identity changed", err)
	}
	if _, err := readCursor(cursor, strings.Repeat("0", 64)); !errors.Is(err, ErrInput) {
		t.Fatal("foreign cursor admitted")
	}
	for _, bad := range []string{"?", cursor + "=", strings.Repeat("x", MaxCursorBytes+1), base64.RawURLEncoding.EncodeToString([]byte("{\"v\":1,\"v\":1}"))} {
		if _, err := readCursor(bad, hash); err == nil {
			t.Fatal("malformed cursor admitted")
		}
	}
	enabled := false
	other := request
	other.Enabled = &enabled
	if cursorBinding("public", true, other, "zh-CN", Definition) == hash {
		t.Fatal("absent filter became false")
	}
	other = request
	other.Limit = 1
	if cursorBinding("public", true, other, "zh-CN", Definition) != hash {
		t.Fatal("page-size change unnecessarily invalidated traversal")
	}
	if _, err := (*Catalog)(nil).List(context.Background(), request); !errors.Is(err, ErrInput) {
		t.Fatal("nil catalog accepted")
	}
	if _, err := readLocale("zh_CN"); !errors.Is(err, ErrInput) {
		t.Fatal("invalid locale admitted")
	}
	if got, err := readLocale("zh-cn-u-ca-chinese"); err != nil || got != "zh-CN-u-ca-chinese" {
		t.Fatal("locale extension stripped", got, err)
	}
}
func TestLocalizedPresenceAndFallback(t *testing.T) {
	row := [][]byte{[]byte("1"), []byte("English"), []byte("")}
	actual, err := readLocalized(row, false, false, "zh-CN", "none")
	if err != nil || !actual.Present || actual.Value != "" || actual.Locale != "zh-CN" || actual.Fallback != "none" {
		t.Fatal("present empty translation fell back", err)
	}
	row[2] = nil
	actual, err = readLocalized(row, true, true, "zh-CN", "none")
	if err != nil || actual.Value != "English" || actual.Fallback != "missing_translation" {
		t.Fatal(err)
	}
	for _, reason := range []string{"unknown_locale", "disabled_locale"} {
		actual, err = readLocalized(row, true, true, "zh-CN", reason)
		if err != nil || actual.Fallback != reason || actual.Locale != "en" {
			t.Fatal(err)
		}
	}
	row[1] = nil
	row[2] = []byte("translation")
	if _, err := readLocalized(row, true, true, "zh-CN", "none"); !errors.Is(err, ErrStored) {
		t.Fatal("translation hid missing English")
	}
	absent, err := readLocalized(make([][]byte, 3), false, false, "en", "none")
	if err != nil || absent.Present || absent.Fallback != "resource_absent" {
		t.Fatal(err)
	}
	if _, err := readLocalized(make([][]byte, 3), true, true, "en", "none"); !errors.Is(err, ErrStored) {
		t.Fatal("missing name fabricated")
	}
}
func TestReadOnlyStateDoesNotRewritePublication(t *testing.T) {
	original := Publication{Effect: Applied, Acknowledgement: CommitAcknowledged, DefinitionID: "1", CleanupComplete: true}
	state := &catalogState{reading: true, accepted: true, last: original}
	state.observe(publicationFinal{acknowledgement: "rollback-acknowledged", released: true})
	if state.last != original || state.reading {
		t.Fatal("read cleanup rewrote publication")
	}
	tx := &publicationTx{readOnly: true}
	if _, err := tx.register(context.Background(), nil); !errors.Is(err, ErrState) {
		t.Fatal("read transaction entered writer")
	}
	if _, err := readID([]byte("01"), false); !errors.Is(err, ErrStored) {
		t.Fatal("aliased ID admitted")
	}
	if _, err := readTimestamp([]byte("today")); !errors.Is(err, ErrStored) {
		t.Fatal("invalid timestamp admitted")
	}
	raw, err := json.Marshal(Page{Items: []Entity{}})
	if err != nil || len(raw) == 0 {
		t.Fatal(err)
	}
	var details *failure.Detailed[Diagnostic]
	if !errors.As(readError(ErrInput, "locale"), &details) {
		t.Fatal("unstructured read diagnostic")
	}
}
func FuzzReadCursor(f *testing.F) {
	binding := strings.Repeat("a", 64)
	f.Add(writeCursor(binding, "one"))
	f.Add("")
	f.Fuzz(func(t *testing.T, value string) {
		after, err := readCursor(value, binding)
		if err == nil && value != "" && (writeCursor(binding, after) != value || !readIdentityText(after, 128, false)) {
			t.Fatal("noncanonical cursor admitted")
		}
	})
}
