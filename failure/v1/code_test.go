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

package failure_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

func TestCode(t *testing.T) {
	t.Run("full-width parsing and encoding", func(t *testing.T) {
		if reflect.TypeFor[failure.Code]().Kind() != reflect.Uint32 || reflect.TypeFor[failure.Code]().Size() != 4 {
			t.Fatal("public identity is not exactly uint32")
		}
		for _, number := range []uint64{0xA0010001, 0xA001FFFF, 0xA3FFFFFF, 0xA4000001, 0xA7FFFFFF} {
			code := failure.Code(number)
			for _, text := range []string{code.String(), strconv.FormatUint(number, 10), "0x" + strconv.FormatUint(number, 16)} {
				got, err := failure.ParseCode(text)
				if err != nil || got != code {
					t.Fatalf("parse %q: %v", text, err)
				}
			}
			raw, err := json.Marshal(code)
			if err != nil || len(raw) != 12 || raw[0] != '"' {
				t.Fatalf("code was not a fixed-width JSON string: %s %v", raw, err)
			}
			var decoded failure.Code
			if err := json.Unmarshal(raw, &decoded); err != nil || decoded != code {
				t.Fatalf("full-width identity lost: %s %v", raw, err)
			}
			text, err := code.MarshalText()
			if err != nil || string(text) != code.String() {
				t.Fatal("text encoding changed", err)
			}
		}
	})
	t.Run("reject without receiver mutation", func(t *testing.T) {
		for _, text := range []string{"", "0", "0x0", "0x", "-1", "+1", " 1", "1 ", "1_0", "1e3", "0b1", "0xG", "4294967296", "0x100000000", "18446744073709551616", "0x8000000100000001", "0x00000000A0010001", "0x80070005", "0xC0000005", "0xA0000001", "0xA0010000", "0xFFFFFFFF"} {
			code := failure.ErrCode
			if _, err := failure.ParseCode(text); !errors.Is(err, failure.ErrCode) {
				t.Fatalf("invalid spelling accepted: %q %v", text, err)
			}
			if err := code.UnmarshalText([]byte(text)); !errors.Is(err, failure.ErrCode) || code != failure.ErrCode {
				t.Fatalf("invalid text mutated receiver: %q", text)
			}
		}
		for _, raw := range []string{"null", "1", "2684420097", "true", "{}", "[]", `"0"`, `"0x0000000100000001"`} {
			code := failure.ErrCode
			if err := json.Unmarshal([]byte(raw), &code); !errors.Is(err, failure.ErrCode) || code != failure.ErrCode {
				t.Fatalf("invalid JSON accepted or mutated receiver: %s %v", raw, err)
			}
		}
		code := failure.ErrCode
		if err := code.UnmarshalJSON([]byte(`"0xA0020001" "0xA0030001"`)); !errors.Is(err, failure.ErrCode) || code != failure.ErrCode {
			t.Fatal("direct JSON hook accepted trailing input", err)
		}
		var syntax *json.SyntaxError
		if err := json.Unmarshal([]byte(`"0xA0020001" "0xA0030001"`), &code); !errors.As(err, &syntax) || code != failure.ErrCode {
			t.Fatal("native JSON preflight did not reject before hook dispatch", err)
		}
		var nilCode *failure.Code
		if err := nilCode.UnmarshalText([]byte("1")); !errors.Is(err, failure.ErrCode) {
			t.Fatal(err)
		}
		if err := nilCode.UnmarshalJSON([]byte(`"1"`)); !errors.Is(err, failure.ErrCode) {
			t.Fatal(err)
		}
		if _, err := json.Marshal(failure.Code(0)); !errors.Is(err, failure.ErrCode) {
			t.Fatal("zero code serialized", err)
		}
	})
	t.Run("identity is not an identifier", func(t *testing.T) {
		if _, ok := any(failure.Identifier("example.source.failed")).(error); ok {
			t.Fatal("symbolic identifier became an error-code target")
		}
		for _, identifier := range []failure.Identifier{"example.source.failed", "example.source.read_failed"} {
			if !identifier.Valid() {
				t.Fatal(identifier)
			}
		}
		for _, identifier := range []failure.Identifier{"", "one", ".bad", "example..bad", "example.bad ", "example.secret/token"} {
			if identifier.Valid() {
				t.Fatal("invalid identifier admitted")
			}
		}
		if code, err := failure.ParseCode(strconv.FormatUint(uint64(failure.ErrCode), 10)); err != nil || code != failure.ErrCode {
			t.Fatal("decimal identity changed", code, err)
		}
	})
}

func FuzzCode(f *testing.F) {
	for _, seed := range []string{"0xA7FFFFFF", "0xA0010001", "2684420097", "0x8000000100000001", "0", "-1", "example.source.failed"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		code, err := failure.ParseCode(text)
		if err != nil {
			if code != 0 || !errors.Is(err, failure.ErrCode) {
				t.Fatal("invalid parse result")
			}
			return
		}
		rebuilt, err := failure.MakeCode(code.Facility(), code.Number())
		if err != nil || rebuilt != code {
			t.Fatal("decoded fields cannot rebuild the code")
		}
		raw, err := json.Marshal(code)
		if err != nil {
			t.Fatal(err)
		}
		var other failure.Code
		if err := json.Unmarshal(raw, &other); err != nil || other != code || !other.Valid() {
			t.Fatal("round trip changed identity", err)
		}
	})
}
