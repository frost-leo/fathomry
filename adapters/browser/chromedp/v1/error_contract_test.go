/*
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

package chromedp

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/chromedp/cdproto"
	"github.com/frost-leo/fathomry/failure/v1"
)

func TestErrorsKeepCausesWithoutOrdinaryDisclosure(t *testing.T) {
	native := &cdproto.Error{Code: -32000, Message: "private-browser-cookie-canary"}
	translated := translate(native, "action")
	var inspected *cdproto.Error
	if !errors.As(translated, &inspected) || inspected != native {
		t.Fatal("deliberate native inspection lost")
	}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(verb, translated), native.Message) {
			t.Fatal("ordinary error leaked native data")
		}
	}
	if _, err := json.Marshal(Result{payload: []byte(native.Message)}); !errors.Is(err, ErrSerialization) {
		t.Fatal("runtime evidence serialized", err)
	}
	classified, err := failure.New(definition(ErrInput), failure.Location{Operation: "input"})
	if err != nil {
		t.Fatal(err)
	}
	if got := translate(classified, "again"); got != classified {
		t.Fatal("public error replaced")
	}
	wrapped := fmt.Errorf("private-browser-cookie-canary: %w", classified)
	if got := translate(wrapped, "again"); !errors.Is(got, classified) || strings.Contains(fmt.Sprint(got), native.Message) {
		t.Fatal("public forwarding lost privacy or identity")
	}
}
func TestMalformedSettingsAndInsufficientPolicy(t *testing.T) {
	if err := Validate(Settings{Name: "bad", ExecPath: "/invalid-\xff"}); err == nil {
		t.Fatal("invalid UTF-8 normalized")
	}
	prepared, err := Prepare(Settings{Name: "offline", ExecPath: "/no-browser"}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	dependencies.Runtime = nil
	if owner, err := prepared.Open(bounded(t), dependencies); owner != nil || err == nil {
		t.Fatal("invalid dependencies acquired source")
	}
	var context Context
	if context.Page() != nil || context.Close(bounded(t)) == nil {
		t.Fatal("zero context accepted")
	}
}
