/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package chromedp

import (
	"context"
	"errors"
	"testing"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/runtime"
)

func TestManagedDebuggerArgumentPresence(t *testing.T) {
	for _, fixture := range []struct {
		name     string
		argument *runtime.CallArgument
		want     string
	}{
		{"empty-string", &runtime.CallArgument{Value: jsonv2.Value(`""`)}, `"value":""`},
		{"empty-array", &runtime.CallArgument{Value: jsonv2.Value(`[]`)}, `"value":[]`},
		{"empty-object", &runtime.CallArgument{Value: jsonv2.Value(`{}`)}, `"value":{}`},
		{"null", &runtime.CallArgument{Value: jsonv2.Value(`null`)}, `"value":null`},
		{"false", &runtime.CallArgument{Value: jsonv2.Value(`false`)}, `"value":false`},
		{"zero", &runtime.CallArgument{Value: jsonv2.Value(`0`)}, `"value":0`},
		{"undefined", &runtime.CallArgument{}, ""},
		{"special", &runtime.CallArgument{UnserializableValue: "NaN"}, `"unserializableValue":"NaN"`},
		{"reference", &runtime.CallArgument{ObjectID: "owned-reference"}, `"objectId":"owned-reference"`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			variable := debugger.SetVariableValueParams{ScopeNumber: 0, VariableName: "local", CallFrameID: "paused-frame", NewValue: fixture.argument}
			returned := debugger.SetReturnValueParams{NewValue: fixture.argument}
			for _, command := range []struct {
				method string
				params any
			}{
				{debugger.CommandSetVariableValue, variable}, {debugger.CommandSetVariableValue, &variable},
				{debugger.CommandSetReturnValue, returned}, {debugger.CommandSetReturnValue, &returned},
			} {
				data, err := encodeCall(WithCallLimits(context.Background(), CallLimits{MaxInputBytes: 1024}), command.method, command.params)
				if err != nil {
					t.Fatal(err)
				}
				var value struct {
					NewValue jsonv2.Value `json:"newValue"`
				}
				if err := jsonv2.Unmarshal(data, &value); err != nil {
					t.Fatal(err)
				}
				if got, want := string(value.NewValue), "{"+fixture.want+"}"; got != want {
					t.Fatalf("%s argument: got %s, want %s", command.method, got, want)
				}
				if variable.NewValue != fixture.argument || returned.NewValue != fixture.argument {
					t.Fatal("encoding changed the caller's argument")
				}
			}
		})
	}
}

func TestManagedDebuggerArgumentLimitAndNil(t *testing.T) {
	for _, command := range []struct {
		method string
		params any
	}{
		{debugger.CommandSetVariableValue, debugger.SetVariableValueParams{NewValue: &runtime.CallArgument{Value: jsonv2.Value(`"too long"`)}}},
		{debugger.CommandSetReturnValue, debugger.SetReturnValueParams{NewValue: &runtime.CallArgument{Value: jsonv2.Value(`"too long"`)}}},
	} {
		if _, err := encodeCall(WithCallLimits(context.Background(), CallLimits{MaxInputBytes: 3}), command.method, command.params); !errors.Is(err, ErrCommandLimit) {
			t.Fatal("argument bytes escaped admission", err)
		}
	}
	for _, params := range []any{debugger.SetReturnValueParams{}, (*debugger.SetReturnValueParams)(nil)} {
		data, err := encodeCall(context.Background(), debugger.CommandSetReturnValue, params)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != `{"newValue":null}` && string(data) != "null" {
			t.Fatal("nil parameters changed", string(data))
		}
	}
}
