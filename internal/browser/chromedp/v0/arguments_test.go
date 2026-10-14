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
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/device"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestBrowserTypedFunctionArgumentsPreserveEmptyValues(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	id := fault.Correlation{Call: "empty-arguments"}
	receipt, err := fixture.client.Run(deadline(t), deadline(t), id, func(session *Session) error {
		object, err := RunObjectAction(session.Context(), session, nil, Evaluate[*runtime.RemoteObject]("window"))
		if err != nil {
			return err
		}
		defer object.Group().Close(context.Background())
		for _, raw := range []string{`""`, `[]`, `{}`, `null`, `false`, `0`} {
			argument := jsonv2.Value(raw)
			result, err := object.Call(session.Context(), "function(value) { return JSON.stringify(value); }", argument)
			if err != nil {
				return err
			}
			var value string
			if err := jsonv2.Unmarshal(result.NativeValue().Value, &value); err != nil {
				return err
			}
			if value != raw || string(argument) != raw {
				return errors.New("typed argument changed or became undefined")
			}
		}
		return object.Group().Close(session.Context())
	})
	if err != nil {
		t.Fatal(err)
	}
	settle(t, fixture, id, receipt, nil, nil, func(testing.TB, Result) {})
}

func TestPDFOptionsRequireAdmissionAndManagedStream(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxCommands = 1
	session.data.commands = 1
	called := false
	if _, err := RunAction(context.Background(), session, PrintToPDF(func(*page.PrintToPDFParams) { called = true })); !errors.Is(err, ErrLimit) || called {
		t.Fatal("exhausted command allowance still invoked PDF options", err)
	}
	for name, option := range map[string]sdk.PDFOption{"nil": nil, "stream": sdk.PDFStream()} {
		t.Run(name, func(t *testing.T) {
			session := unitSession(t)
			if _, err := RunAction(context.Background(), session, PrintToPDF(option)); !errors.Is(err, ErrInput) {
				t.Fatal("invalid or unmanaged PDF output mode reached native execution", err)
			}
		})
	}
}

type observedDevice struct{ called *bool }

func (value observedDevice) Device() device.Info {
	*value.called = true
	return device.Info{}
}

func TestDevicePreparationRequiresAdmissionAndRejectsTypedNil(t *testing.T) {
	var missing *device.Info
	if _, err := RunAction(context.Background(), unitSession(t), Emulate(missing)); !errors.Is(err, ErrInput) {
		t.Fatal("typed nil device reached native setup", err)
	}
	called := false
	action := Emulate(observedDevice{called: &called})
	if called {
		t.Fatal("device preparation ran at construction")
	}
	session := unitSession(t)
	session.closed = true
	if _, err := RunAction(context.Background(), session, action); !errors.Is(err, ErrState) || called {
		t.Fatal("device callback ran before session admission", err)
	}
}

func TestBrowserCallerPreparationRequiresCommandAdmission(t *testing.T) {
	options := browserOptions(t)
	options.MaxCommands = 1
	fixture := bindFixture(t, options, 1)
	id := fault.Correlation{Call: "preparation-admission"}
	retained, err := fixture.client.Retain(deadline(t), id)
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close(context.Background())
	if _, err := RunAction(deadline(t), retained.Page(), Title()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"key", "mouse", "viewport", "device"} {
		t.Run(name, func(t *testing.T) {
			called := false
			var action Action[sdk.Void]
			switch name {
			case "key":
				action = KeyEvent("a", func(*input.DispatchKeyEventParams) { called = true })
			case "mouse":
				action = MouseEvent(input.DispatchMouseEventTypeMouseMoved, 0, 0, func(*input.DispatchMouseEventParams) { called = true })
			case "viewport":
				action = EmulateViewport(320, 240, func(*emulation.SetDeviceMetricsOverrideParams, *emulation.SetTouchEmulationEnabledParams) {
					called = true
				})
			case "device":
				action = Emulate(observedDevice{called: &called})
			}
			if _, err := RunAction(deadline(t), retained.Page(), action); called || !errors.Is(err, ErrLimit) {
				t.Fatal("caller preparation ran without command admission", called, err)
			}
		})
	}
	_ = retained.Close(deadline(t))
	settle(t, fixture, id, retained.Receipt(), ErrLimit, nil, func(testing.TB, Result) {})
}

func TestAllNativeOptionBridgesBoundConstructorCopying(t *testing.T) {
	for _, action := range []Operation{
		Evaluate[string]("value", make([]EvaluateOption, 1<<16)...),
		KeyEvent("value", make([]sdk.KeyOption, 1<<16)...),
		MouseEvent(input.DispatchMouseEventTypeMouseMoved, 0, 0, make([]sdk.MouseOption, 1<<16)...),
		EmulateViewport(320, 240, make([]sdk.EmulateViewportOption, 1<<16)...),
	} {
		session := unitSession(t)
		if err := action.apply(context.Background(), session); !errors.Is(err, ErrInput) || session.objectOrigin != nil || session.data.commands != 0 {
			t.Fatal("oversized options reached native setup", err)
		}
	}
}

func TestQueryOptionCountIsBoundedBeforeConstructorCopy(t *testing.T) {
	options := make([]QueryOption, 1<<16)
	options[0] = NodeReady()
	if copied := cloneQueryOptions(options); len(copied) != 1 || copied[0].native != nil {
		t.Fatal("oversized invalid query options were retained or copied")
	}
	for _, operation := range []Operation{Text("p", options...), WaitVisible("p", options...), SetUploadFiles("#file", []string{}, options...)} {
		if err := operation.apply(context.Background(), unitSession(t)); !errors.Is(err, ErrInput) {
			t.Fatal("invalid option count reached native setup", err)
		}
	}
	valid := []QueryOption{NodeReady()}
	copied := cloneQueryOptions(valid)
	valid[0] = QueryOption{}
	if copied[0].native == nil {
		t.Fatal("valid option snapshots were not preserved")
	}
}

func TestExistingOptionLimitsRejectBeforeSetup(t *testing.T) {
	for _, operation := range []Operation{
		Poll[bool]("true", make([]PollOption, 1<<16)...),
		PollFunction[bool]("()=>true", make([]PollOption, 1<<16)...),
		PrintToPDF(make([]sdk.PDFOption, 1<<16)...),
	} {
		session := unitSession(t)
		if err := operation.apply(context.Background(), session); !errors.Is(err, ErrInput) || session.objectOrigin != nil || session.data.commands != 0 {
			t.Fatal("oversized options reached native setup", err)
		}
	}
	session := unitSession(t)
	session.owner.settings.MaxCommandBytes = 8
	if _, err := RunAction(context.Background(), session, SetAttributes("p", map[string]string{"too-large": "input"})); !errors.Is(err, ErrLimit) || session.data.commands != 0 {
		t.Fatal("oversized attributes reached the native query", err)
	}
}

func TestBrowserBorrowedConstructorDataIsPreparedDuringExecution(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	id := fault.Correlation{Call: "borrowed-constructor-data"}
	retained, err := fixture.client.Retain(deadline(t), id)
	if err != nil {
		t.Fatal(err)
	}
	if err := retained.Page().Navigate(deadline(t), "data:text/html,<p id='value'>fixture</p>"); err != nil {
		t.Fatal(err)
	}
	attributes := map[string]string{"data-value": "construction"}
	action := SetAttributes("#value", attributes)
	attributes["data-value"] = "execution"
	if _, err := RunAction(deadline(t), retained.Page(), action); err != nil {
		t.Fatal(err)
	}
	attributes["data-value"] = "caller-after-return"
	value, err := RunAction(deadline(t), retained.Page(), Attributes("#value"))
	if err != nil || value["data-value"] != "execution" {
		t.Fatal("attribute borrowing/copy boundary changed", err)
	}
	arguments := []any{1}
	poll := PollFunction[int]("value=>value", WithPollingArgs(arguments...))
	arguments[0] = 42
	if value, err := RunAction(deadline(t), retained.Page(), poll); err != nil || value != 42 {
		t.Fatal("poll argument container did not retain native borrowing semantics", err)
	}
	if err := retained.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, fixture, id, retained.Receipt(), nil, nil, func(testing.TB, Result) {})
}
