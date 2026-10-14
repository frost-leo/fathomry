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
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
)

func TestLiveOwnedWindowBoundsAndRevocation(t *testing.T) {
	checkWindow(t, false)
}

func TestLiveVisibleOwnedWindowBoundsAndRevocation(t *testing.T) {
	if os.Getenv("FATHOMRY_CHROMEDP_VISIBLE") != "1" || os.Getenv("DISPLAY") == "" {
		t.Skip("visible fixture not explicitly selected")
	}
	checkWindow(t, true)
}

func checkWindow(t *testing.T, visible bool) {
	t.Helper()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	settings := Settings{Name: "owned-window", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), MaxCommands: new(30), Flags: map[string]string{"no-first-run": "", "disable-background-networking": ""}}
	if visible {
		settings.Environment = []string{"DISPLAY=" + os.Getenv("DISPLAY")}
		if value := os.Getenv("XDG_RUNTIME_DIR"); value != "" {
			settings.Environment = append(settings.Environment, "XDG_RUNTIME_DIR="+value)
		}
		settings.Flags["ozone-platform"] = "x11"
	} else {
		settings.Flags["headless"] = ""
	}
	prepared, err := Prepare(settings, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	window, _, err := owner.Window(bounded(t), retained.Page())
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	var original browser.GetWindowForTargetResult
	if json.Unmarshal(window.DataCopy(), &original) != nil || original.Bounds == nil {
		t.Fatal("native window metadata missing")
	}
	params := browser.SetWindowBoundsParams{Bounds: &browser.Bounds{Left: new(int64(0)), Top: new(int64(0)), Width: 640, Height: 480}}
	if _, _, err := RunWindowCommand(bounded(t), window, browser.SetWindowBounds, params); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if params.WindowID != 0 || *params.Bounds.Left != 0 || *params.Bounds.Top != 0 {
		t.Fatal("window identity fill changed caller parameters or zero values")
	}
	result, receipt, err := RunWindowCommand(bounded(t), window, browser.GetWindowBounds, browser.GetWindowBoundsParams{})
	if err != nil || result.Bounds == nil || result.Bounds.Width != 640 || result.Bounds.Height != 480 {
		t.Fatal("native window bounds were not applied", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	result.Bounds.Width = 777
	var frozen browser.GetWindowBoundsResult
	if json.Unmarshal(evidence.PayloadCopy(), &frozen) != nil || frozen.Bounds == nil || frozen.Bounds.Width != 640 {
		t.Fatal("window result aliases its independent evidence")
	}
	drain(t, dependencies, 1)
	if _, _, err := RunWindowCommand(bounded(t), window, browser.SetContentsSize, browser.SetContentsSizeParams{Width: 500, Height: 300}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunWindowCommand(bounded(t), window, browser.GetWindowBounds, browser.GetWindowBoundsParams{WindowID: original.WindowID + 1000}); !errors.Is(err, ErrInput) {
		t.Fatal("foreign window ID acquired authority", err)
	}
	drain(t, dependencies, 1)
	forged := cdp.Command[browser.GetWindowBoundsParams, cdp.Empty]{Method: browser.GetWindowBounds.Method}
	if _, _, err := RunWindowCommand(bounded(t), window, forged, browser.GetWindowBoundsParams{}); !errors.Is(err, ErrInput) {
		t.Fatal("forged window schema accepted", err)
	}
	drain(t, dependencies, 1)
	if err := retained.Page().Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunWindowCommand(bounded(t), window, browser.GetWindowBounds, browser.GetWindowBoundsParams{}); !errors.Is(err, ErrState) {
		t.Fatal("window grant outlived its target", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("window operations prevented cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveWindowRequiresMatchingProcessOwnerAndSharedAllowance(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "about:blank", 3)
	window, _, err := owner.Window(bounded(t), retained.Page())
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunWindowCommand(bounded(t), window, browser.GetWindowBounds, browser.GetWindowBoundsParams{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	copy := *window
	if _, _, err := RunWindowCommand(bounded(t), &copy, browser.GetWindowBounds, browser.GetWindowBoundsParams{}); !errors.Is(err, ErrLimit) {
		t.Fatal("window alias reset the two-command owner allowance", err)
	}
	drain(t, dependencies, 1)
	other, foreign, otherDependencies := controlFixture(t, "about:blank", 2)
	if _, _, err := other.Window(bounded(t), retained.Page()); !errors.Is(err, ErrInput) {
		t.Fatal("another source acquired this target's window", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := owner.Window(bounded(t), foreign.Page()); !errors.Is(err, ErrInput) {
		t.Fatal("owner adopted another source generation", err)
	}
	drain(t, otherDependencies, 1)
	_ = other.Close(bounded(t))
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !other.ShutdownComplete() {
		t.Fatal("window refusal prevented shutdown")
	}
	for _, dependencies := range []Dependencies{dependencies, otherDependencies} {
		status, _ := dependencies.Evidence.Inspect()
		drain(t, dependencies, status.Outstanding)
	}
}

func TestLiveBorrowedBrowserCannotGrantWindowMutation(t *testing.T) {
	owner, retained, _, dependencies := borrowedControlFixture(t)
	if _, _, err := owner.Window(bounded(t), retained.Page()); !errors.Is(err, ErrState) {
		t.Fatal("connection ownership became window ownership", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("refused window ownership prevented peer detach")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
