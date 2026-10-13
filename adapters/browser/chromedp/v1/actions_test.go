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

package chromedp_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/device"
	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
)

func publicAction[T any](t *testing.T, ctx context.Context, page *browser.Page, action browser.Action[T], dependencies browser.Dependencies) T {
	t.Helper()
	value, receipt, err := browser.RunAction(ctx, page, action)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil {
		t.Fatal("direct action receipt failed", err)
	}
	delivery, err := dependencies.Evidence.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	independent, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := independent.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	direct, present := snapshot.ValueCopy()
	copy, copied := evidence.ValueCopy()
	if !present || !copied || evidence.Info().Sequence != snapshot.Info().Sequence || !bytes.Equal(direct.PayloadCopy(), copy.PayloadCopy()) {
		t.Fatal("independent action evidence differs")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestLivePublicNativeHelpersAndResponseMeaning(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if r.URL.Path == "/first" {
			w.WriteHeader(http.StatusTeapot)
		}
		_, _ = w.Write([]byte(`<!doctype html><title>fixture</title><input id="field" value="initial">
        <div id="hidden" style="display:none">hidden</div><select><option id="selected" selected>one</option></select>
        <div id="box" style="width:120px;height:40px;color:rgb(255,0,0)">visible</div>
        <div class="entry">first</div><div class="entry">second</div>`))
	}))
	defer server.Close()
	prepared, err := browser.Prepare(browser.Settings{Name: "helpers", ExecPath: path, TempDir: t.TempDir(), MaxCommands: new(1024), MaxEvents: new(2048), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, browser.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := publicDependencies(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	retained, _, err := owner.Client().Retain(ctx, browser.ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close(context.Background())
	page := retained.Page()
	response := publicAction(t, ctx, page, browser.NavigateResponse(server.URL+"/first"), dependencies)
	if response == nil || response.Status != 418 {
		t.Fatal("load was confused with HTTP success")
	}
	for _, action := range []browser.Action[sdk.Void]{
		browser.WaitReady(sdk.CSS("#field")), browser.WaitVisible(sdk.CSS("#box")),
		browser.WaitNotVisible(sdk.CSS("#hidden")), browser.WaitEnabled(sdk.CSS("#field")),
		browser.WaitSelected(sdk.CSS("#selected")), browser.WaitNotPresent(sdk.CSS("#absent")),
		browser.SetValue(sdk.CSS("#field"), "changed"),
	} {
		publicAction(t, ctx, page, action, dependencies)
	}
	if value := publicAction(t, ctx, page, browser.Value(sdk.CSS("#field")), dependencies); value != "changed" {
		t.Fatal("native value helper lost its result")
	}
	publicAction(t, ctx, page, browser.Clear(sdk.CSS("#field")), dependencies)
	if value := publicAction(t, ctx, page, browser.Value(sdk.CSS("#field")), dependencies); value != "" {
		t.Fatalf("native Clear left the synthetic value %q", value)
	}
	publicAction(t, ctx, page, browser.Focus(sdk.CSS("#field")), dependencies)
	publicAction(t, ctx, page, browser.KeyEvent("typed"), dependencies)
	if value := publicAction(t, ctx, page, browser.Value(sdk.CSS("#field")), dependencies); value != "typed" {
		t.Fatal("native keyboard helper did not type")
	}
	publicAction(t, ctx, page, browser.Blur(sdk.CSS("#field")), dependencies)
	attributes := map[string]string{"data-owned": "original"}
	action := browser.SetAttributes(sdk.CSS("#box"), attributes)
	attributes["data-owned"] = "mutated"
	publicAction(t, ctx, page, action, dependencies)
	attributes["data-owned"] = "after-return"
	got := publicAction(t, ctx, page, browser.Attributes(sdk.CSS("#box")), dependencies)
	if got["data-owned"] != "mutated" {
		t.Fatal("attribute execution did not copy its borrowed input at the documented boundary")
	}
	publicAction(t, ctx, page, browser.RemoveAttribute(sdk.CSS("#box"), "data-owned"), dependencies)
	if got := publicAction(t, ctx, page, browser.Attributes(sdk.CSS("#box")), dependencies); got["data-owned"] != "" {
		t.Fatal("attribute removal failed")
	}
	if all := publicAction(t, ctx, page, browser.AttributesAll(sdk.CSSAll(".entry")), dependencies); len(all) != 2 {
		t.Fatal("native multi-selector lost results")
	}
	if box := publicAction(t, ctx, page, browser.Dimensions(sdk.CSS("#box")), dependencies); box == nil || box.Width != 120 {
		t.Fatal("native box dimensions differ")
	}
	if style := publicAction(t, ctx, page, browser.ComputedStyle(sdk.CSS("#box")), dependencies); len(style) == 0 {
		t.Fatal("computed styles absent")
	}
	if html := publicAction(t, ctx, page, browser.OuterHTML(sdk.CSS("#box")), dependencies); !strings.Contains(html, "visible") {
		t.Fatal("native DOM serialization absent")
	}
	publicAction(t, ctx, page, browser.EmulateViewport(320, 240), dependencies)
	if width := publicAction(t, ctx, page, browser.Evaluate[int]("window.innerWidth"), dependencies); width != 320 {
		t.Fatal("viewport helper did not execute")
	}
	publicAction(t, ctx, page, browser.Emulate(device.IPhone7), dependencies)
	if agent := publicAction(t, ctx, page, browser.Evaluate[string]("navigator.userAgent"), dependencies); agent != device.IPhone7.Device().UserAgent {
		t.Fatal("native device profile did not apply its user agent")
	}
	publicAction(t, ctx, page, browser.EmulateReset(), dependencies)
	if agent := publicAction(t, ctx, page, browser.Evaluate[string]("navigator.userAgent"), dependencies); agent == device.IPhone7.Device().UserAgent {
		t.Fatal("device reset left the emulated user agent")
	}
	if pdf := publicAction(t, ctx, page, browser.PrintToPDF(sdk.PDFPaper(sdk.PaperA4), sdk.PDFMargin(0), sdk.PDFLandscape()), dependencies); !bytes.HasPrefix(pdf, []byte("%PDF-")) {
		t.Fatal("native finite PDF helper did not return a PDF")
	}
	publicAction(t, ctx, page, browser.ResetViewport(), dependencies)
	if value := publicAction(t, ctx, page, browser.Evaluate[int]("Promise.resolve(7)", browser.EvalAwaitPromise()), dependencies); value != 7 {
		t.Fatal("promise result lost")
	}
	if value := publicAction(t, ctx, page, browser.PollFunction[string]("value => value", browser.WithPollingArgs("ready")), dependencies); value != "ready" {
		t.Fatal("native poll arguments differ")
	}
	if shot := publicAction(t, ctx, page, browser.CaptureScreenshot(), dependencies); !bytes.HasPrefix(shot, []byte{137, 80, 78, 71}) {
		t.Fatal("native PNG capture absent")
	}
	publicAction(t, ctx, page, browser.NavigateResponse(server.URL+"/second"), dependencies)
	if history := publicAction(t, ctx, page, browser.NavigationEntries(), dependencies); len(history.Entries) < 2 {
		t.Fatal("navigation history absent")
	}
	publicAction(t, ctx, page, browser.NavigateBack(), dependencies)
	if address := publicAction(t, ctx, page, browser.Location(), dependencies); address != server.URL+"/first" {
		t.Fatal("back navigation differs")
	}
	publicAction(t, ctx, page, browser.NavigateForward(), dependencies)
	publicAction(t, ctx, page, browser.Reload(), dependencies)
	if title := publicAction(t, ctx, page, browser.Title(), dependencies); title != "fixture" {
		t.Fatal("native title result differs")
	}
	if err := retained.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := dependencies.Evidence.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	for range status.Outstanding {
		delivery, err := dependencies.Evidence.NextReleased(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
