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
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/domsnapshot"
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/performance"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestQueryOptionConstructionRejectsOversizeBeforeCopy(t *testing.T) {
	options := make([]QueryOption, 1<<16)
	if values := nativeQuery(options); len(values) != 1 {
		t.Fatal("invalid query-option slice was copied before admission")
	}
}

func TestLiveTypedPageCommandsKeepNativeDataAndPageScope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body><input id='entry'><p>typed-snapshot-canary</p></body></html>"))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 64)
	first := retained.Page()
	second, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Navigate(bounded(t), server.URL); err != nil {
		t.Fatal(err)
	}
	secondBefore, _, err := RunAction(bounded(t), second, Evaluate[bool]("matchMedia('print').matches"))
	if err != nil {
		t.Fatal(err)
	}
	params := emulation.SetEmulatedMediaParams{Media: "print", Features: []*emulation.MediaFeature{{Name: "prefers-color-scheme", Value: "dark"}}}
	if _, _, err := RunAction(bounded(t), first, Command(emulation.SetEmulatedMedia, params)); err != nil {
		t.Fatal(err)
	}
	params.Features[0].Value = "light"
	matches, _, err := RunAction(bounded(t), first, Evaluate[bool]("matchMedia('print').matches && matchMedia('(prefers-color-scheme: dark)').matches"))
	if err != nil || !matches {
		t.Fatal("typed native emulation was not applied or retained caller input", err)
	}
	secondAfter, _, err := RunAction(bounded(t), second, Evaluate[bool]("matchMedia('print').matches"))
	if err != nil || secondAfter != secondBefore {
		t.Fatal("page media override changed its peer", err)
	}
	if _, _, err := RunAction(bounded(t), first, Focus("#entry")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), first, Command(input.InsertText, input.InsertTextParams{Text: "native IME text"})); err != nil {
		t.Fatal(err)
	}
	value, _, err := RunAction(bounded(t), first, Value("#entry"))
	if err != nil || value != "native IME text" {
		t.Fatal("typed input lost native behavior", err)
	}
	snapshot, receipt, err := RunAction(bounded(t), first, Command(page.CaptureSnapshot, page.CaptureSnapshotParams{}))
	if err != nil || !strings.Contains(snapshot.Data, "typed-snapshot-canary") {
		t.Fatal("MHTML capture did not retain the synthetic page", err)
	}
	snapshot.Data = "changed-return-value"
	evidence, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	record, _ := evidence.ValueCopy()
	if !bytes.Contains(record.PayloadCopy(), []byte("typed-snapshot-canary")) || bytes.Contains(record.PayloadCopy(), []byte(snapshot.Data)) {
		t.Fatal("native capture evidence was not independent")
	}
	tree, _, err := RunAction(bounded(t), first, Command(domsnapshot.CaptureSnapshot, domsnapshot.CaptureSnapshotParams{ComputedStyles: []string{"display"}, IncludeDOMRects: new(true)}))
	if err != nil || len(tree.Documents) == 0 || len(tree.Strings) == 0 {
		t.Fatal("native DOM snapshot failed", err)
	}
	png, _, err := RunAction(bounded(t), first, Command(page.CaptureScreenshot, page.CaptureScreenshotParams{CaptureBeyondViewport: new(false)}))
	if err != nil || !bytes.HasPrefix(png.Data, []byte("\x89PNG\r\n\x1a\n")) {
		t.Fatal("typed finite screenshot failed", err)
	}
	layout, _, err := RunAction(bounded(t), first, Command(page.GetLayoutMetrics, cdp.Empty{}))
	if err != nil || layout.CSSContentSize == nil || layout.CSSContentSize.Width <= 0 {
		t.Fatal("native layout metrics failed", err)
	}
	if _, _, err := RunAction(bounded(t), first, Command(emulation.SetEmulatedMedia, emulation.SetEmulatedMediaParams{})); err != nil {
		t.Fatal("native reset failed", err)
	}
	if _, _, err := RunAction(bounded(t), first, Command(performance.GetMetrics, cdp.Empty{})); err != nil {
		t.Fatal("finite metrics query failed", err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("finite commands retained cleanup obligations", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveTypedPageCommandsRefuseOwnerAndBareIdentityWithEvidence(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>authority-control</p>", 32)
	targetPage := retained.Page()
	_, receipt, err := RunAction(bounded(t), targetPage, Command(browser.Close, cdp.Empty{}))
	if !errors.Is(err, ErrUnsupported) || receipt == nil {
		t.Fatal("page obtained browser-owner authority", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Primary(), ErrUnsupported) {
		t.Fatal("owner refusal lost its independent evidence", err)
	}
	_, _, err = RunAction(bounded(t), targetPage, Command(dom.DescribeNode, dom.DescribeNodeParams{BackendNodeID: 1}))
	if !errors.Is(err, ErrUnsupported) {
		t.Fatal("a bare node ID became authorization", err)
	}
	if _, _, err := RunAction(bounded(t), targetPage, Command(emulation.SetDisplayFeaturesOverride, emulation.SetDisplayFeaturesOverrideParams{Features: []*emulation.DisplayFeature{}})); !errors.Is(err, ErrInput) {
		t.Fatal("empty native display features reached the browser", err)
	}
	value, _, err := RunAction(bounded(t), targetPage, Text("p"))
	if err != nil || value != "authority-control" {
		t.Fatal("refused owner command reached the browser", err)
	}
	_ = retained.Close(bounded(t))
	if _, _, err := RunAction(bounded(t), targetPage, Command(page.GetLayoutMetrics, cdp.Empty{})); !errors.Is(err, adapters.ErrClosed) {
		t.Fatal("closed page retained command authority", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("refused commands prevented real cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
