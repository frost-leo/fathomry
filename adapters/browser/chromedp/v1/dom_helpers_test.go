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
	"context"
	"errors"
	"image/png"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/input"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveNativePopulationDumpInputAndScaledCapture(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, `data:text/html,<style>body{margin:0}.box{position:absolute;width:20px;height:20px;top:0;background:red}.box+.box{left:40px;background:blue}</style><section><div class=box id=first>first</div><div class=box id=second>second</div></section><input style="position:absolute;top:40px">`, 64)
	page := retained.Page()
	nodes, _, err := QueryNodes(bounded(t), page, sdk.CSSAll(".box"), Populate(1, true, PopulateWait(20*time.Millisecond)), AtLeast(2))
	if err != nil || len(nodes) != 2 {
		t.Fatal("native population failed", err)
	}
	drain(t, dependencies, 1)
	data, _, err := RunAction(bounded(t), page, ScreenshotNodes(nodes, 2))
	if err != nil {
		t.Fatal(err)
	}
	config, err := png.DecodeConfig(bytes.NewReader(data))
	if err != nil || config.Width != 120 || config.Height != 40 {
		t.Fatal("native multi-node extents or scale changed", config, err)
	}
	drain(t, dependencies, 1)
	data, _, err = RunAction(bounded(t), page, ScreenshotScale(sdk.CSS("#first"), 2))
	if err != nil {
		t.Fatal(err)
	}
	config, err = png.DecodeConfig(bytes.NewReader(data))
	if err != nil || config.Width != 40 || config.Height != 40 {
		t.Fatal("native selector scale changed", config, err)
	}
	drain(t, dependencies, 1)
	tree, receipt, err := RunAction(bounded(t), page, DumpTree(sdk.CSS("section"), ">", "--", false, -1, true, 30*time.Millisecond))
	if err != nil || !strings.Contains(tree, ">section") || !strings.Contains(tree, "div#first") || !strings.Contains(tree, "div#second") || !strings.Contains(tree, "--") {
		t.Fatal("native tree format or children missing", err)
	}
	if snapshot, err := receipt.WaitReleased(bounded(t)); err != nil || snapshot.Err() != nil {
		t.Fatal("tree receipt not independently available", err)
	}
	drain(t, dependencies, 1)
	inputs, _, err := QueryNodes(bounded(t), page, sdk.CSS("input"))
	if err != nil || len(inputs) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), page, MouseClickNode(inputs[0], sdk.ClickCount(1))); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), page, KeyEventNode(inputs[0], "x", sdk.KeyModifiers(sdk.ModifierShift))); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	value, _, err := RunAction(bounded(t), page, Value(sdk.CSS("input")))
	if err != nil || value != "x" {
		t.Fatal("node-specific key options changed native character semantics", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), page, MouseClickXY(10, 10)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("ordinary helpers retained an owner")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveNodeHelpersRejectForeignStaleAndInvalidNodes(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<input>", 32)
	nodes, _, err := QueryNodes(bounded(t), retained.Page(), sdk.CSS("input"))
	if err != nil || len(nodes) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	called := false
	if _, _, err := RunAction(bounded(t), other, KeyEventNode(nodes[0], "x", func(*input.DispatchKeyEventParams) { called = true })); !errors.Is(err, ErrInput) || called {
		t.Fatal("foreign node reached native option preparation", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), ScreenshotNodes([]*Node{{}}, 1)); !errors.Is(err, ErrInput) {
		t.Fatal("zero node accepted", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), ScreenshotNodes(nil, 1)); !errors.Is(err, ErrInput) {
		t.Fatal("empty nodes reached a native panic", err)
	}
	drain(t, dependencies, 1)
	if _, err := retained.Page().Navigate(bounded(t), "about:blank"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), MouseClickNode(nodes[0])); !errors.Is(err, ErrState) {
		t.Fatal("stale node accepted", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLivePopulateWaitCancellationJoinsNativeWork(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<section><span>child</span></section>", 16)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err := QueryNodes(ctx, retained.Page(), sdk.CSS("section"), Populate(-1, true, PopulateWait(time.Hour)))
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 5*time.Second {
		t.Fatal("population did not join after cancellation", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("canceled population retained native work")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
