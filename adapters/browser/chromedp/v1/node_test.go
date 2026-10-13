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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveNodeQueriesPreserveNativeSubtreeAndSourceAuthority(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<section id='first'><input><span>first</span></section><section id='second'><span>second</span></section>", 64)
	nodes, receipt, err := QueryNodes(bounded(t), retained.Page(), sdk.CSSAll("section"), NodeReady(), AtLeast(2))
	if err != nil || len(nodes) != 2 {
		t.Fatal("native node query/readiness failed", err)
	}
	var metadata cdp.Node
	if err := json.Unmarshal(nodes[0].DataCopy(), &metadata); err != nil || metadata.NodeID == 0 || metadata.NodeName != "SECTION" {
		t.Fatal("native node metadata missing", err)
	}
	metadata.NodeID = 0
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	var stored []cdp.Node
	if err := json.Unmarshal(evidence.PayloadCopy(), &stored); err != nil || len(stored) != 2 || stored[0].NodeID == 0 {
		t.Fatal("node evidence lost its native data", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Text(sdk.CSS("span"), FromNode(nodes[0]))); err != nil || value != "first" {
		t.Fatal("native subtree selector escaped its node", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), SendKeys(sdk.CSS("input"), "managed", FromNode(nodes[0]))); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Value(sdk.CSS("input"), FromNode(nodes[0]))); err != nil || value != "managed" {
		t.Fatal("native input/readiness changed under node scope", err)
	}
	drain(t, dependencies, 1)
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), other, Click(sdk.CSS("input"), FromNode(nodes[0]))); !errors.Is(err, ErrInput) {
		t.Fatal("foreign page accepted node authority", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Text(sdk.CSS("span"), FromNode(&Node{}))); !errors.Is(err, ErrInput) {
		t.Fatal("zero node silently became root", err)
	}
	drain(t, dependencies, 1)
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<section><span>replacement</span></section>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Text(sdk.CSS("span"), FromNode(nodes[0]))); !errors.Is(err, ErrState) {
		t.Fatal("departed document node accepted", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Text(sdk.CSS("span"), FromNode(nil))); err != nil || value != "replacement" {
		t.Fatal("explicit native nil/root semantics changed", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("borrowed node metadata retained a native owner")
	}
	if _, _, err := QueryNodes(bounded(t), retained.Page(), "section"); err == nil {
		t.Fatal("closed page minted node authority")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveNodeIframeDelegationAndDocumentRevocation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte("<iframe id='child' src='/old'></iframe>"))
			return
		}
		_, _ = w.Write([]byte("<main><span>" + r.URL.Path + "</span><input></main>"))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 64)
	frames, _, err := QueryNodes(bounded(t), retained.Page(), sdk.CSS("#child"))
	if err != nil || len(frames) != 1 {
		t.Fatal("native iframe node missing", err)
	}
	drain(t, dependencies, 1)
	children, _, err := QueryNodes(bounded(t), retained.Page(), sdk.CSS("main"), FromNode(frames[0]))
	if err != nil || len(children) != 1 {
		t.Fatal("native same-process iframe traversal failed", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Text(sdk.CSS("span"), FromNode(children[0]))); err != nil || value != "/old" {
		t.Fatal("iframe execution context changed", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("document.querySelector('iframe').src='/new';true")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Poll[bool]("document.querySelector('iframe').contentDocument?.body?.textContent.includes('/new')")); err != nil || !value {
		t.Fatal("new iframe document not observed", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), SendKeys(sdk.CSS("input"), "stale", FromNode(children[0]))); !errors.Is(err, ErrState) {
		t.Fatal("old iframe node retained input authority", err)
	}
	drain(t, dependencies, 1)
	if empty, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("document.querySelector('iframe').contentDocument.querySelector('input').value === ''")); err != nil || !empty {
		t.Fatal("stale node affected replacement iframe", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("iframe node cleanup did not join")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveNativeNodeIDSelectionUsesGrantsRatherThanRawIDs(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<input id=one><input id=two><span>first</span><span>second</span>", 128)
	nodes, _, err := QueryNodes(bounded(t), retained.Page(), sdk.CSSAll("input"), AtLeast(2))
	if err != nil || len(nodes) != 2 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	input := []*Node{nodes[0]}
	action := SendKeys("not a real selector", "node-owned", ByNodes(input...))
	input[0] = nodes[1]
	if _, _, err := RunAction(bounded(t), retained.Page(), action); err != nil {
		t.Fatal("native NodeIDs input failed", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Value("", ByNodes(nodes[1]))); err != nil || value != "node-owned" {
		t.Fatal("selected wrong node or copied before admission", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Value("", ByNodes(nodes[0]))); err != nil || value != "" {
		t.Fatal("unselected node changed", err)
	}
	drain(t, dependencies, 1)
	selected, _, err := QueryNodes(bounded(t), retained.Page(), "label", ByNodes(nodes[1], nodes[0]), AtLeast(2))
	if err != nil || len(selected) != 2 {
		t.Fatal("native order or readiness missing", err)
	}
	drain(t, dependencies, 1)
	var first, second cdp.Node
	if json.Unmarshal(selected[0].DataCopy(), &first) != nil || json.Unmarshal(nodes[1].DataCopy(), &second) != nil || first.NodeID != second.NodeID {
		t.Fatal("NodeIDs order changed")
	}
	empty, _, err := QueryNodes(bounded(t), retained.Page(), "label", ByNodes(), AtLeast(0))
	if err != nil || len(empty) != 0 {
		t.Fatal("explicit empty node selection changed", err)
	}
	drain(t, dependencies, 1)
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), other, Click("", ByNodes(nodes[0]))); !errors.Is(err, ErrInput) {
		t.Fatal("foreign page adopted NodeIDs", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Click("", ByNodes(nil))); !errors.Is(err, ErrInput) {
		t.Fatal("nil node became a root grant", err)
	}
	drain(t, dependencies, 1)
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<input>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Click("", ByNodes(nodes[0]))); !errors.Is(err, ErrState) {
		t.Fatal("departed document node was reauthorized", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("node selection retained ownership")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
