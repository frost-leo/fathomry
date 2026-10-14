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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdk "github.com/chromedp/chromedp"
)

func pageActionValue[T any](t *testing.T, page *Page, dependencies Dependencies, action Action[T]) T {
	t.Helper()
	value, receipt, err := RunAction(bounded(t), page, action)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Err(); err != nil {
		t.Fatal("independent action failed", err)
	}
	drain(t, dependencies, 1)
	return value
}

func TestLiveRenderedPageAutomationOwnedAndBorrowed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/data":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"text": "rendered:" + r.URL.Query().Get("q")})
		case "/frame":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<input id="nested"><button id="inner" onclick="document.querySelector('#inside').textContent=document.querySelector('#nested').value">apply</button><span id="inside">initial</span>`))
		default:
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<!doctype html><title>synthetic rendered fixture</title><input id="query"><button id="search">fetch</button><div id="result" hidden></div><iframe id="frame" src="/frame"></iframe><div style="height:1600px"></div><div id="bottom">bottom</div><script>document.querySelector('#search').onclick=async()=>{const r=await fetch('/data?q='+encodeURIComponent(document.querySelector('#query').value));const data=await r.json();const e=document.querySelector('#result');e.textContent=data.text;e.hidden=false;};</script>`))
		}
	}))
	defer server.Close()
	for _, mode := range []string{"owned", "borrowed"} {
		t.Run(mode, func(t *testing.T) {
			var owner *Owner
			var retained *Context
			var dependencies Dependencies
			var peer context.Context
			if mode == "owned" {
				owner, retained, dependencies = controlFixture(t, server.URL, 1024)
			} else {
				owner, retained, peer, dependencies = borrowedControlFixture(t)
				pageActionValue(t, retained.Page(), dependencies, NavigateResponse(server.URL))
			}
			page := retained.Page()
			pageActionValue(t, page, dependencies, WaitReady(sdk.CSS("#query")))
			input := "local 雪 <&>"
			pageActionValue(t, page, dependencies, SendKeys(sdk.CSS("#query"), input))
			if value := pageActionValue(t, page, dependencies, Value(sdk.CSS("#query"))); value != input {
				t.Fatalf("typed input mismatch: %q", value)
			}
			pageActionValue(t, page, dependencies, Click(sdk.CSS("#search")))
			pageActionValue(t, page, dependencies, WaitVisible(sdk.CSS("#result")))
			if value := pageActionValue(t, page, dependencies, Text(sdk.CSS("#result"))); value != "rendered:"+input {
				t.Fatalf("rendered text mismatch: %q", value)
			}
			if html := pageActionValue(t, page, dependencies, OuterHTML(sdk.CSS("#result"))); !strings.Contains(html, "rendered:") || !strings.Contains(html, "&lt;&amp;&gt;") {
				t.Fatalf("rendered HTML mismatch: %q", html)
			}
			pageActionValue(t, page, dependencies, ScrollIntoView(sdk.CSS("#bottom")))
			if !pageActionValue(t, page, dependencies, Evaluate[bool](`(()=>{const r=document.querySelector('#bottom').getBoundingClientRect();return r.top>=0&&r.bottom<=innerHeight})()`)) {
				t.Fatal("scroll did not expose target")
			}
			frames, _, err := QueryNodes(bounded(t), page, sdk.CSS("#frame"))
			if err != nil || len(frames) != 1 {
				t.Fatal("iframe node missing", err)
			}
			drain(t, dependencies, 1)
			pageActionValue(t, page, dependencies, SendKeys(sdk.CSS("#nested"), "inside", FromNode(frames[0])))
			pageActionValue(t, page, dependencies, Click(sdk.CSS("#inner"), FromNode(frames[0])))
			if text := pageActionValue(t, page, dependencies, Text(sdk.CSS("#inside"), FromNode(frames[0]))); text != "inside" {
				t.Fatal("iframe result mismatch", text)
			}
			short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			_, receipt, err := RunAction(short, page, WaitVisible(sdk.CSS("#never-present")))
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || receipt == nil {
				t.Fatal("missing element did not honor cancellation", err)
			}
			snapshot, err := receipt.WaitReleased(bounded(t))
			if err != nil || !errors.Is(snapshot.Primary(), context.DeadlineExceeded) {
				t.Fatal("cancellation evidence missing", err)
			}
			drain(t, dependencies, 1)
			if value := pageActionValue(t, page, dependencies, Text(sdk.CSS("#result"))); value != "rendered:"+input {
				t.Fatal("canceled wait poisoned later page work")
			}
			_, receipt, err = RunAction(bounded(t), page, Evaluate[int](`throw new Error('synthetic-evaluation-failure')`))
			if err == nil || receipt == nil {
				t.Fatal("JS failure was lost")
			}
			snapshot, err = receipt.WaitReleased(bounded(t))
			if err != nil || snapshot.Primary() == nil {
				t.Fatal("JS failure evidence missing", err)
			}
			drain(t, dependencies, 1)
			if value := pageActionValue(t, page, dependencies, Evaluate[int]("6*7")); value != 42 {
				t.Fatal("JS failure poisoned later execution")
			}
			_ = owner.Close(bounded(t))
			if !owner.ShutdownComplete() {
				t.Fatal("page automation or failure cleanup retained source")
			}
			if mode == "borrowed" {
				peerWork, stopPeerWork := context.WithTimeout(peer, 10*time.Second)
				defer stopPeerWork()
				value, err := sdk.Run(peerWork, sdk.Evaluate[string](`document.querySelector('#query').value`))
				if err != nil || value != input {
					t.Fatal("borrowed page or state was not preserved", err)
				}
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}
