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
	"html"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/input"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveCommonInputCollectionOutcomes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if r.URL.Path == "/submitted" {
			_, _ = w.Write([]byte(`<p id="submitted">` + html.EscapeString(r.URL.Query().Get("query")) + `</p>`))
			return
		}
		_, _ = w.Write([]byte(`<!doctype html><style>body{margin:0;height:1900px}#hover{position:absolute;left:10px;top:10px;width:90px;height:30px;background:gray}#menu{display:none}#hover:hover #menu{display:block}#double{position:absolute;left:150px;top:10px}#item{position:absolute;left:20px;top:100px;width:80px;height:40px;background:blue}#drop{position:absolute;left:300px;top:100px;width:120px;height:80px;background:green}form{position:absolute;top:240px}#poll{position:absolute;top:290px}</style><div id="hover">hover<span id="menu">opened</span></div><button id="double" ondblclick="window.doubles++">double</button><div id="item" draggable="true">drag</div><div id="drop">drop</div><form action="/submitted"><input id="field" name="query" value="initial"></form><div id="poll">waiting</div><script>window.doubles=0;item.ondragstart=e=>e.dataTransfer.setData('text/plain','payload');drop.ondragover=e=>e.preventDefault();drop.ondrop=e=>{e.preventDefault();window.dropped=e.dataTransfer.getData('text/plain');drop.appendChild(item);};</script>`))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 1024)
	page := retained.Page()
	pageActionValue(t, page, dependencies, EmulateViewport(640, 480))
	pageActionValue(t, page, dependencies, MouseEvent(input.DispatchMouseEventTypeMouseMoved, 30, 20))
	pageActionValue(t, page, dependencies, WaitVisible(sdk.CSS("#menu")))
	if text := pageActionValue(t, page, dependencies, Text(sdk.CSS("#menu"))); text != "opened" {
		t.Fatal("hover did not reveal content", text)
	}
	pageActionValue(t, page, dependencies, DoubleClick(sdk.CSS("#double")))
	if count := pageActionValue(t, page, dependencies, Evaluate[int]("window.doubles")); count != 1 {
		t.Fatal("double click did not dispatch its browser event", count)
	}
	pageActionValue(t, page, dependencies, DragAndDrop(sdk.CSS("#item"), sdk.CSS("#drop")))
	if !pageActionValue(t, page, dependencies, Evaluate[bool](`window.dropped==='payload' && item.parentNode.id==='drop'`)) {
		t.Fatal("HTML5 drag did not transfer data and move the element")
	}
	pageActionValue(t, page, dependencies, Evaluate[bool](`setTimeout(()=>document.querySelector('#poll').textContent='mutated',80);true`))
	if value := pageActionValue(t, page, dependencies, Poll[string](`document.querySelector('#poll').textContent==='mutated' && document.querySelector('#poll').textContent`, WithPollingMutation(), WithPollingTimeout(time.Second))); value != "mutated" {
		t.Fatal("mutation polling did not return rendered content", value)
	}
	viewport := pageActionValue(t, page, dependencies, CaptureScreenshot())
	full := pageActionValue(t, page, dependencies, FullScreenshot(100))
	viewportConfig, err := png.DecodeConfig(bytes.NewReader(viewport))
	if err != nil {
		t.Fatal(err)
	}
	fullConfig, err := png.DecodeConfig(bytes.NewReader(full))
	if err != nil || viewportConfig.Height != 480 || fullConfig.Height < 1900 || fullConfig.Width <= 0 || fullConfig.Width > viewportConfig.Width {
		t.Fatal("full screenshot did not cover offscreen content", viewportConfig, fullConfig, err)
	}
	pageActionValue(t, page, dependencies, SetValue(sdk.CSS("#field"), "changed"))
	pageActionValue(t, page, dependencies, Reset(sdk.CSS("#field")))
	if value := pageActionValue(t, page, dependencies, Value(sdk.CSS("#field"))); value != "initial" {
		t.Fatal("form reset did not restore its default", value)
	}
	pageActionValue(t, page, dependencies, SetValue(sdk.CSS("#field"), "local form value"))
	pageActionValue(t, page, dependencies, Submit(sdk.CSS("#field")))
	pageActionValue(t, page, dependencies, WaitVisible(sdk.CSS("#submitted")))
	if value := pageActionValue(t, page, dependencies, Text(sdk.CSS("#submitted"))); value != "local form value" {
		t.Fatal("form submission did not reach the synthetic server", value)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("ordinary collection cleanup incomplete", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveSendKeysWhilePageReplacesAttributes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!doctype html><input id="field"><script>let tick=0;window.churn=setInterval(()=>{if (++tick%2) field.setAttribute('data-tick','local');else field.removeAttribute('data-tick');},0);</script>`))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 4096)
	for range 200 {
		pageActionValue(t, retained.Page(), dependencies, SendKeys(sdk.CSS("#field"), "x"))
	}
	if value := pageActionValue(t, retained.Page(), dependencies, Value(sdk.CSS("#field"))); value != strings.Repeat("x", 200) {
		t.Fatal("typing did not reach the input", len(value))
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("typing cleanup incomplete", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLivePublicContextProxyRoutingIsIsolated(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	var directHits, proxyHits atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/page" {
			directHits.Add(1)
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<p id="route">direct</p>`))
	}))
	defer origin.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/page" {
			proxyHits.Add(1)
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<p id="route">proxy</p>`))
	}))
	defer proxy.Close()
	prepared, err := Prepare(Settings{Name: "context-proxy", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(2), MaxCommands: new(256), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	proxied, _, err := owner.Client().Retain(bounded(t), ContextOptions{ProxyServer: proxy.URL, ProxyBypassList: "<-loopback>"})
	if err != nil {
		t.Fatal(err)
	}
	direct, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pageActionValue(t, proxied.Page(), dependencies, NavigateResponse(origin.URL+"/page"))
	pageActionValue(t, direct.Page(), dependencies, NavigateResponse(origin.URL+"/page"))
	if value := pageActionValue(t, proxied.Page(), dependencies, Text(sdk.CSS("#route"))); value != "proxy" {
		t.Fatal("proxied context bypassed local proxy", value)
	}
	if value := pageActionValue(t, direct.Page(), dependencies, Text(sdk.CSS("#route"))); value != "direct" || directHits.Load() != 1 || proxyHits.Load() != 1 {
		t.Fatal("proxy setting crossed context ownership", value, directHits.Load(), proxyHits.Load())
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("proxy contexts did not close", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
