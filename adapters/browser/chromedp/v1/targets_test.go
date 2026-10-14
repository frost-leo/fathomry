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
	"os"
	"strings"
	"testing"

	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
)

func targetOfKind(t *testing.T, values []*TargetRef, kind string) *TargetRef {
	t.Helper()
	for _, value := range values {
		var info target.Info
		if err := json.Unmarshal(value.DataCopy(), &info); err != nil {
			t.Fatal(err)
		}
		if info.Type == kind {
			return value
		}
	}
	t.Fatalf("expected synthetic %s target was not observed", kind)
	return nil
}

func TestLiveOOPIFAttachmentPreservesNativeFrameOperations(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	var childURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if r.URL.Path == "/child" {
			_, _ = w.Write([]byte("<p>cross-site frame</p><input id='inside'>"))
			return
		}
		_, _ = w.Write([]byte("<iframe src='" + childURL + "'></iframe>"))
	}))
	defer server.Close()
	childURL = strings.Replace(server.URL, "127.0.0.1", "localhost", 1) + "/child"
	prepared, err := Prepare(Settings{Name: "oopif", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), MaxPages: new(2), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": "", "site-per-process": ""}}, NativeOptions{})
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
	if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	values, _, err := retained.Targets(bounded(t), target.GetTargetsParams{Filter: target.Filter{{Type: "iframe"}}})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	frame := targetOfKind(t, values, "iframe")
	attached, _, err := retained.Attach(bounded(t), frame)
	if err != nil {
		t.Fatal("native OOPIF attachment failed", err)
	}
	drain(t, dependencies, 1)
	if text, _, err := RunAction(bounded(t), attached, Text(sdk.CSS("p"))); err != nil || text != "cross-site frame" {
		t.Fatal("OOPIF DOM operation lost native semantics", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), attached, SendKeys(sdk.CSS("#inside"), "frame input")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), attached, Value(sdk.CSS("#inside"))); err != nil || value != "frame input" {
		t.Fatal("OOPIF input went to the wrong target", err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("ignored frame attachment did not cleanly close", err)
	}
	snapshot, err := retained.call.Receipt().WaitReleased(bounded(t))
	if err != nil || snapshot.Cleanup() != nil {
		t.Fatal("frame cleanup error remained in independent context evidence", err, snapshot.Cleanup())
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveScopedTargetDiscoveryAndPopupAttachment(t *testing.T) {
	owner, first, second, origin, dependencies := storageFixture(t)
	if _, _, err := RunAction(bounded(t), first.Page(), Evaluate[bool]("document.body.innerHTML='<button id=popup>open</button>';document.querySelector('button').onclick=()=>window.open('/popup');true")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	initial, _, err := first.Targets(bounded(t), target.GetTargetsParams{})
	if err != nil || len(initial) != 1 {
		t.Fatal("target discovery leaked another context", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := second.Attach(bounded(t), initial[0]); !errors.Is(err, ErrInput) {
		t.Fatal("foreign context adopted target provenance", err)
	}
	drain(t, dependencies, 1)
	duplicate, _, err := first.Attach(bounded(t), initial[0])
	if err != nil || duplicate.native != first.Page().native {
		t.Fatal("repeat attachment multiplied native state", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), first.Page(), Click(sdk.CSS("#popup"))); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	discovered, _, err := first.Targets(bounded(t), target.GetTargetsParams{})
	if err != nil || len(discovered) != 2 {
		t.Fatal("popup discovery missing or crossed context", err)
	}
	drain(t, dependencies, 1)
	var popup *TargetRef
	for _, value := range discovered {
		var info target.Info
		if err := json.Unmarshal(value.DataCopy(), &info); err != nil {
			t.Fatal(err)
		}
		if info.OpenerID != "" {
			popup = value
		}
	}
	if popup == nil {
		t.Fatal("native popup attribution was discarded")
	}
	page, _, err := first.Attach(bounded(t), popup)
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if location, _, err := RunAction(bounded(t), page, Location()); err != nil || location != origin+"/popup" {
		t.Fatal("attached wrong popup", err)
	}
	drain(t, dependencies, 1)
	alias := *first
	repeated, _, err := alias.Attach(bounded(t), popup)
	if err != nil || repeated.native != page.native {
		t.Fatal("context alias reset attachment quota", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("popup attachment prevented cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedWorkerAttachmentDetachesAndKeepsPeerUsable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/worker.js" {
			w.Header().Set("Content-Type", "application/javascript")
			_, _ = w.Write([]byte("self.answer=42;self.onmessage=()=>postMessage(self.answer);postMessage('ready');"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<h1>worker fixture</h1>"))
	}))
	defer server.Close()
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := sdk.Do(peer, sdk.Navigate(server.URL), sdk.Evaluate[sdk.Void]("globalThis.worker=new Worker('/worker.js');worker.onmessage=e=>globalThis.workerMessage=e.data")); err != nil {
		t.Fatal(err)
	}
	if _, err := sdk.Run(peer, sdk.Poll[bool]("globalThis.workerMessage==='ready'")); err != nil {
		t.Fatal(err)
	}
	values, _, err := retained.Targets(bounded(t), target.GetTargetsParams{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	worker := targetOfKind(t, values, "worker")
	attached, _, err := retained.Attach(bounded(t), worker)
	if err != nil {
		t.Fatal("native worker attachment failed", err)
	}
	drain(t, dependencies, 1)
	if answer, _, err := RunAction(bounded(t), attached, Evaluate[int]("self.answer")); err != nil || answer != 42 {
		t.Fatal("worker runtime operation did not use the worker target", err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("borrowed worker cleanup failed", err)
	}
	if err := sdk.Do(peer, sdk.Evaluate[sdk.Void]("workerMessage=null;worker.postMessage('after-detach')")); err != nil {
		t.Fatal(err)
	}
	if ok, err := sdk.Run(peer, sdk.Poll[bool]("workerMessage===42")); err != nil || !ok {
		t.Fatal("borrowed worker or its parent was killed", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
