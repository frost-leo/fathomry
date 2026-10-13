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
	"github.com/chromedp/cdproto/serviceworker"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

func workerServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/worker.js" {
			w.Header().Set("Content-Type", "text/javascript")
			_, _ = w.Write([]byte(`self.addEventListener('install',()=>self.skipWaiting());self.addEventListener('activate',e=>e.waitUntil(clients.claim()));self.addEventListener('sync',e=>e.waitUntil(clients.matchAll({includeUncontrolled:true}).then(all=>all.forEach(c=>c.postMessage(e.tag)))));self.addEventListener('periodicsync',e=>e.waitUntil(clients.matchAll({includeUncontrolled:true}).then(all=>all.forEach(c=>c.postMessage(e.tag)))));`))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<script>window.workerMessage='';navigator.serviceWorker.onmessage=e=>window.workerMessage=e.data;</script>`))
	}))
	t.Cleanup(server.Close)
	return server
}

func waitWorkerVersion(t *testing.T, control *Control, address string, status serviceworker.VersionRunningStatus) ServiceWorker {
	t.Helper()
	for range 96 {
		pending, err := control.Next(bounded(t))
		if err != nil {
			causes := []error{control.state.native.Err(), err}
			for len(causes) != 0 {
				cause := causes[0]
				causes = causes[1:]
				if diagnostic, ok := cause.(*fault.Error); ok {
					t.Logf("synthetic worker diagnostic: %+v", diagnostic.Diagnostic())
				}
				if joined, ok := cause.(interface{ Unwrap() []error }); ok {
					causes = append(causes, joined.Unwrap()...)
				} else if wrapped, ok := cause.(interface{ Unwrap() error }); ok {
					causes = append(causes, wrapped.Unwrap())
				}
			}
			t.Fatal("native worker observation failed", err)
		}
		if pending.Method() != serviceworker.WorkerVersionUpdated.Method {
			continue
		}
		var event serviceworker.EventWorkerVersionUpdated
		if err := json.Unmarshal(pending.DataCopy(), &event); err != nil {
			t.Fatal(err)
		}
		for index, version := range event.Versions {
			if version != nil && version.ScriptURL == address+"/worker.js" && version.RunningStatus == status && version.Status == serviceworker.VersionStatusActivated {
				worker, err := pending.ServiceWorker(index)
				if err == nil {
					return worker
				}
			}
		}
	}
	t.Fatal("native worker state absent")
	return ServiceWorker{}
}

func TestLiveServiceWorkerObservedGrantsAndContextLifecycle(t *testing.T) {
	server := workerServer(t)
	owner, retained, dependencies := controlFixture(t, server.URL, 40)
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{ServiceWorkers: true})
	if err != nil {
		t.Fatal(err)
	}
	if value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool](`(async()=>{await navigator.serviceWorker.register('/worker.js');await navigator.serviceWorker.ready;return true})()`, EvalAwaitPromise())); err != nil || !value {
		t.Fatal("isolated native registration failed", err)
	}
	drain(t, dependencies, 1)
	worker := waitWorkerVersion(t, control, server.URL, serviceworker.VersionRunningStatusRunning)
	params := serviceworker.DispatchSyncEventParams{Tag: "synthetic-sync", LastChance: true}
	if _, _, err := RunServiceWorkerCommand(bounded(t), worker, serviceworker.DispatchSyncEvent, params); err != nil || params.RegistrationID != "" || params.Origin != "" {
		t.Fatal("worker identity fill or sync dispatch failed", err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Poll[bool](`window.workerMessage==='synthetic-sync'`)); err != nil || !value {
		t.Fatal("native sync event did not reach the registered handler", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunServiceWorkerCommand(bounded(t), worker, serviceworker.StopWorker, serviceworker.StopWorkerParams{VersionID: "foreign"}); !errors.Is(err, ErrInput) {
		t.Fatal("foreign worker version ID accepted", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunServiceWorkerCommand(bounded(t), worker, serviceworker.StopWorker, serviceworker.StopWorkerParams{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	_ = waitWorkerVersion(t, control, server.URL, serviceworker.VersionRunningStatusStopped)
	if _, _, err := RunServiceWorkerContextCommand(bounded(t), control, serviceworker.StartWorker, serviceworker.StartWorkerParams{ScopeURL: server.URL + "/"}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	worker = waitWorkerVersion(t, control, server.URL, serviceworker.VersionRunningStatusRunning)
	if _, _, err := RunServiceWorkerContextCommand(bounded(t), control, serviceworker.SetForceUpdateOnPageLoad, serviceworker.SetForceUpdateOnPageLoadParams{ForceUpdateOnPageLoad: false}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunServiceWorkerContextCommand(bounded(t), control, serviceworker.Unregister, serviceworker.UnregisterParams{ScopeURL: server.URL + "/"}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), PollFunction[bool](`async()=>!(await navigator.serviceWorker.getRegistration('/'))`)); err != nil || !value {
		t.Fatal("native unregister acknowledgement never completed", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunServiceWorkerCommand(bounded(t), worker, serviceworker.DispatchSyncEvent, params); !errors.Is(err, ErrState) {
		t.Fatal("deleted worker grant retained authority", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("worker observation did not close")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveServiceWorkerBorrowedScopeNeedsGrantAndPreservesRegistration(t *testing.T) {
	server := workerServer(t)
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := sdk.Do(peer, sdk.Navigate(server.URL)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := retained.Page().Control(retained.Context(), ControlOptions{ServiceWorkers: true}); !errors.Is(err, ErrState) {
		t.Fatal("borrowed page silently granted context-wide worker authority", err)
	}
	drain(t, dependencies, 1)
	_ = retained.Close(bounded(t))
	drain(t, dependencies, 1)
	granted, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(sdk.FromContext(peer).Target.TargetID), SharedStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := granted.Page().Control(granted.Context(), ControlOptions{ServiceWorkers: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sdk.Run(peer, sdk.Evaluate[bool](`(async()=>{await navigator.serviceWorker.register('/worker.js');await navigator.serviceWorker.ready;return true})()`, sdk.EvalAwaitPromise)); err != nil {
		t.Fatal(err)
	}
	_ = waitWorkerVersion(t, control, server.URL, serviceworker.VersionRunningStatusRunning)
	if _, _, err := RunServiceWorkerContextCommand(bounded(t), control, serviceworker.StopAllWorkers, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("borrowed worker control failed to detach", err)
	}
	if count, err := sdk.Run(peer, sdk.Evaluate[int](`navigator.serviceWorker.getRegistrations().then(all=>all.length)`, sdk.EvalAwaitPromise)); err != nil || count != 1 {
		t.Fatal("control close unregistered or destroyed peer workers", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveServiceWorkerContextMutationCannotAffectSiblingContext(t *testing.T) {
	server := workerServer(t)
	owner, first, second, _, dependencies := storageFixture(t)
	controls := make([]*Control, 2)
	for index, retained := range []*Context{first, second} {
		if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
			t.Fatal(err)
		}
		drain(t, dependencies, 1)
		control, _, err := retained.Page().Control(retained.Context(), ControlOptions{ServiceWorkers: true})
		if err != nil {
			t.Fatal(err)
		}
		controls[index] = control
		if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool](`(async()=>{await navigator.serviceWorker.register('/worker.js');await navigator.serviceWorker.ready;return true})()`, EvalAwaitPromise())); err != nil {
			t.Fatal(err)
		}
		drain(t, dependencies, 1)
		_ = waitWorkerVersion(t, control, server.URL, serviceworker.VersionRunningStatusRunning)
	}
	if _, _, err := RunServiceWorkerContextCommand(bounded(t), controls[0], serviceworker.Unregister, serviceworker.UnregisterParams{ScopeURL: server.URL + "/"}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), first.Page(), PollFunction[bool](`async()=>!(await navigator.serviceWorker.getRegistration('/'))`)); err != nil || !value {
		t.Fatal("selected context's unregister did not complete", err)
	}
	drain(t, dependencies, 1)
	if count, _, err := RunAction(bounded(t), second.Page(), Evaluate[int](`navigator.serviceWorker.getRegistrations().then(all=>all.length)`, EvalAwaitPromise())); err != nil || count != 1 {
		t.Fatal("native scope URL escaped its original BrowserContext", err)
	}
	drain(t, dependencies, 1)
	fillControlInbox(t, first, dependencies)
	if err := controls[1].Close(bounded(t)); err != nil || !controls[1].Closed() {
		t.Fatal("saturated evidence prevented observer cleanup", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("context worker cleanup incomplete")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
