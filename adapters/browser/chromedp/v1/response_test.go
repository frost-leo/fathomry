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
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func responseFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/result", http.StatusFound)
		case "/result":
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte(`<a id="fragment" href="#next">fragment</a><h1>result</h1>`))
		case "/submit":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("<h1>submitted</h1>"))
		case "/child":
			_, _ = w.Write([]byte(`<script>let n=0;setInterval(()=>location.hash=++n,10)</script>`))
		default:
			_, _ = w.Write([]byte(`<!doctype html><a id="link" href="/redirect">navigate</a><form action="/submit"><input id="field" name="query"></form><button id="counter" onclick="window.effects++">count</button><script>window.effects=0</script>`))
			if r.URL.Path == "/frames" {
				_, _ = w.Write([]byte(`<iframe src="/child"></iframe>`))
			}
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestRunResponseRejectsMalformedComposition(t *testing.T) {
	for _, steps := range [][]Action[sdk.Void]{nil, make([]Action[sdk.Void], 65), {Action[sdk.Void]{}}, {Click(sdk.CSS("#link")), Action[sdk.Void]{}}} {
		if _, receipt, err := RunResponse(context.Background(), nil, steps...); !errors.Is(err, ErrInput) || receipt != nil {
			t.Fatal("invalid response composition was admitted", err)
		}
	}
}

func TestLiveRunResponseClickSubmitAndHistory(t *testing.T) {
	server := responseFixtureServer(t)
	owner, retained, dependencies := controlFixture(t, server.URL, 1024)
	page := retained.Page()
	run := func(steps ...Action[sdk.Void]) *network.Response {
		t.Helper()
		response, receipt, err := RunResponse(bounded(t), page, steps...)
		if err != nil {
			t.Fatal("response composition failed", err)
		}
		snapshot, err := receipt.WaitReleased(bounded(t))
		if err != nil || snapshot.Err() != nil {
			t.Fatal("response receipt failed", err)
		}
		drain(t, dependencies, len(steps)+1)
		return response
	}
	if response := run(Click(sdk.CSS("#link"))); response == nil || response.Status != 418 || response.URL != server.URL+"/result" {
		t.Fatal("click did not return the redirect's final non-2xx response")
	}
	if response := run(Click(sdk.CSS("#fragment"))); response != nil {
		t.Fatal("fragment navigation invented an HTTP response")
	}
	pageActionValue(t, page, dependencies, NavigateResponse(server.URL))
	if response := run(SetValue(sdk.CSS("#field"), "query"), Submit(sdk.CSS("#field"))); response == nil || response.Status != 500 || response.URL != server.URL+"/submit?query=query" {
		t.Fatal("submit response did not preserve form values or HTTP status")
	}
	if response := run(Reload()); response == nil || response.Status != 500 {
		t.Fatal("reload could not run within the response navigation lane")
	}
	if response := run(NavigateBack()); response != nil && response.Status != 200 {
		t.Fatal("back response missing")
	}
	if location := pageActionValue(t, page, dependencies, Location()); location != server.URL+"/" {
		t.Fatal("back did not restore its document", location)
	}
	if response := run(NavigateForward()); response != nil && response.Status != 500 {
		t.Fatal("forward response missing")
	}
	if location := pageActionValue(t, page, dependencies, Location()); location != server.URL+"/submit?query=query" {
		t.Fatal("forward did not restore its document", location)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("response operations retained native work")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveRunResponseIgnoresChildFragmentsAndReleasesCanceledWait(t *testing.T) {
	server := responseFixtureServer(t)
	owner, retained, dependencies := controlFixture(t, server.URL+"/frames", 256)
	page := retained.Page()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	response, receipt, err := RunResponse(ctx, page, Click(sdk.CSS("#counter")))
	if response != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("an iframe fragment ended a root navigation wait", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Err(), context.DeadlineExceeded) {
		t.Fatal("response cancellation lost independent evidence", err)
	}
	drain(t, dependencies, 2)
	if effects := pageActionValue(t, page, dependencies, Evaluate[int]("window.effects")); effects != 1 {
		t.Fatal("canceled response wait undid or repeated the click", effects)
	}
	if _, _, err := RunResponse(bounded(t), page, Click(sdk.CSS("#counter")), Action[sdk.Void]{}); !errors.Is(err, ErrInput) {
		t.Fatal("invalid later step was not rejected before the click")
	}
	if effects := pageActionValue(t, page, dependencies, Evaluate[int]("window.effects")); effects != 1 {
		t.Fatal("composition validation happened after an effect", effects)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("canceled response wait retained native work")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveRunResponseStepEvidenceRefusalPrecedesPreparation(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>response admission</p>", 64)
	options, _ := dependencies.Evidence.Options()
	initial, _ := dependencies.Evidence.Inspect()
	filled := options.Capacity - initial.Outstanding - 1
	for range filled {
		_, err := retained.endpoint.Child(bounded(t), retained.call.Scope(), request("response_fill", "", 0, retained.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
			_ = call.Resolve(adapters.Outcome[Result]{})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	prepared := false
	_, receipt, err := RunResponse(bounded(t), retained.Page(), MouseEvent(input.DispatchMouseEventTypeMouseMoved, 10, 10, func(*input.DispatchMouseEventParams) { prepared = true }))
	if !errors.Is(err, adapters.ErrEvidence) || prepared || receipt == nil {
		t.Fatal("nested step bypassed evidence admission", err, prepared)
	}
	drain(t, dependencies, filled+1)
	pageActionValue(t, retained.Page(), dependencies, Text(sdk.CSS("p")))
	_ = owner.Close(bounded(t))
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveRunResponseKeepsControlRepliesAvailable(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if r.URL.Path == "/next" {
			hits.Add(1)
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte("next"))
			return
		}
		_, _ = w.Write([]byte(`<a id="link" href="/next">next</a>`))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 128)
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Fetch: &fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: server.URL + "/next"}}}})
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		response *network.Response
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		response, _, err := RunResponse(bounded(t), retained.Page(), Click(sdk.CSS("#link")))
		done <- outcome{response, err}
	}()
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	fillControlInbox(t, retained, dependencies)
	if err := pending.Reply(bounded(t), ContinueRequest(fetch.ContinueRequestParams{})); err != nil {
		t.Fatal("response wait blocked its reserved reply", err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.response == nil || got.response.Status != 418 || hits.Load() != 1 {
			t.Fatal("response wait lost the continued navigation", got.err, hits.Load())
		}
	case <-bounded(t).Done():
		t.Fatal("response wait did not finish after its control reply")
	}
	if err := control.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("control response wait prevented shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveRunResponsePreservesStepValueCustody(t *testing.T) {
	for _, mode := range []string{"exception", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			owner, retained, dependencies := objectFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			expression := "throw new Error('response step exception')"
			if mode == "unknown" {
				expression = "new Promise(resolve => {globalThis.finishResponse = () => resolve({})})"
			}
			_, receipt, err := RunResponse(ctx, retained.Page(), Evaluate[sdk.Void](expression, EvalAwaitPromise()))
			if err == nil || receipt == nil {
				t.Fatal("response step failure disappeared")
			}
			if mode == "exception" {
				var cause *sdk.ExceptionError
				if !errors.As(err, &cause) {
					t.Fatal("response step exception lost its native error", err)
				}
				groups := 0
				for range 2 {
					delivery, err := dependencies.Evidence.NextReleased(bounded(t))
					if err != nil {
						t.Fatal(err)
					}
					step, _ := delivery.Receipt()
					snapshot, _ := step.WaitReleased(bounded(t))
					value, ok := snapshot.ValueCopy()
					if ok && value.Object().GroupReleased && errors.As(snapshot.Primary(), &cause) {
						groups++
					}
					_ = delivery.Ack()
				}
				if groups != 1 {
					t.Fatal("nested value lost its exception cleanup receipt", groups)
				}
			} else {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("unknown value step lost cancellation", err)
				}
				drain(t, dependencies, 1)
				pageActionValue(t, retained.Page(), dependencies, Evaluate[bool]("finishResponse(); true"))
				short, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
				_, err := dependencies.Evidence.NextReleased(short)
				stop()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("unknown step custody was refunded before teardown", err)
				}
			}
			_ = owner.Close(bounded(t))
			if !owner.ShutdownComplete() {
				t.Fatal("response value custody did not settle on teardown")
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}
