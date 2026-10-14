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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func controlFixture(t *testing.T, address string, commands int) (*Owner, *Context, Dependencies) {
	t.Helper()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	p, err := Prepare(Settings{Name: "controls", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), MaxCommands: new(commands), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, p)
	owner, err := p.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), address); err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	return owner, retained, d
}

func fillControlInbox(t *testing.T, retained *Context, dependencies Dependencies) {
	t.Helper()
	options, err := dependencies.Evidence.Options()
	if err != nil {
		t.Fatal(err)
	}
	for range options.Capacity {
		_, err := retained.endpoint.Child(bounded(t), retained.call.Scope(), request("control_fill", "", 0, retained.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
			_ = call.Resolve(adapters.Outcome[Result]{})
		})
		if err != nil {
			if !errors.Is(err, adapters.ErrEvidence) {
				t.Fatal("unexpected refusal", err)
			}
			status, err := dependencies.Evidence.Inspect()
			if err != nil || status.Outstanding != options.Capacity {
				t.Fatal("Inbox was not genuinely full", err)
			}
			return
		}
	}
	t.Fatal("failed to reach evidence saturation")
}

func TestLiveControlRepliesProgressWhileFetchEvaluationAndEvidenceAreBlocked(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/value" {
			requests.Add(1)
			_, _ = w.Write([]byte("continued"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<h1>control fixture</h1>"))
	}))
	defer server.Close()
	owner, retained, d := controlFixture(t, server.URL, 2)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Fetch: &fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: server.URL + "/value"}}}})
	if err != nil {
		t.Fatal(err)
	}
	type completed struct {
		value string
		err   error
	}
	result := make(chan completed, 1)
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`fetch('/value').then(r=>r.text())`, EvalAwaitPromise()))
		result <- completed{value, err}
	}()
	pending, err := control.Next(bounded(t))
	if err != nil || pending.Method() != fetch.RequestPaused.Method {
		t.Fatal("request did not pause", err)
	}
	fillControlInbox(t, retained, d)
	params := fetch.ContinueRequestParams{}
	if err := pending.Reply(bounded(t), ContinueRequest(params)); err != nil {
		t.Fatal("pre-admitted reply was blocked", err)
	}
	if params.RequestID != "" {
		t.Fatal("reply mutated caller identity")
	}
	select {
	case got := <-result:
		if got.err != nil || got.value != "continued" {
			t.Fatal("parent evaluation did not progress", got.err)
		}
	case <-bounded(t).Done():
		t.Fatal("reply deadlocked behind its parent")
	}
	if requests.Load() != 1 {
		t.Fatal("original effect repeated or omitted")
	}
	if err := pending.Reply(bounded(t), ContinueRequest(params)); !errors.Is(err, ErrState) {
		t.Fatal("reply capability reused", err)
	}
	if err := control.Close(bounded(t)); err != nil || !control.Closed() {
		t.Fatal("cleanup required fresh evidence", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	value, ok := snapshot.ValueCopy()
	if !ok || !bytes.Contains(value.PayloadCopy(), []byte(`"acknowledged":true`)) {
		t.Fatal("ignored reply lost independent evidence")
	}
	status, _ := d.Evidence.Inspect()
	drain(t, d, status.Outstanding-2)
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int]("1")); !errors.Is(err, ErrLimit) {
		t.Fatal("ordinary command quota was not exhausted", err)
	}
	drain(t, d, 1)
	_ = retained.Close(bounded(t))
	if err := owner.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, d, 2)
}

func TestLiveControlDialogReplyWhileEvaluationWaits(t *testing.T) {
	_, retained, d := controlFixture(t, "data:text/html,<h1>dialog fixture</h1>", 2)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Dialogs: true})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`prompt("private-dialog", "default")`))
		if err == nil && value != "answered" {
			err = errors.New("wrong dialog answer")
		}
		result <- err
	}()
	pending, err := control.Next(bounded(t))
	if err != nil || pending.Method() != page.JavascriptDialogOpening.Method {
		t.Fatal("dialog did not open", err)
	}
	fillControlInbox(t, retained, d)
	if err := pending.Reply(bounded(t), HandleDialog(page.HandleJavaScriptDialogParams{Accept: true, PromptText: "answered"})); err != nil {
		t.Fatal("dialog reply could not bypass ordinary work", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("dialog parent deadlocked")
	}
	if err := control.Close(bounded(t)); err != nil {
		t.Fatal("dialog cleanup absence not observed", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Err() != nil {
		t.Fatal(err, snapshot.Err())
	}
	status, _ := d.Evidence.Inspect()
	drain(t, d, status.Outstanding-2)
}

func TestLiveControlAuthenticationAndFulfillment(t *testing.T) {
	var authenticated atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<h1>auth fixture</h1>"))
			return
		}
		user, password, ok := r.BasicAuth()
		if !ok || user != "synthetic" || password != "credential-canary" {
			w.Header().Set("WWW-Authenticate", `Basic realm="synthetic"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		authenticated.Store(true)
		_, _ = w.Write([]byte("authenticated"))
	}))
	defer server.Close()
	_, retained, d := controlFixture(t, server.URL, 3)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Fetch: &fetch.EnableParams{HandleAuthRequests: new(true), Patterns: []*fetch.RequestPattern{{URLPattern: server.URL + "/auth"}}}})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`fetch('/auth').then(r=>r.text())`, EvalAwaitPromise()))
		if err == nil && value != "authenticated" {
			err = errors.New("auth result mismatch")
		}
		result <- err
	}()
	request, err := control.Next(bounded(t))
	if err != nil || request.Method() != fetch.RequestPaused.Method {
		t.Fatal(err)
	}
	if err := request.Reply(bounded(t), ContinueRequest(fetch.ContinueRequestParams{})); err != nil {
		t.Fatal(err)
	}
	challenge, err := control.Next(bounded(t))
	if err != nil || challenge.Method() != fetch.AuthRequired.Method {
		t.Fatal("challenge not observed", err)
	}
	if err := challenge.Reply(bounded(t), ContinueWithAuth(fetch.ContinueWithAuthParams{AuthChallengeResponse: &fetch.AuthChallengeResponse{Response: fetch.AuthChallengeResponseResponseProvideCredentials, Username: "synthetic", Password: "credential-canary"}})); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("authentication parent blocked")
	}
	if !authenticated.Load() {
		t.Fatal("authentication effect absent")
	}
	drain(t, d, 1)
	if err := control.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || bytes.Contains([]byte(fmt.Sprint(snapshot)), []byte("credential-canary")) {
		t.Fatal("safe receipt presentation failed", err)
	}
	drain(t, d, 1)

	control, _, err = retained.Page().Control(retained.Context(), ControlOptions{Fetch: &fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: server.URL + "/fulfilled"}}}})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`fetch('/fulfilled').then(r=>r.text())`, EvalAwaitPromise()))
		if err == nil && value != "synthetic-body" {
			err = errors.New("fulfillment mismatch")
		}
		result <- err
	}()
	request, err = control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := request.Reply(bounded(t), FulfillRequest(fetch.FulfillRequestParams{ResponseCode: 200, Body: []byte("synthetic-body")})); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	if err := control.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
}

func TestControlForeignIdentityIsNotSentAndCallerMayCorrectIt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("fixture")) }))
	defer server.Close()
	_, retained, d := controlFixture(t, server.URL, 2)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Fetch: &fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: server.URL + "/blocked"}}}})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool](`fetch('/blocked').then(()=>false,()=>true)`, EvalAwaitPromise()))
		result <- err
	}()
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := pending.Reply(bounded(t), ContinueRequest(fetch.ContinueRequestParams{RequestID: "foreign"})); !errors.Is(err, ErrInput) {
		t.Fatal("foreign request accepted", err)
	}
	if err := pending.Reply(bounded(t), HandleDialog(page.HandleJavaScriptDialogParams{Accept: true})); !errors.Is(err, ErrInput) {
		t.Fatal("wrong reply kind accepted", err)
	}
	if err := pending.Reply(bounded(t), FulfillRequest(fetch.FulfillRequestParams{ResponseCode: 200, Body: []byte(strings.Repeat("x", 2<<20))})); !errors.Is(err, ErrLimit) {
		t.Fatal("oversized reply bypassed preflight", err)
	}
	if err := pending.Reply(bounded(t), FailRequest(fetch.FailRequestParams{ErrorReason: network.ErrorReasonAborted})); err != nil {
		t.Fatal("preflight refusal consumed original token", err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	if err := control.Close(bounded(t)); !errors.Is(err, ErrInput) || !control.Closed() {
		t.Fatal("refusal evidence disappeared", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Err(), ErrInput) {
		t.Fatal(err)
	}
	value, _ := snapshot.ValueCopy()
	var records []struct {
		Refused      int  `json:"refused"`
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.Unmarshal(value.PayloadCopy(), &records); err != nil || len(records) != 1 || records[0].Refused != 3 || !records[0].Acknowledged {
		t.Fatal("control outcomes conflated", err)
	}
	drain(t, d, 1)
}
