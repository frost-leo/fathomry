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
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func responseBodyServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/body" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write(body)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<h1>response fixture</h1>"))
	}))
	t.Cleanup(server.Close)
	return server
}

func responseOptions(address string) ControlOptions {
	return ControlOptions{Fetch: &fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: address + "/body", RequestStage: fetch.RequestStageResponse}}}}
}

func TestLiveCanceledBodyOnBorrowedOwnedPageCannotPreventSourceShutdown(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/body" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<title>owned process, borrowed page</title>"))
			return
		}
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("unfinished"))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	prepared, err := Prepare(Settings{Name: "owned-borrowed-body", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		owner.state.stopNative(context.Canceled)
		ctx := bounded(t)
		for !owner.ShutdownComplete() && ctx.Err() == nil {
			_ = owner.Close(ctx)
			time.Sleep(time.Millisecond)
		}
		if !owner.ShutdownComplete() {
			t.Error("owned fixture cleanup incomplete")
		}
	})
	targets, _, err := RunOwnerCommand(bounded(t), owner, target.GetTargets, target.GetTargetsParams{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	var initial target.ID
	for _, info := range targets.TargetInfos {
		if info.Type == "page" && info.URL == "about:blank" {
			initial = info.TargetID
			break
		}
	}
	if initial == "" {
		t.Fatal("default page missing")
	}
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(initial)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	control, receipt, err := retained.Page().Control(retained.Context(), responseOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool](`fetch('/body').catch(()=>null);true`)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	work, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := pending.ResponseBody(work, 4096); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("response did not remain pending", err)
	}
	shutdown, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	for !owner.ShutdownComplete() && shutdown.Err() == nil {
		_ = owner.Close(shutdown)
		time.Sleep(time.Millisecond)
	}
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("borrowed page custody prevented termination of its owned source")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Cleanup() == nil {
		t.Fatal("settled cleanup discarded its uncertain acquisition history", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveResponseBodyKeepsNativeBytesAndEvidenceAtSaturation(t *testing.T) {
	body := []byte("response-body <&> \x00 snow 雪")
	server := responseBodyServer(t, body)
	owner, retained, dependencies := controlFixture(t, server.URL, 2)
	control, receipt, err := retained.Page().Control(retained.Context(), responseOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`fetch('/body').then(r=>r.text())`, EvalAwaitPromise()))
		if err == nil && value != string(body) {
			err = errors.New("original response differs")
		}
		done <- err
	}()
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	fillControlInbox(t, retained, dependencies)
	value, err := pending.ResponseBody(bounded(t), 2048)
	if err != nil || !bytes.Equal(value.Body, body) {
		t.Fatal("body required ordinary admission or lost native bytes", err)
	}
	value.Body[0] = 'X'
	if _, err := pending.ResponseBody(bounded(t), 2048); !errors.Is(err, ErrState) {
		t.Fatal("copied pending token repeated body acquisition", err)
	}
	if err := pending.Reply(bounded(t), ContinueResponse(fetch.ContinueResponseParams{})); err != nil {
		t.Fatal("body read consumed continuation grant", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("response parent did not finish")
	}
	if err := control.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	var records []struct {
		Body *struct {
			Body         []byte
			Acknowledged bool
		} `json:"responseBody"`
	}
	if err := json.Unmarshal(evidence.PayloadCopy(), &records); err != nil || len(records) != 1 || records[0].Body == nil || !records[0].Body.Acknowledged || !bytes.Equal(records[0].Body.Body, body) {
		t.Fatal("independent body evidence changed with caller output", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("body work prevented saturated shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveResponseBodyStreamConsumesThenFulfills(t *testing.T) {
	body := bytes.Repeat([]byte{0, 255, 1, 127, 'x'}, 700)
	server := responseBodyServer(t, body)
	owner, retained, dependencies := controlFixture(t, server.URL, 3)
	control, _, err := retained.Page().Control(retained.Context(), responseOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`fetch('/body').then(r=>r.text())`, EvalAwaitPromise()))
		if err == nil && value != "replacement" {
			err = errors.New("replacement response differs")
		}
		done <- err
	}()
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	stream, streamReceipt, err := pending.ResponseBodyStream(bounded(t))
	if err != nil || stream == nil {
		t.Fatal("native response stream unavailable", err)
	}
	if _, err := pending.ResponseBody(bounded(t), 1024); !errors.Is(err, ErrState) {
		t.Fatal("stream and finite acquisition were not exclusive", err)
	}
	if err := pending.Reply(bounded(t), FulfillRequest(fetch.FulfillRequestParams{ResponseCode: 200, Body: []byte("premature")})); !errors.Is(err, ErrState) {
		t.Fatal("unread body permitted request mutation", err)
	}
	var received []byte
	complete := false
	for range 64 {
		chunk, _, err := stream.Read(bounded(t), 257)
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, chunk.DataCopy()...)
		drain(t, dependencies, 1)
		if chunk.EOF() {
			complete = true
			break
		}
	}
	if !complete || !bytes.Equal(received, body) {
		t.Fatal("sequential binary response or positive EOF differs")
	}
	if err := pending.Reply(bounded(t), ContinueRequest(fetch.ContinueRequestParams{})); !errors.Is(err, ErrInput) {
		t.Fatal("taken body allowed original continuation", err)
	}
	if err := pending.Reply(bounded(t), FulfillRequest(fetch.FulfillRequestParams{RequestID: "foreign", ResponseCode: 200})); !errors.Is(err, ErrInput) {
		t.Fatal("foreign request ID was accepted", err)
	}
	fillControlInbox(t, retained, dependencies)
	if err := pending.Reply(bounded(t), FulfillRequest(fetch.FulfillRequestParams{ResponseCode: 200, Body: []byte("replacement")})); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("stream parent did not finish")
	}
	if err := stream.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := streamReceipt.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	_ = control.Close(bounded(t))
	if !control.Closed() {
		t.Fatal("negative controls prevented cleanup")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("stream prevented source cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedResponseStreamIgnoredCleanupKeepsPeer(t *testing.T) {
	server := responseBodyServer(t, bytes.Repeat([]byte("ignored body"), 100))
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := sdk.Do(peer, sdk.Navigate(server.URL)); err != nil {
		t.Fatal(err)
	}
	control, _, err := retained.Page().Control(retained.Context(), responseOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sdk.Run(peer, sdk.Evaluate[bool](`window.requestDone=fetch('/body').then(()=>false,()=>true); true`)); err != nil {
		t.Fatal(err)
	}
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	stream, _, err := pending.ResponseBodyStream(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Close(bounded(t)); err != nil || !control.Closed() {
		t.Fatal("ignored stream did not close and abort", err)
	}
	if err := stream.Close(bounded(t)); err != nil || !stream.Closed() {
		t.Fatal("public stream could not release after control cleanup", err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[bool]("window.requestDone", sdk.EvalAwaitPromise)); err != nil || !value {
		t.Fatal("ignored stream did not abort its own request", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("borrowed response cleanup remained incomplete")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("17")); err != nil || value != 17 {
		t.Fatal("borrowed browser was not preserved", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveResponseStreamCleanupHistoryDoesNotPreventCompletion(t *testing.T) {
	server := responseBodyServer(t, []byte("ignored cleanup body"))
	endpoint, peer := cleanupFaultPeer(t, map[string]int64{"IO.close": 1})
	prepared, err := Prepare(Settings{Name: "body-cleanup", RemoteURL: endpoint, MaxSessions: new(1)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(sdk.FromContext(peer).Target.TargetID)})
	if err != nil {
		t.Fatal(err)
	}
	if err := sdk.Do(peer, sdk.Navigate(server.URL)); err != nil {
		t.Fatal(err)
	}
	control, controlReceipt, err := retained.Page().Control(retained.Context(), responseOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sdk.Run(peer, sdk.Evaluate[bool](`window.requestDone=fetch('/body').then(()=>false,()=>true); true`)); err != nil {
		t.Fatal(err)
	}
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	stream, streamReceipt, err := pending.ResponseBodyStream(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	first := stream.Close(bounded(t))
	var cause *cdproto.Error
	if !errors.As(first, &cause) || cause.Code != -32001 || stream.Closed() {
		t.Fatal("first stream cleanup was not rejected", first)
	}
	if err := control.Close(bounded(t)); !errors.Is(err, cause) || !control.Closed() {
		t.Fatal("cleanup history prevented confirmed control closure", err)
	}
	if err := stream.Close(bounded(t)); !errors.Is(err, cause) || !stream.Closed() {
		t.Fatal("public stream lost retry history", err)
	}
	for _, receipt := range []*adapters.Receipt[Result]{controlReceipt, streamReceipt} {
		snapshot, err := receipt.WaitReleased(bounded(t))
		if err != nil || !errors.Is(snapshot.Cleanup(), cause) {
			t.Fatal("independent cleanup history absent", err)
		}
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("historical failure stranded borrowed ownership")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[bool]("window.requestDone", sdk.EvalAwaitPromise)); err != nil || !value {
		t.Fatal("peer did not survive exact request cleanup", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveResponseBodyLimitLeavesContinuationAvailable(t *testing.T) {
	server := responseBodyServer(t, bytes.Repeat([]byte("large body"), 300))
	owner, retained, dependencies := controlFixture(t, server.URL, 3)
	control, _, err := retained.Page().Control(retained.Context(), responseOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool](`window.requestDone=fetch('/body').then(r=>r.text()); true`)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.ResponseBody(bounded(t), 64); !errors.Is(err, ErrLimit) {
		t.Fatal("encoded result bound was ignored", err)
	}
	if err := pending.Reply(bounded(t), ContinueResponse(fetch.ContinueResponseParams{})); err != nil {
		t.Fatal("positive response arrival was confused with an unknown producer", err)
	}
	_ = control.Close(bounded(t))
	if !control.Closed() {
		t.Fatal("oversized completed result prevented cleanup")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("oversized response stranded source")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveCanceledResponseBodyKeepsCustodyUntilTargetDestruction(t *testing.T) {
	release, exited := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/body" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<title>canceled response</title>"))
			return
		}
		defer close(exited)
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("unfinished"))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	owner, retained, dependencies := controlFixture(t, server.URL, 3)
	control, receipt, err := retained.Page().Control(retained.Context(), responseOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool](`window.requestDone=fetch('/body').catch(()=>null);true`)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	pending, err := control.Next(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	work, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := pending.ResponseBody(work, 4096); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unfinished native body did not observe caller cancellation", err)
	}
	if err := control.Close(bounded(t)); err == nil || control.Closed() {
		t.Fatal("local call cancellation falsely settled native body production", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("owned resource destruction did not settle body custody")
	}
	select {
	case <-exited:
	case <-bounded(t).Done():
		t.Fatal("native HTTP producer remained active after owned shutdown")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Primary() == nil || snapshot.Cleanup() == nil {
		t.Fatal("independent cancellation or cleanup history disappeared", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
