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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
)

func completedRequest(t *testing.T, control *Control, suffix string) Request {
	t.Helper()
	var identity network.RequestID
	for range 64 {
		pending, err := control.Next(bounded(t))
		if err != nil {
			t.Fatal("request observation failed", err)
		}
		switch pending.Method() {
		case network.RequestWillBeSent.Method:
			var event network.EventRequestWillBeSent
			if err := json.Unmarshal(pending.DataCopy(), &event); err != nil {
				t.Fatal(err)
			}
			if event.Request != nil && strings.HasSuffix(event.Request.URL, suffix) {
				identity = event.RequestID
			}
		case network.LoadingFinished.Method:
			var event network.EventLoadingFinished
			if err := json.Unmarshal(pending.DataCopy(), &event); err != nil {
				t.Fatal(err)
			}
			if identity != "" && event.RequestID == identity {
				grant, err := pending.Request()
				if err != nil {
					t.Fatal("completed observed request had no grant", err)
				}
				return grant
			}
		}
	}
	t.Fatal("request completion was not observed")
	return Request{}
}

func TestLiveRequestCommandsPreserveBodiesIdentityAndIndependentEvidence(t *testing.T) {
	body := []byte{0, 255, 127, 'b', 'o', 'd', 'y'}
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/body" {
			data, err := io.ReadAll(r.Body)
			if err != nil || string(data) != "synthetic post" {
				t.Error("native post body changed", err)
			}
			posts.Add(1)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write(body)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<title>network fixture</title>"))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 20)
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Network: true})
	if err != nil {
		t.Fatal(err)
	}
	status, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`fetch('/body',{method:'POST',body:'synthetic post'}).then(async r=>{await r.arrayBuffer();return r.status})`, EvalAwaitPromise()))
	if err != nil || status != http.StatusTeapot {
		t.Fatal("HTTP failure response was not preserved", err)
	}
	drain(t, dependencies, 1)
	request := completedRequest(t, control, "/body")
	params := network.GetResponseBodyParams{}
	value, receipt, err := RunRequestCommand(bounded(t), request, network.GetResponseBody, params)
	if err != nil || !bytes.Equal(value.Body, body) || params.RequestID != "" {
		t.Fatal("native body or parameter copy differs", err)
	}
	value.Body[0] = 99
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	var recorded struct {
		Body []byte `json:"body"`
	}
	if err := json.Unmarshal(evidence.PayloadCopy(), &recorded); err != nil || !bytes.Equal(recorded.Body, body) {
		t.Fatal("request output mutated independent evidence", err)
	}
	drain(t, dependencies, 1)
	post, _, err := RunRequestCommand(bounded(t), request, network.GetRequestPostData, network.GetRequestPostDataParams{})
	if err != nil || string(post.PostData) != "synthetic post" {
		t.Fatal("native request body differs", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunRequestCommand(bounded(t), request, network.GetResponseBody, network.GetResponseBodyParams{RequestID: "foreign"}); !errors.Is(err, ErrInput) {
		t.Fatal("bare foreign identity gained authority", err)
	}
	drain(t, dependencies, 1)
	forged := cdp.Command[network.GetResponseBodyParams, cdp.Empty]{Method: network.GetResponseBody.Method}
	if _, _, err := RunRequestCommand(bounded(t), request, forged, network.GetResponseBodyParams{}); !errors.Is(err, ErrInput) {
		t.Fatal("forged result schema accepted", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunRequestCommand(bounded(t), request, network.ReplayXHR, network.ReplayXHRParams{}); !errors.Is(err, ErrInput) {
		t.Fatal("Fetch observation was treated as XHR", err)
	}
	drain(t, dependencies, 1)
	if posts.Load() != 1 {
		t.Fatal("read or rejecting controls replayed an HTTP effect")
	}
	if err := control.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunRequestCommand(bounded(t), request, network.GetResponseBody, network.GetResponseBodyParams{}); !errors.Is(err, ErrState) {
		t.Fatal("closed control grant revived", err)
	}
	drain(t, dependencies, 1)
	if title, _, err := RunAction(bounded(t), retained.Page(), Title()); err != nil || title != "network fixture" {
		t.Fatal("observation close damaged native page behavior", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("request observation prevented shutdown")
	}
	remaining, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, remaining.Outstanding)
}

func TestLiveRequestSearchAndExplicitXHRReplay(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xhr" {
			requests.Add(1)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("needle body\nsecond line"))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 10)
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Network: true})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = RunAction(bounded(t), retained.Page(), Evaluate[bool](`new Promise((resolve,reject)=>{let r=new XMLHttpRequest();r.open('GET','/xhr');r.onload=()=>resolve(true);r.onerror=reject;r.send()})`, EvalAwaitPromise()))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	request := completedRequest(t, control, "/xhr")
	result, _, err := RunRequestCommand(bounded(t), request, network.SearchInResponseBody, network.SearchInResponseBodyParams{Query: "NEEDLE", CaseSensitive: new(false), IsRegex: new(false)})
	if err != nil || len(result.Result) != 1 || result.Result[0].LineContent != "needle body" {
		t.Fatal("native body search options lost", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunRequestCommand(bounded(t), request, network.ReplayXHR, network.ReplayXHRParams{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	_ = completedRequest(t, control, "/xhr")
	if requests.Load() != 2 {
		t.Fatal("explicit replay did not send exactly one extra request")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("replay observation prevented cleanup")
	}
	remaining, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, remaining.Outstanding)
}
