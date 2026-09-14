/**
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

package doris

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
)

const goodLoad = `{"Label":"gh42-run-1","TxnId":42,"Status":"Success","TwoPhaseCommit":"false","NumberTotalRows":1,"NumberLoadedRows":1,"NumberFilteredRows":0,"NumberUnselectedRows":0}`

func testBatch() Batch {
	return Batch{Table: "gh42_rows", Label: "gh42-run-1", JSON: []byte(`[{"value":18446744073709551615}]`)}
}
func httpOptions(server *httptest.Server) OptionsV1 {
	return OptionsV1{Name: "doris", HTTPOrigins: []string{server.URL}, Database: "gh42", User: "synthetic", Password: "credential-canary", Plaintext: true, Timeout: 2 * time.Second}
}
func TestStreamLoadRejectingResponsesAndRowEvidence(t *testing.T) {
	cases := []struct {
		name, body     string
		kind           error
		state          LoadState
		rows, complete bool
	}{
		{"success", goodLoad, nil, LoadVisible, true, true},
		{"empty", "", ErrProtocol, LoadUnknown, false, false},
		{"object", "{}", ErrProtocol, LoadUnknown, false, false},
		{"null", "null", ErrProtocol, LoadUnknown, false, false},
		{"truncated", goodLoad[:len(goodLoad)-1], ErrProtocol, LoadUnknown, false, false},
		{"trailing", goodLoad + "{}", ErrProtocol, LoadUnknown, false, false},
		{"duplicate", strings.Replace(goodLoad, `"Status":"Success"`, `"Status":"Fail","Status":"Success"`, 1), ErrProtocol, LoadUnknown, false, false},
		{"identity", strings.Replace(goodLoad, "gh42-run-1", "other-run", 1), ErrProtocol, LoadUnknown, false, false},
		{"status", strings.Replace(goodLoad, "Success", "Unrecognized", 1), ErrProtocol, LoadUnknown, false, false},
		{"missing-txn", strings.Replace(goodLoad, `"TxnId":42,`, "", 1), ErrProtocol, LoadUnknown, false, false},
		{"null-txn", strings.Replace(goodLoad, `"TxnId":42`, `"TxnId":null`, 1), ErrProtocol, LoadUnknown, false, false},
		{"missing-count", strings.Replace(goodLoad, `"NumberFilteredRows":0,`, "", 1), ErrProtocol, LoadVisible, false, false},
		{"negative-count", strings.Replace(goodLoad, `"NumberFilteredRows":0`, `"NumberFilteredRows":-1`, 1), ErrProtocol, LoadVisible, false, false},
		{"inconsistent-count", strings.Replace(goodLoad, `"NumberLoadedRows":1`, `"NumberLoadedRows":2`, 1), ErrProtocol, LoadVisible, false, false},
		{"filtered", strings.Replace(strings.Replace(goodLoad, `"NumberLoadedRows":1`, `"NumberLoadedRows":0`, 1), `"NumberFilteredRows":0`, `"NumberFilteredRows":1`, 1), ErrRowQuality, LoadVisible, true, false},
		{"unselected", strings.Replace(strings.Replace(goodLoad, `"NumberLoadedRows":1`, `"NumberLoadedRows":0`, 1), `"NumberUnselectedRows":0`, `"NumberUnselectedRows":1`, 1), ErrRowQuality, LoadVisible, true, false},
		{"publish-timeout", strings.Replace(goodLoad, "Success", "Publish Timeout", 1), ErrUncertain, LoadCommitted, true, false},
		{"two-phase", strings.Replace(goodLoad, `"TwoPhaseCommit":"false"`, `"TwoPhaseCommit":"true"`, 1), ErrUnsupported, LoadUnknown, false, false},
		{"fail", strings.Replace(goodLoad, "Success", "Fail", 1), ErrLoad, LoadRejected, true, false},
		{"already-exists", `{"Label":"gh42-run-1","Status":"Label Already Exists","ExistingJobStatus":"FINISHED"}`, ErrDuplicate, LoadUnknown, false, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
				user, password, ok := r.BasicAuth()
				if !ok || user != "synthetic" || password != "credential-canary" || r.Method != "PUT" ||
					r.URL.Path != "/api/gh42/gh42_rows/_stream_load" || string(body) != string(testBatch().JSON) ||
					r.Header.Get("strict_mode") != "true" || r.Header.Get("group_commit") != "off_mode" || r.Header.Get("max_filter_ratio") != "0" {
					t.Error("native load request contract changed")
				}
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			f := bindFixture(t, httpOptions(server), 1)
			receipt, err := f.client.StreamLoad(context.Background(), correlation("load"), testBatch())
			result := observe(t, receipt, err)
			if (test.kind == nil && result.Err() != nil) || (test.kind != nil && !errors.Is(result.Err(), test.kind)) {
				t.Fatalf("wrong response error: %v", result.Err())
			}
			value := result.Outcome.Value
			load, present := value.Load()
			if !present || load.State != test.state || load.RowsKnown != test.rows || value.Complete() != test.complete || !value.Dispatched() || calls.Load() != 1 {
				t.Fatalf("false effect: state=%d rowsKnown=%t complete=%t dispatches=%d", load.State, load.RowsKnown, value.Complete(), calls.Load())
			}
			if load.PayloadSHA256 != sha256.Sum256(testBatch().JSON) {
				t.Fatal("payload witness changed")
			}
			if test.name == "already-exists" && (!load.Duplicate || load.ExistingJobStatus != "FINISHED") {
				t.Fatal("lost duplicate-job evidence")
			}
			conformance.Runtime(t, value, new(Result), "gh42-run-1")
			conformance.Runtime(t, load, new(LoadEvidence), "gh42-run-1")
			drain(t, f.inbox, 1)
		})
	}
}
func TestLabelStateNeverInventsRowOrPayloadEvidence(t *testing.T) {
	for _, state := range []string{"UNKNOWN", "PREPARE", "PRECOMMITTED", "COMMITTED", "VISIBLE", "ABORTED", "FUTURE"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" || r.URL.Path != "/api/gh42/get_load_state" || r.URL.Query().Get("label") != "gh42-run-1" {
				t.Error("label request changed")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "msg": "success", "data": state})
		}))
		f := bindFixture(t, httpOptions(server), 1)
		receipt, err := f.client.InspectLabel(context.Background(), correlation("label"), "gh42-run-1")
		result := observe(t, receipt, err)
		load, _ := result.Outcome.Value.Load()
		if load.RowsKnown || load.TransactionKnown || load.Table != "" || load.PayloadSHA256 != [32]byte{} {
			t.Fatal("label state invented load witness")
		}
		if (state == "VISIBLE") != (result.Err() == nil) {
			t.Fatal("non-visible state certified", state)
		}
		if state == "UNKNOWN" && (load.State != LoadUnknown || !errors.Is(result.Err(), ErrUncertain)) {
			t.Fatal("UNKNOWN became absence")
		}
		drain(t, f.inbox, 1)
		server.Close()
	}
	for _, body := range []string{"{}", `{"code":0,"data":"VISIBLE"}`, `{"code":null,"msg":"success","data":"VISIBLE"}`,
		`{"code":0,"msg":"success","data":null}`, `{"code":0,"msg":"success","data":"UNKNOWN","data":"VISIBLE"}`} {
		evidence := LoadEvidence{}
		if parseLabel([]byte(body), &evidence) == nil {
			t.Fatal("malformed label observation accepted")
		}
	}
}
func TestUnknownAfterLostReplyNeverRetriesMutation(t *testing.T) {
	var puts, gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			gets.Add(1)
			_, _ = io.WriteString(w, `{"msg":"success","code":0,"data":"UNKNOWN"}`)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		puts.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	f := bindFixture(t, httpOptions(server), 1)
	receipt, err := f.client.StreamLoad(context.Background(), correlation("lost"), testBatch())
	result := observe(t, receipt, err)
	load, _ := result.Outcome.Value.Load()
	if result.Err() == nil || load.State != LoadUnknown || puts.Load() != 1 || gets.Load() != 0 {
		t.Fatal("lost reply retried or reconciled implicitly")
	}
	drain(t, f.inbox, 1)
	receipt, err = f.client.InspectLabel(context.Background(), correlation("reconcile"), "gh42-run-1")
	result = observe(t, receipt, err)
	if !errors.Is(result.Err(), ErrUncertain) || puts.Load() != 1 || gets.Load() != 1 {
		t.Fatal("UNKNOWN caused replay")
	}
	drain(t, f.inbox, 1)
}
func TestBatchValidationAndResponseBounds(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, strings.Repeat("x", 2048))
	}))
	defer server.Close()
	o := httpOptions(server)
	o.MaxHTTPResponseBytes = 1024
	f := bindFixture(t, o, 1)
	for _, body := range []string{"{}", `[1]`, `[null]`, `[{}]{}`, `[]`, `[{"x":1,"x":2}]`} {
		batch := testBatch()
		batch.JSON = []byte(body)
		receipt, err := f.client.StreamLoad(context.Background(), correlation("invalid"), batch)
		result := observe(t, receipt, err)
		load, _ := result.Outcome.Value.Load()
		if result.Err() == nil || load.State != LoadNotDispatched || result.Outcome.Value.Dispatched() {
			t.Fatal("invalid input dispatched")
		}
		drain(t, f.inbox, 1)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid batch reached transport")
	}
	receipt, err := f.client.StreamLoad(context.Background(), correlation("oversize"), testBatch())
	result := observe(t, receipt, err)
	if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() || calls.Load() != 1 {
		t.Fatal("response not bounded")
	}
	drain(t, f.inbox, 1)
}
func TestLoadCancellationQueueAndSourceClose(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	o := httpOptions(server)
	o.Active = 1
	o.Queued = 1
	f := bindFixture(t, o, 3)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan invocationResult, 1)
	go func() {
		r, e := f.client.StreamLoad(ctx, correlation("active"), testBatch())
		done <- invocationResult{r, e}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("load not entered")
	}
	queued, cancelQueued := context.WithTimeout(context.Background(), 20*time.Millisecond)
	receipt, err := f.client.StreamLoad(queued, correlation("queued"), testBatch())
	cancelQueued()
	if receipt != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("canceled admission became accepted upload")
	}
	closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err := f.assembly.Close(closeContext); err == nil {
		t.Fatal("source close claimed live call released")
	}
	cancelClose()
	cancel()
	select {
	case call := <-done:
		result := observe(t, call.receipt, call.err)
		load, _ := result.Outcome.Value.Load()
		if !errors.Is(result.Err(), context.Canceled) || load.State != LoadUnknown || !result.Released {
			t.Fatal("cancel lost load uncertainty/local cleanup")
		}
	case <-time.After(time.Second):
		t.Fatal("load/cleanup not joined")
	}
	drain(t, f.inbox, 1)
	closeContext, cancelClose = context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	if err := f.assembly.Close(closeContext); err != nil {
		t.Fatal(err)
	}
}
func TestAdmissionOverloadAndCanceledContextDoNotSubmit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected HTTP dispatch") }))
	defer server.Close()
	o := httpOptions(server)
	o.Active = 1
	f := bindFixture(t, o, 1)
	lease, err := f.client.access.Acquire(context.Background(), f.client.owner.settings.reservation())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.client.StreamLoad(context.Background(), correlation("full"), testBatch())
	lease.Release()
	if receipt != nil || !errors.Is(err, resource.ErrCapacity) || f.inbox.Usage().Outstanding != 0 {
		t.Fatal("overload not rejected/released")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err := f.client.StreamLoad(ctx, correlation("canceled"), testBatch()); receipt != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled call accepted")
	}
}

func FuzzLoadResponseNeverPanics(f *testing.F) {
	for _, body := range []string{goodLoad, "{}", "null", `{"Status":"Success"}`} {
		f.Add(body)
	}
	f.Fuzz(func(t *testing.T, body string) {
		if len(body) > 64<<10 {
			return
		}
		evidence := LoadEvidence{Label: "gh42-run-1"}
		err := parseLoad([]byte(body), 1, &evidence)
		if err == nil && (evidence.State != LoadVisible || !evidence.RowsKnown || evidence.LoadedRows != 1 || !evidence.TransactionKnown) {
			t.Fatal("false success")
		}
		_ = parseLabel([]byte(body), &LoadEvidence{})
	})
}
func BenchmarkValidateJSONBatch(b *testing.B) {
	payload := []byte("[" + strings.TrimSuffix(strings.Repeat(`{"id":42,"value":"synthetic"},`, 128), ",") + "]")
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		if _, err := validateBatch(payload, 1024); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateJSONLargeCell(b *testing.B) {
	payload := []byte(`[{"value":"` + strings.Repeat("x", 1<<20-14) + `"}]`)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		if _, err := validateBatch(payload, 1); err != nil {
			b.Fatal(err)
		}
	}
}
func ExampleResult_Load() {
	var result Result
	evidence, present := result.Load()
	fmt.Println(present, evidence.State == LoadUnknown, result.Complete())
	// Output: false true false
}
