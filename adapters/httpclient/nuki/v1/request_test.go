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

package nuki

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	nativehttp "github.com/nukilabs/http"
	"github.com/nukilabs/http/cookiejar"
)

func nativeRequest(t testing.TB, method, address string, body io.Reader) *nativehttp.Request {
	t.Helper()
	request, err := nativehttp.NewRequest(method, address, body)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
func publicResult(t testing.TB, dependencies Dependencies, receipt *adapters.Receipt[Result], admission error) (Result, error) {
	t.Helper()
	if admission != nil || receipt == nil {
		t.Fatal("operation was not admitted", admission)
	}
	snapshot, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := dependencies.Evidence.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	otherReceipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	other, err := otherReceipt.WaitReleased(testContext(t))
	if err != nil || other.Info() != snapshot.Info() {
		t.Fatal("independent evidence attribution changed", err)
	}
	value, present := snapshot.ValueCopy()
	copy, copied := other.ValueCopy()
	if present != copied || value.Attribution() != copy.Attribution() || string(value.DataCopy()) != string(copy.DataCopy()) {
		t.Fatal("direct/independent result changed")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	return value, snapshot.Err()
}

func TestFiniteAsyncAndCallbackCompletionAreDistinct(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Result", "original")
		w.Header().Set("Content-Length", "7")
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "payload")
	}))
	t.Cleanup(peer.Close)
	owner, dependencies := testOwner(t, testSettings("response"), testNative())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	input := nativeRequest(t, "GET", peer.URL, nil).WithContext(canceled)
	receipt, err := owner.Client().Do(testContext(t), input)
	value, err := publicResult(t, dependencies, receipt, err)
	if err != nil || !value.Complete() || value.Metadata().StatusCode() != http.StatusTeapot || string(value.DataCopy()) != "payload" {
		t.Fatal("method context or finite native result changed", err)
	}
	for _, complete := range []bool{false, true} {
		var retained *Response
		receipt, err := owner.Client().Consume(testContext(t), nativeRequest(t, "GET", peer.URL, nil), func(ctx context.Context, response *Response) error {
			retained = response
			if complete {
				_, err := io.ReadAll(response)
				return err
			}
			_, err := response.Read(make([]byte, 1))
			return err
		})
		value, err := publicResult(t, dependencies, receipt, err)
		if err != nil || value.Complete() != complete || value.DataCopy() != nil {
			t.Fatal("callback return fabricated EOF/retention", err)
		}
		if _, err := retained.Read(make([]byte, 1)); !errors.Is(err, ErrState) {
			t.Fatal("expired callback retained read authority", err)
		}
		headers := retained.Metadata().HeadersCopy()
		headers.Set("X-Result", "mutated")
		if value.Metadata().HeadersCopy().Get("X-Result") != "original" {
			t.Fatal("metadata copy aliased evidence")
		}
	}
}

func TestDoReturnsBeforeHeldResponseAndWaitCancellationDoesNotCancelWork(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(peer.Close)
	owner, dependencies := testOwner(t, testSettings("asynchronous"), testNative())
	type admitted struct {
		receipt *adapters.Receipt[Result]
		err     error
	}
	done := make(chan admitted, 1)
	work := testContext(t)
	input := nativeRequest(t, "GET", peer.URL, nil)
	go func() {
		receipt, err := owner.Client().Do(work, input)
		done <- admitted{receipt, err}
	}()
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("peer never received request")
	}
	var call admitted
	select {
	case call = <-done:
	case <-testContext(t).Done():
		t.Fatal("Do waited for response completion")
	}
	if call.err != nil || call.receipt == nil {
		t.Fatal("asynchronous request refused", call.err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := call.receipt.Wait(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("wait did not retain cancellation", err)
	}
	if snapshot, _ := call.receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("canceled waiter released live request")
	}
	unblock()
	value, err := publicResult(t, dependencies, call.receipt, nil)
	if err != nil || !value.Complete() || string(value.DataCopy()) != "finished" {
		t.Fatal("wait cancellation canceled actual work", err)
	}
}

func TestExactSourceRequestBoundRejectsBeforeNativeAdmission(t *testing.T) {
	var dials atomic.Int64
	native := testNative()
	native.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("unexpected dial")
	}
	settings := testSettings("exact-source")
	settings.MaxHeaderBytes = pointer[int64](1024)
	owner, dependencies := testOwner(t, settings, native)
	body := new(untouchedBody)
	input := nativeRequest(t, "POST", "http://127.0.0.1:1", body)
	input.Header.Set("X-Large", strings.Repeat("x", 2048))
	receipt, err := owner.Client().Do(testContext(t), input)
	if receipt == nil || !errors.Is(err, ErrLimit) {
		t.Fatal("source-bound refusal lacked public admission evidence", err)
	}
	value, resultErr := publicResult(t, dependencies, receipt, nil)
	if !errors.Is(resultErr, ErrLimit) || value.HasData() || dials.Load() != 0 || body.reads.Load() != 0 || body.closes.Load() != 0 {
		t.Fatal("exact source refusal acquired native work/input", resultErr)
	}
}

type untouchedBody struct{ reads, closes atomic.Int64 }

func (body *untouchedBody) Read([]byte) (int, error) { body.reads.Add(1); return 0, io.EOF }
func (body *untouchedBody) Close() error             { body.closes.Add(1); return nil }

func TestNativeExtensionPreflightPrecedesPublicAdmission(t *testing.T) {
	owner, dependencies := testOwner(t, testSettings("preflight"), testNative())
	before, _ := dependencies.Evidence.Inspect()
	body := new(untouchedBody)
	input := nativeRequest(t, "POST", "http://127.0.0.1:1", body)
	input.ExcludedCookies = map[string]struct{}{strings.Repeat("x", 1<<20): {}}
	if receipt, err := owner.Client().Do(testContext(t), input); err == nil || receipt != nil {
		t.Fatal("oversized native extension was admitted")
	}
	after, _ := dependencies.Evidence.Inspect()
	if before != after || body.reads.Load() != 0 || body.closes.Load() != 0 {
		t.Fatal("preflight acquired input or evidence")
	}
}

func TestNativePriorityAndCookieControlsSurvivePublicRoute(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var headers atomic.Value
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers.Store(r.Header.Clone())
		w.Header().Set("Set-Cookie", "received=present; Path=/")
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(peer.Close)
	var seen atomic.Int64
	native := testNative()
	native.Jar = jar
	native.Before = []func(context.Context, *nativehttp.Request) error{func(_ context.Context, request *nativehttp.Request) error {
		if request.Priority != nativehttp.PriorityLow || request.Header.Get("Priority") != "u=3" || !request.DiscardResponseCookies {
			return errors.New("native extensions changed")
		}
		if _, exists := request.ExcludedCookies["omit"]; !exists {
			return errors.New("cookie exclusions lost")
		}
		seen.Add(1)
		return nil
	}}
	owner, dependencies := testOwner(t, testSettings("cookies"), native)
	input := nativeRequest(t, "GET", peer.URL, nil)
	jar.SetCookies(input.URL, []*nativehttp.Cookie{{Name: "keep", Value: "yes"}, {Name: "omit", Value: "no"}})
	input.SetPriority(nativehttp.PriorityLow, false)
	input.Header.Set("X-Control", "preserved")
	input.ExcludedCookies = map[string]struct{}{"omit": {}}
	input.DiscardResponseCookies = true
	receipt, err := owner.Client().Do(testContext(t), input)
	_, err = publicResult(t, dependencies, receipt, err)
	if err != nil {
		t.Fatal(err)
	}
	observed := headers.Load().(http.Header)
	if seen.Load() != 1 || !strings.Contains(observed.Get("Cookie"), "keep=yes") || strings.Contains(observed.Get("Cookie"), "omit=") || observed.Get("X-Control") != "preserved" {
		t.Fatal("native cookie/header controls lacked observed effect", observed)
	}
	if observed.Get("Priority") != "" {
		t.Fatal("selected native H1 Priority filtering changed", observed)
	}
	for _, cookie := range jar.Cookies(input.URL) {
		if cookie.Name == "received" {
			t.Fatal("response cookie discard lost")
		}
	}
}

func TestCallbackHoldAndEvidenceSaturationDoNotBlockCleanup(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "body") }))
	t.Cleanup(peer.Close)
	value := testSettings("saturated")
	value.MaxActive = pointer(1)
	owner, dependencies := testOwner(t, value, testNative())
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	receipt, err := owner.Client().Consume(testContext(t), nativeRequest(t, "GET", peer.URL, nil), func(ctx context.Context, response *Response) error {
		close(entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("callback not entered")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := receipt.Wait(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("wait cancellation lost", err)
	}
	body := new(untouchedBody)
	if next, err := owner.Client().Do(testContext(t), nativeRequest(t, "POST", peer.URL, body)); next != nil || !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("evidence saturation admitted work", err)
	}
	if body.reads.Load() != 0 || body.closes.Load() != 0 {
		t.Fatal("refused input was borrowed")
	}
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("saturated cleanup failed", err)
	}
	if _, err := receipt.WaitReleased(testContext(t)); err != nil {
		t.Fatal(err)
	}
	status, _ := dependencies.Runtime.Inspect()
	if status.Active != 0 || status.WorkBytes != 0 {
		t.Fatal("callback/source retained work")
	}
}
