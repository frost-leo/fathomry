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

package httpcloak

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func TestRequestContextCancellationRemainsNativeInput(t *testing.T) {
	entered := make(chan struct{})
	address, options := peer(t, HTTP2, func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() })
	fixture := bindFixture(t, options, 1)
	outer, cancelOuter := context.WithCancel(testContext(t))
	defer cancelOuter()
	requestContext, cancelRequest := context.WithCancel(testContext(t))
	defer cancelRequest()
	input := request(t, "GET", address, nil).WithContext(requestContext)
	type outcome struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		receipt, err := fixture.client.Do(outer, testContext(t), fault.Correlation{Call: "cancel"}, input)
		done <- outcome{receipt, err}
	}()
	select {
	case <-entered:
	case <-outer.Done():
		t.Fatal("request did not reach peer")
	}
	cancelRequest()
	select {
	case result := <-done:
		got := settle(t, fixture, result.receipt)
		if !errors.Is(got.Err(), context.Canceled) {
			t.Fatal("native request context cause missing")
		}
	case <-time.After(250 * time.Millisecond):
		t.Error("request context cancellation was discarded")
		cancelOuter()
		select {
		case result := <-done:
			settle(t, fixture, result.receipt)
		case <-time.After(time.Second):
			t.Fatal("control could not join caller")
		}
	}
}
func TestClosingPartialStreamDoesNotInventCancellation(t *testing.T) {
	address, options := peer(t, HTTP1, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "64")
		_, _ = io.WriteString(w, "part")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	fixture := bindFixture(t, options, 1)
	stream, receipt, err := fixture.client.Open(testContext(t), fault.Correlation{Call: "partial"}, request(t, "GET", address, nil))
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 4)
	if _, err := io.ReadFull(stream, data); err != nil {
		t.Fatal(err)
	}
	closeErr := stream.Close(testContext(t))
	got := settle(t, fixture, receipt)
	if got.Outcome.Primary != nil {
		t.Fatal("intentional partial close invented primary failure", got.Outcome.Primary)
	}
	if (closeErr == nil) != (got.Outcome.Cleanup == nil) {
		t.Fatal("native cleanup result disappeared")
	}
	if got.Outcome.Value.Complete() {
		t.Fatal("partial close certified complete")
	}
}

func TestCanceledCallerRetainsVerifierAndAllCauses(t *testing.T) {
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(releaseCallback) }) }
	defer release()
	callbackErr := errors.New("test verifier failure")
	cancelErr := errors.New("test caller cause")
	address, options := peer(t, HTTP2, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "unexpected") })
	options.MaxActive = 1
	options.Native.Verify.VerifyConnection = func(tls.ConnectionState) error {
		close(callbackEntered)
		<-releaseCallback
		return callbackErr
	}
	fixture := bindFixture(t, options, 2)
	ctx, cancel := context.WithCancelCause(testContext(t))
	defer cancel(context.Canceled)
	type outcome struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		receipt, err := fixture.client.Do(ctx, testContext(t), fault.Correlation{Call: "callback"}, request(t, "GET", address, nil))
		done <- outcome{receipt, err}
	}()
	select {
	case <-callbackEntered:
	case <-ctx.Done():
		t.Fatal("verifier not reached")
	}
	cancel(cancelErr)
	var returned outcome
	select {
	case returned = <-done:
	case <-time.After(time.Second):
		t.Fatal("caller could not stop waiting")
	}
	if returned.receipt == nil {
		t.Fatal("accepted call disappeared")
	}
	if result, available := returned.receipt.Result(); available && result.Released {
		t.Fatal("callback still running after release evidence")
	}
	report := fixture.assembly.Snapshot()
	if report.Sources[0].Usage.Active != 1 {
		t.Fatal("callback lost source admission")
	}
	release()
	result := settle(t, fixture, returned.receipt)
	for _, cause := range []error{context.Canceled, cancelErr, callbackErr} {
		if !errors.Is(result.Err(), cause) {
			t.Errorf("lost original cause %T", cause)
		}
	}
}
