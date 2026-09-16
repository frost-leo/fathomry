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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	nativehttp "github.com/nukilabs/http"
)

func TestProviderWaitAliasAdmissionAndRequiredEvidence(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		close(entered)
		writer.Header().Set("Content-Length", "4")
		writer.WriteHeader(200)
		writer.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(writer, "done")
		case <-request.Context().Done():
		}
	}))
	defer peer.Close()
	options := providerOptions()
	options.MaxActive = 1
	fixture := bindProvider(t, options)
	borrowed := resource.Borrow("nuki-alias", fixture.assembly, fixture.selected)
	receiver, err := resource.Assemble(testContext(t), testContext(t), "borrower", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close(testContext(t))
	alias, err := Bind(receiver, borrowed, fixture.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "held"}, nativeRequest(t, "GET", peer.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("request not entered")
	}
	wait, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := receipt.Wait(wait); !errors.Is(err, invocation.ErrWait) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiting and completion conflated", err)
	}
	if _, err := alias.Do(testContext(t), fault.Correlation{Call: "alias"}, nativeRequest(t, "GET", peer.URL, nil)); !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("alias multiplied admission", err)
	}
	if err := fixture.assembly.Close(testContext(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("active call was released", err)
	}
	if result, ready := receipt.Result(); ready || result.Released {
		t.Fatal("waiting cancellation fabricated a result")
	}
	unblock()
	result := outcome(t, fixture, receipt)
	if result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "done" {
		t.Fatal("accepted request did not finish", result.Err())
	}
	conformance.Result(t, result, conformance.Expected[Result]{
		Context: fault.Context{Operation: "request", Provider: ProviderID, Scope: "nuki-tests", Source: options.Name, Correlation: fault.Correlation{Call: "held"}}, Source: fixture.info, Limits: fixture.client.access.Limits(),
		Shape: invocation.Finite, Present: true, Final: true, Released: true, Attempts: invocation.Attempts{Observed: 1},
		Value: func(t testing.TB, value Result) {
			if string(value.DataCopy()) != "done" || !value.Complete() {
				t.Error("peer oracle mismatch")
			}
		},
	})
	if err := receiver.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

type closeReader struct {
	io.Reader
	closed atomic.Int32
	cause  error
}

func (reader *closeReader) Close() error { reader.closed.Add(1); return reader.cause }

func TestProviderEvidenceRefusalDoesNotAcquireBody(t *testing.T) {
	options := providerOptions()
	fixture := bindProvider(t, options)
	tiny, err := invocation.NewInbox[Result](1, 1)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(fixture.assembly, fixture.selected, tiny, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := &closeReader{Reader: strings.NewReader("data")}
	request := nativeRequest(t, "POST", "http://127.0.0.1:1", body)
	receipt, err := client.Do(testContext(t), fault.Correlation{Call: "refused"}, request)
	if receipt != nil || !errors.Is(err, invocation.ErrEvidence) || body.closed.Load() != 0 {
		t.Fatal("refusal borrowed input or bypassed evidence", err)
	}
}

func TestProviderCancellationEndsNativeUseWithoutClaimingNonEffect(t *testing.T) {
	entered, stopped := make(chan struct{}), make(chan struct{})
	fixtureStop := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer close(stopped)
		if _, err := io.ReadAll(request.Body); err != nil {
			t.Error("peer input", err)
			return
		}
		close(entered)
		select {
		case <-request.Context().Done():
		case <-fixtureStop:
		}
	}))
	defer peer.Close()
	defer close(fixtureStop)
	fixture := bindProvider(t, providerOptions())
	cause := errors.New("synthetic-cancellation")
	ctx, cancel := context.WithCancelCause(testContext(t))
	receipt, err := fixture.client.Do(ctx, fault.Correlation{Call: "cancel"}, nativeRequest(t, "POST", peer.URL, strings.NewReader("mutation-input")))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("request not submitted")
	}
	cancel(cause)
	result := outcome(t, fixture, receipt)
	if result.Err() == nil || !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) || result.Attempts.Observed != 1 || result.Attempts.Exact || result.Outcome.Value.Complete() {
		t.Fatal("cancellation evidence changed", result.Err())
	}
	select {
	case <-stopped:
	case <-testContext(t).Done():
		t.Fatal("native peer context not canceled")
	}
}

func TestProviderStreamRevocationAndPartialCompleteness(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "abcdef") }))
	defer peer.Close()
	for _, complete := range []bool{false, true} {
		fixture := bindProvider(t, providerOptions())
		var escaped *Response
		receipt, err := fixture.client.Consume(testContext(t), fault.Correlation{Call: "stream"}, nativeRequest(t, "GET", peer.URL, nil),
			func(_ context.Context, stream *Response) error {
				escaped = stream
				if complete {
					data, err := io.ReadAll(stream)
					if string(data) != "abcdef" {
						t.Error("stream data mismatch")
					}
					return err
				}
				var first [2]byte
				_, err := io.ReadFull(stream, first[:])
				if string(first[:]) != "ab" {
					t.Error("partial data mismatch")
				}
				return err
			})
		if err != nil {
			t.Fatal(err)
		}
		result := outcome(t, fixture, receipt)
		if result.Err() != nil || result.Outcome.Value.Complete() != complete || result.Outcome.Value.DataCopy() != nil || result.Shape != invocation.Stream {
			t.Fatal("partial/complete stream confused", result.Err())
		}
		if _, err := escaped.Read(make([]byte, 1)); !errors.Is(err, ErrState) {
			t.Fatal("retained capability reentered native work")
		}
	}
}

func TestProviderHooksRetainPrimaryAndCleanupCauses(t *testing.T) {
	primary, cleanup := errors.New("synthetic-hook"), errors.New("synthetic-input-close")
	options := providerOptions()
	options.Native.Before = []func(context.Context, *nativehttp.Request) error{func(_ context.Context, request *nativehttp.Request) error {
		if request.Body != nil || request.GetBody != nil || request.Response != nil {
			t.Fatal("owning input escaped")
		}
		return primary
	}}
	fixture := bindProvider(t, options)
	body := &closeReader{Reader: strings.NewReader("data"), cause: cleanup}
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "causes"}, nativeRequest(t, "POST", "http://127.0.0.1:1", body))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fixture, receipt)
	if !errors.Is(result.Outcome.Primary, primary) || !errors.Is(result.Outcome.Cleanup, cleanup) || result.Attempts.Observed != 0 || body.closed.Load() != 1 {
		t.Fatal("causal or cleanup evidence lost", result.Err())
	}
}
