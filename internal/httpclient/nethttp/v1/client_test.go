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

package nethttp

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type fixture struct {
	client       *Client
	selected     resource.Selection[Source]
	assembly     *resource.Assembly
	inbox        *invocation.Inbox[Result]
	cleanupCause error
}

func deadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func bindFixture(t *testing.T, options OptionsV1, capacity int, layers ...resource.Layer) *fixture {
	t.Helper()
	selected, err := Select(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "test", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := &fixture{client: client, selected: selected, assembly: assembly, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(deadline(t)); err != nil && (result.cleanupCause == nil || !errors.Is(err, result.cleanupCause)) {
			t.Error("assembly cleanup remains incomplete", err)
		}
	})
	return result
}
func newPeer(t *testing.T, h2 bool, handler http.HandlerFunc) (*httptest.Server, OptionsV1) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = h2
	server.StartTLS()
	t.Cleanup(server.Close)
	roots := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	return server, OptionsV1{Name: "source", HTTP1: !h2, HTTP2: h2, RootCAPEM: string(roots)}
}
func newRequest(t *testing.T, method, endpoint string, body io.Reader) *http.Request {
	t.Helper()
	request, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
func correlation(value string) fault.Correlation { return fault.Correlation{Call: value} }
func settle(t *testing.T, fixture *fixture, receipt *invocation.Receipt[Result]) invocation.Result[Result] {
	t.Helper()
	if receipt == nil {
		t.Fatal("missing accepted receipt")
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	independent, err := delivery.Receipt().WaitReleased(deadline(t))
	if err != nil || independent.Context != result.Context || !independent.Final || !independent.Released {
		t.Fatal("independent evidence differs", err)
	}
	if !errors.Is(independent.Err(), result.Err()) && (independent.Err() != nil || result.Err() != nil) {
		// Result.Err may construct an aggregate wrapper; compare its primary identity.
		if independent.Outcome.Primary != result.Outcome.Primary || independent.Outcome.Cleanup != result.Outcome.Cleanup {
			t.Fatal("independent errors differ")
		}
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	return result
}
func eachProtocol(t *testing.T, run func(*testing.T, bool)) {
	t.Helper()
	t.Run("h1", func(t *testing.T) { run(t, false) })
	t.Run("h2", func(t *testing.T) { run(t, true) })
}

func TestNamedClientNativeRequests(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var received atomic.Int64
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			received.Add(1)
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
			}
			writer.Header().Set("X-Observed-Method", request.Method)
			writer.WriteHeader(429)
			_, _ = writer.Write(body)
		})
		f := bindFixture(t, options, 4)
		request := newRequest(t, "PATCH", server.URL, http.NoBody)
		request.Header.Set("Authorization", "synthetic")
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("empty"), request)
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		wantProtocol := "HTTP/1.1"
		if h2 {
			wantProtocol = "HTTP/2.0"
		}
		data := result.Outcome.Value
		if result.Err() != nil || !data.Complete() || data.DataCopy() == nil || len(data.DataCopy()) != 0 ||
			data.Metadata().StatusCode() != 429 || data.Metadata().Protocol() != wantProtocol ||
			data.Metadata().HeadersCopy().Get("X-Observed-Method") != "PATCH" || received.Load() != 1 {
			t.Fatal("native response or empty-data semantics differ", result.Err())
		}
		if result.Attempts.Exact || result.Attempts.Observed != 1 || data.Exchanges() != 1 {
			t.Fatal("native attempt evidence overstated")
		}
	})
}

func TestCanceledRequestAndInboxSaturationDoNotSend(t *testing.T) {
	var received atomic.Int64
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { received.Add(1) })
	f := bindFixture(t, options, 1)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	receipt, err := f.client.Do(canceled, deadline(t), correlation("canceled"), newRequest(t, "GET", server.URL, nil))
	if receipt != nil || !errors.Is(err, context.Canceled) || received.Load() != 0 {
		t.Fatal("pre-canceled work admitted")
	}
	receipt, err = f.client.Do(deadline(t), deadline(t), correlation("first"), newRequest(t, "GET", server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := f.client.Do(deadline(t), deadline(t), correlation("second"), newRequest(t, "GET", server.URL, nil))
	if rejected != nil || !errors.Is(err, invocation.ErrEvidence) || received.Load() != 1 {
		t.Fatal("evidence saturation sent native work")
	}
	settle(t, f, receipt)
}
