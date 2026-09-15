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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestDecodedBoundsAndNativeTrailers(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var compressed bytes.Buffer
		encoder := gzip.NewWriter(&compressed)
		_, _ = encoder.Write(bytes.Repeat([]byte("x"), 64<<10))
		if err := encoder.Close(); err != nil {
			t.Fatal(err)
		}
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path == "/compressed" {
				writer.Header().Set("Content-Encoding", "gzip")
				_, _ = writer.Write(compressed.Bytes())
				return
			}
			writer.Header().Set("Trailer", "X-Final")
			_, _ = io.WriteString(writer, "body")
			writer.Header().Set("X-Final", "complete")
		})
		options.MaxResponseBytes = 16
		f := bindFixture(t, options, 1)
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("decoded"), newRequest(t, "GET", server.URL+"/compressed", nil))
		result := settle(t, f, receipt)
		if !errors.Is(err, ErrLimit) || !result.Outcome.Value.Metadata().Uncompressed() || result.Outcome.Value.Complete() || len(result.Outcome.Value.DataCopy()) != 16 {
			t.Fatal("decoded overflow was hidden", err)
		}
		receipt, err = f.client.Do(deadline(t), deadline(t), correlation("trailer"), newRequest(t, "GET", server.URL+"/trailer", nil))
		if err != nil {
			t.Fatal(err)
		}
		result = settle(t, f, receipt)
		if !result.Outcome.Value.Complete() || result.Outcome.Value.TrailersCopy().Get("X-Final") != "complete" {
			t.Fatal("terminal trailers were lost")
		}
	})
}

func TestNativeRetryIsNotAnExactLogicalAttemptCount(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if requests.Add(1) == 2 {
			connection, _, err := writer.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		_, _ = io.WriteString(writer, "retry-evidence")
	}))
	t.Cleanup(server.Close)
	f := bindFixture(t, OptionsV1{Name: "native-retry", HTTP1: true}, 1)
	for _, id := range []string{"warm", "retried"} {
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation(id), newRequest(t, "GET", server.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		if result.Attempts.Exact || result.Attempts.Observed != 1 || result.Outcome.Value.Exchanges() != 1 {
			t.Fatal("native retry count was overstated")
		}
	}
	if requests.Load() != 3 {
		t.Fatal("native retry control did not exercise the additional peer request")
	}
}

func TestNativeDoBlockedByCloseStillReturnsOwnedWait(t *testing.T) {
	body := &blockingBody{Reader: bytes.NewReader([]byte("body")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(body.release) }) }
	defer unblock()
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.Copy(io.Discard, request.Body) })
	options.Timeout = 50 * time.Millisecond
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("blocked-close"), newRequest(t, "POST", server.URL, body))
	if !errors.Is(err, context.DeadlineExceeded) || receipt == nil {
		t.Fatal("native Do blocked the controlled waiter", err)
	}
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("native close boundary not reached")
	}
	if result, ok := receipt.Result(); ok && result.Released {
		t.Fatal("native callback was prematurely released")
	}
	if err := f.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("native callback ownership lost")
	}
	// Release through the caller-owned body; the SDK operation and its independent
	// evidence must subsequently finish without another admission slot.
	unblock()
	result := settle(t, f, receipt)
	if !errors.Is(result.Err(), context.DeadlineExceeded) || result.Outcome.Value.Complete() {
		t.Fatal("blocked native failure changed", result.Err())
	}
}
