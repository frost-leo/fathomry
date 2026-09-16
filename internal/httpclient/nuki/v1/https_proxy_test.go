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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
)

type flushingWriter struct{ http.ResponseWriter }

func (writer flushingWriter) Write(data []byte) (int, error) {
	count, err := writer.ResponseWriter.Write(data)
	writer.ResponseWriter.(http.Flusher).Flush()
	return count, err
}

func TestProviderHTTPSH2ProxySharesPhysicalQuotaNotStreamCancellation(t *testing.T) {
	var workers sync.WaitGroup
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "CONNECT" || request.ProtoMajor != 2 {
			t.Error("native HTTPS proxy was not H2 CONNECT")
			writer.WriteHeader(400)
			return
		}
		target, err := net.Dial("tcp", request.Host)
		if err != nil {
			t.Error(err)
			writer.WriteHeader(502)
			return
		}
		defer target.Close()
		writer.WriteHeader(200)
		writer.(http.Flusher).Flush()
		done := make(chan struct{})
		workers.Add(1)
		go func() { defer workers.Done(); defer close(done); _, _ = io.Copy(target, request.Body); target.Close() }()
		_, _ = io.Copy(flushingWriter{writer}, target)
		target.Close()
		request.Body.Close()
		<-done
	}))
	proxy.EnableHTTP2 = true
	proxy.StartTLS()
	defer proxy.Close()
	first := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Length", "4")
		if request.URL.Path == "/slow" {
			_, _ = io.WriteString(writer, "b")
			writer.(http.Flusher).Flush()
			<-request.Context().Done()
			return
		}
		_, _ = io.WriteString(writer, "body")
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "other") }))
	defer second.Close()
	options := providerOptions()
	options.ProxyURL = proxy.URL
	options.Native.TLS = testRoots(proxy)
	options.MaxConnections = 1
	fixture := bindProvider(t, options)
	partial, err := fixture.client.Consume(testContext(t), fault.Correlation{Call: "h2-cancel"}, nativeRequest(t, "GET", first.URL+"/slow", nil), func(_ context.Context, response *Response) error {
		var prefix [1]byte
		_, err := io.ReadFull(response, prefix[:])
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	partialResult := outcome(t, fixture, partial)
	if partialResult.Err() != nil || partialResult.Outcome.Value.Complete() {
		t.Fatal("early stream closure was conflated with complete data", partialResult.Err())
	}
	for _, address := range []string{first.URL, second.URL, first.URL} {
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "h2-proxy"}, nativeRequest(t, "GET", address, nil))
		if err != nil {
			t.Fatal(err)
		}
		result := outcome(t, fixture, receipt)
		expected := "body"
		if address == second.URL {
			expected = "other"
		}
		if result.Err() != nil || string(result.Outcome.Value.DataCopy()) != expected {
			t.Fatal("H2 proxy stream failed", result.Err())
		}
	}
	fixture.client.owner.mu.Lock()
	sockets := len(fixture.client.owner.sockets)
	fixture.client.owner.mu.Unlock()
	if sockets != 1 {
		t.Fatal("proxy sessions bypassed physical source quota", sockets)
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal("H2 proxy cleanup", err)
	}
	workers.Wait()
}
