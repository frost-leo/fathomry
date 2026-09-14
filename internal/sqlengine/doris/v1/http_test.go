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
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestTrustedRedirectPreservesExactBodyAndAuthorization(t *testing.T) {
	var targetCalls, sourceCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		_, password, _ := r.BasicAuth()
		if string(body) != string(testBatch().JSON) || r.Method != "PUT" || r.Header.Get("label") != "gh42-run-1" || password != "credential-canary" {
			t.Error("authorized redirect changed body/identity")
		}
		_, _ = io.WriteString(w, goodLoad)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceCalls.Add(1)
		w.Header().Set("Location", target.URL+r.URL.Path)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	o := httpOptions(source)
	o.HTTPOrigins = append(o.HTTPOrigins, target.URL)
	f := bindFixture(t, o, 1)
	o.HTTPOrigins[1] = "http://untrusted.invalid:80"
	receipt, err := f.client.StreamLoad(context.Background(), correlation("redirect"), testBatch())
	result := observe(t, receipt, err)
	if result.Err() != nil || !result.Outcome.Value.Complete() || targetCalls.Load() != 1 || sourceCalls.Load() != 1 {
		t.Fatal("trusted redirect failed", result.Err())
	}
	if result.Attempts.Observed != 2 || result.Attempts.Exact {
		t.Fatal("redirect dispatch accounting changed")
	}
	drain(t, f.inbox, 1)
}
func TestUntrustedRedirectCannotDispatchCredentialsOrData(t *testing.T) {
	for _, suffix := range []string{"", "/other", "?bad=true", "#fragment", "/../gh42/gh42_rows/_stream_load"} {
		var hits atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", target.URL+r.URL.Path+suffix)
			w.WriteHeader(307)
		}))
		o := httpOptions(source)
		if suffix != "" {
			o.HTTPOrigins = append(o.HTTPOrigins, target.URL)
		}
		f := bindFixture(t, o, 1)
		receipt, err := f.client.StreamLoad(context.Background(), correlation("deny"), testBatch())
		result := observe(t, receipt, err)
		if !errors.Is(result.Err(), ErrUnsupported) || hits.Load() != 0 || result.Outcome.Value.Complete() {
			t.Fatal("redirect escaped trust")
		}
		drain(t, f.inbox, 1)
		source.Close()
		target.Close()
	}
	previous, _ := url.Parse("https://trusted.test:443/api/gh42/gh42_rows/_stream_load")
	downgraded, _ := url.Parse("http://trusted.test:80/api/gh42/gh42_rows/_stream_load")
	s := defaults(OptionsV1{Plaintext: true, HTTPOrigins: []string{"http://trusted.test:80"}})
	if s.redirectAllowed(previous, downgraded, previous.Path, "") {
		t.Fatal("HTTPS downgrade trusted")
	}
}
func TestHTTPStatusHeadersRedirectLoopAndTLS(t *testing.T) {
	for _, mode := range []string{"status", "header", "loop"} {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			switch mode {
			case "status":
				w.WriteHeader(503)
				_, _ = io.WriteString(w, goodLoad)
			case "header":
				w.Header().Set("X-Large", strings.Repeat("a", 40<<10))
				_, _ = io.WriteString(w, goodLoad)
			case "loop":
				w.Header().Set("Location", r.URL.String())
				w.WriteHeader(307)
			}
		}))
		f := bindFixture(t, httpOptions(server), 1)
		receipt, err := f.client.StreamLoad(context.Background(), correlation("reject-http"), testBatch())
		result := observe(t, receipt, err)
		if result.Err() == nil || result.Outcome.Value.Complete() || hits.Load() > 3 {
			t.Fatal("HTTP failure certified or redirect bound exceeded")
		}
		conformance.Private(t, result.Err(), "credential-canary", "gh42-run-1")
		drain(t, f.inbox, 1)
		server.Close()
	}
	certificate, roots := testCertificate(t)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, goodLoad) }))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	o := httpOptions(server)
	o.Plaintext = false
	o.RootCAPEM = roots
	f := bindFixture(t, o, 1)
	receipt, err := f.client.StreamLoad(context.Background(), correlation("tls"), testBatch())
	result := observe(t, receipt, err)
	if result.Err() != nil || !result.Outcome.Value.Complete() {
		t.Fatal("verified HTTPS failed", result.Err())
	}
	drain(t, f.inbox, 1)
}

type closeErrorConn struct {
	net.Conn
	cause  error
	closes atomic.Int32
}

func (conn *closeErrorConn) Close() error {
	conn.closes.Add(1)
	_ = conn.Conn.Close()
	return conn.cause
}

func TestHTTPFirstCloseErrorSurvivesTransportAndOwnerCleanup(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	sentinel := errors.New("synthetic-close-failure")
	witness := &closeErrorConn{Conn: local, cause: sentinel}
	conn := &httpConn{Conn: witness}
	if !errors.Is(conn.Close(), sentinel) {
		t.Fatal("first transport close lost its cause")
	}
	session := &httpSession{transport: &http.Transport{}, cancel: func() {}, connections: []*httpConn{conn}}
	err := session.close()
	if !errors.Is(err, ErrCleanup) || !errors.Is(err, sentinel) || witness.closes.Load() != 1 {
		t.Fatal("owner cleanup erased or repeated the native close")
	}
}

func TestCallerHTTPTraceCannotObtainNativeConnections(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, goodLoad)
	}))
	defer server.Close()
	fixture := bindFixture(t, httpOptions(server), 1)
	var callbacks atomic.Int32
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		if info.Conn != nil {
			callbacks.Add(1)
		}
	}}
	ctx := httptrace.WithClientTrace(context.Background(), trace)
	receipt, err := fixture.client.StreamLoad(ctx, correlation("no-trace-escape"), testBatch())
	result := observe(t, receipt, err)
	if result.Err() != nil || callbacks.Load() != 0 {
		t.Fatal("caller callback crossed the native boundary", result.Err())
	}
	drain(t, fixture.inbox, 1)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if callbacks.Load() == 0 {
		t.Fatal("standard-library trace positive control did not run")
	}
}

func TestNativeContextIsolationPreservesCallerCancellationCause(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	fixture := bindFixture(t, httpOptions(server), 1)
	cause := errors.New("synthetic-owner-cancellation")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var callbacks atomic.Int32
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { callbacks.Add(1) }})
	done := make(chan invocationResult, 1)
	go func() {
		receipt, err := fixture.client.StreamLoad(ctx, correlation("isolated-cause"), testBatch())
		done <- invocationResult{receipt, err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not enter")
	}
	cancel(cause)
	select {
	case call := <-done:
		result := observe(t, call.receipt, call.err)
		if !errors.Is(result.Err(), cause) || !errors.Is(result.Err(), context.Canceled) || callbacks.Load() != 0 {
			t.Fatal("isolation lost cause or invoked callback")
		}
	case <-time.After(time.Second):
		t.Fatal("isolated caller cancellation did not release the operation")
	}
	drain(t, fixture.inbox, 1)
}

func TestDorisRedirectUserinfoMustMatchConfiguredCredentials(t *testing.T) {
	for _, password := range []string{"credential-canary", "untrusted-credential"} {
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			targetCalls.Add(1)
			user, received, _ := r.BasicAuth()
			if user != "synthetic" || received != "credential-canary" {
				t.Error("redirect supplied credentials replaced configuration")
			}
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, goodLoad)
		}))
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			location, _ := url.Parse(target.URL + r.URL.Path)
			location.User = url.UserPassword("synthetic", password)
			w.Header().Set("Location", location.String())
			w.WriteHeader(307)
		}))
		options := httpOptions(source)
		options.HTTPOrigins = append(options.HTTPOrigins, target.URL)
		fixture := bindFixture(t, options, 1)
		receipt, err := fixture.client.StreamLoad(context.Background(), correlation("userinfo"), testBatch())
		result := observe(t, receipt, err)
		if password == "credential-canary" {
			if result.Err() != nil || targetCalls.Load() != 1 {
				t.Fatal("verified native redirect rejected", result.Err())
			}
		} else if !errors.Is(result.Err(), ErrUnsupported) || targetCalls.Load() != 0 {
			t.Fatal("untrusted redirect credentials dispatched")
		}
		drain(t, fixture.inbox, 1)
		source.Close()
		target.Close()
	}
}
