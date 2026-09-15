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

package tls_client

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/quic-go-utls/http3"
)

func TestFathomryControlReservesBeforeNativeDial(t *testing.T) {
	state := newCompatibilityState()
	sentinel := errors.New("synthetic capacity")
	var entered atomic.Int64
	state.control.AcquireTCP = func() (func(), error) { return nil, sentinel }
	if _, err := state.dial(context.Background(), "tcp", "unused.invalid", func(context.Context, string, string) (net.Conn, error) { entered.Add(1); return nil, nil }); !errors.Is(err, sentinel) || entered.Load() != 0 {
		t.Fatal("native dial bypassed acquisition")
	}
	var held atomic.Int64
	state.control.AcquireTCP = func() (func(), error) { held.Add(1); return func() { held.Add(-1) }, nil }
	local, remote := net.Pipe()
	defer remote.Close()
	conn, err := state.dial(context.Background(), "tcp", "unused.invalid", func(context.Context, string, string) (net.Conn, error) { return local, nil })
	if err != nil || held.Load() != 1 {
		t.Fatal("native connection did not retain ticket", err)
	}
	if err := conn.Close(); err != nil || held.Load() != 0 {
		t.Fatal("confirmed connection Close did not release ticket", err)
	}
	if err := conn.Close(); err != nil || held.Load() != 0 {
		t.Fatal("duplicate close released twice", err)
	}
	if _, err := state.dial(context.Background(), "tcp", "unused.invalid", func(context.Context, string, string) (net.Conn, error) { return nil, sentinel }); !errors.Is(err, sentinel) || held.Load() != 0 {
		t.Fatal("failed dial retained an unused reservation", err)
	}
}
func TestFathomryControlH3RegistryAndQuiescence(t *testing.T) {
	state := newCompatibilityState()
	rt := &roundTripper{compat: state, cachedTransports: make(map[string]http.RoundTripper), cachedConnections: make(map[string]net.Conn)}
	native := &httpClient{Client: http.Client{Transport: rt}}
	var held atomic.Int64
	control := FathomryControlV1{AcquireHTTP3: func() (func(), error) { held.Add(1); return func() { held.Add(-1) }, nil }}
	if err := ConfigureFathomry(native, control); err != nil {
		t.Fatal(err)
	}
	if err := ConfigureFathomry(native, control); err == nil {
		t.Fatal("live controls could be replaced")
	}
	transport := &http3.Transport{}
	if err := state.registerH3(transport); err != nil {
		t.Fatal(err)
	}
	if held.Load() != 1 || FathomryQuiescent(native) {
		t.Fatal("unclosed transport certified released")
	}
	if err := Close(native); err != nil {
		t.Fatal(err)
	}
	if held.Load() != 0 || !FathomryQuiescent(native) {
		t.Fatal("native H3 registry not released")
	}
	if err := Close(native); err != nil || held.Load() != 0 {
		t.Fatal("duplicate shutdown released twice")
	}
}

type closeFailureBody struct {
	io.Reader
	cause error
}

func (body closeFailureBody) Close() error { return body.cause }
func TestFathomryWinningBodyRetainsLosingCleanupError(t *testing.T) {
	sentinel := errors.New("synthetic losing cleanup")
	response := func(body io.ReadCloser) compatTestTransport {
		return func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: body}, nil }
	}
	// The losing upload is closed even when the delayed TCP leg never sends.
	racer, rt, ctx, done := compatTestRacer(t, context.Background(), response(io.NopCloser(strings.NewReader("winner"))), response(io.NopCloser(strings.NewReader("loser"))))
	request, _ := http.NewRequestWithContext(ctx, "POST", "https://peer.invalid/", strings.NewReader("upload"))
	request.GetBody = func() (io.ReadCloser, error) { return closeFailureBody{strings.NewReader("upload"), sentinel}, nil }
	result, err := racer.race(request, "peer.invalid:443", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, result.Body)
	if err := result.Body.Close(); !errors.Is(err, sentinel) {
		t.Fatal("losing cleanup error absent from winner close", err)
	}
	done()
	if err := rt.compat.close(rt); !errors.Is(err, sentinel) {
		t.Fatal("source cleanup history erased", err)
	}
}
