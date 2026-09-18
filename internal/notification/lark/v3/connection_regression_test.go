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

package lark

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestNativeHTTPDialOutlivesCanceledRequest(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	finished := make(chan struct{})
	transport := &http.Transport{DisableKeepAlives: true, DialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		entered <- ctx
		<-release
		close(finished)
		return nil, context.Canceled
	}}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, "GET", "https://fixture.invalid", nil)
	returned := make(chan error, 1)
	go func() { _, err := transport.RoundTrip(request); returned <- err }()
	dialContext := <-entered
	cancel()
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatal("control request did not cancel")
	}
	if dialContext.Err() != nil {
		t.Fatal("native detached-dial premise changed; requalify implementation")
	}
	select {
	case <-finished:
		t.Fatal("native callback unexpectedly joined")
	default:
	}
	close(release)
	<-finished
}
func TestCanceledTLSHandshakeClosesSocketBeforeRelease(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			close(accepted)
			close(closed)
			return
		}
		defer conn.Close()
		close(accepted)
		_, _ = io.Copy(io.Discard, conn)
		close(closed)
	}()
	peer := newPeer(t, nil)
	options := testOptions(peer)
	options.BaseURL = "https://" + listener.Addr().String()
	bound := bindTest(t, options, 1)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("handshake canceled")
	go func() { <-accepted; cancel(cause) }()
	receipt, err := bound.client.Send(ctx, fault.Correlation{Call: "handshake"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "handshake", textContent(t))
	got := resolved(t, receipt, err)
	if !errors.Is(got.Err(), cause) || got.Outcome.Value.Effect() != NotAttempted {
		t.Fatal("handshake failure lost cause or invented send")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("released call left a live TLS socket")
	}
	if err := bound.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
