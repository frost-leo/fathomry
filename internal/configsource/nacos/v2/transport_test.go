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

package nacos

import (
	"context"
	"crypto/x509"
	"errors"
	"net/http/httptrace"
	"sync"
	"testing"
	"time"
)

func TestHTTPAuthenticationTLSIsVerifiedIndependently(t *testing.T) {
	fixture := newFixture(t, true)
	input := fixture.options()
	input.Username, input.Password = "reader", "credential-canary"
	client := openClient(t, input)
	if _, err := client.token(context.Background(), 0); err != nil {
		t.Fatal("trusted HTTP login failed", err)
	}
	input.RootCAPEM = ""
	_, err := openClient(t, input).token(context.Background(), 0)
	var native x509.UnknownAuthorityError
	if !errors.As(err, &native) {
		t.Fatal("HTTP authentication did not retain TLS refusal", err)
	}
}
func TestNativeDialRemainsOwnedAfterCallerCancellation(t *testing.T) {
	fixture := newFixture(t, false)
	client := openClient(t, fixture.options())
	entered, unblock := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(unblock) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{ConnectStart: func(string, string) { close(entered); <-unblock }})
	result := make(chan error, 1)
	go func() {
		socket, err := client.dial(ctx, fixture.listener.Addr().String())
		if socket != nil {
			_ = socket.Close()
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("owned native dial not entered")
	}
	cancel()
	closeCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if err := client.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) || !client.assembly.Snapshot().Sources[0].Pending {
		t.Fatal("unfinished dial was called released", err)
	}
	release.Do(func() { close(unblock) })
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("unblocked dial did not exit")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestRegistrationFailurePreservesLastNativeCause(t *testing.T) {
	fixture := newFixture(t, false)
	fixture.mu.Lock()
	fixture.unregistered = true
	fixture.mu.Unlock()
	_, err := openClient(t, fixture.options()).Read(context.Background(), KeyV1{DataID: "settings.yaml"})
	var native *RemoteError
	if !errors.As(err, &native) || native.ErrorCode() != 301 {
		t.Fatal("exhausted registration lost last native cause", err)
	}
}
