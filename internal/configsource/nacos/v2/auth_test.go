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
	"errors"
	"github.com/frost-leo/fathomry/internal/conformance"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestPasswordAuthenticationRefreshAndDenial(t *testing.T) {
	fixture := newFixture(t, false)
	fixture.mu.Lock()
	fixture.token = "first-token-canary"
	fixture.mu.Unlock()
	input := fixture.options()
	input.Username, input.Password = "reader", "credential-canary"
	input.QueuedRequests = 8
	client := openClient(t, input)
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			if _, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if fixture.loginCount.Load() != 1 {
		t.Fatal("concurrent callers amplified login")
	}
	fixture.mu.Lock()
	fixture.token = "rotated-token-canary"
	fixture.mu.Unlock()
	if _, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); !errors.Is(err, ErrDenied) {
		t.Fatal("token revocation became stale success", err)
	}
	if _, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); err != nil {
		t.Fatal("denial did not invalidate cached token", err)
	}
	if fixture.loginCount.Load() != 2 {
		t.Fatal("revoked token was not refreshed")
	}
	bad := input
	bad.Name = "denied"
	bad.Password = "bad-secret-canary"
	if _, err := openClient(t, bad).Read(context.Background(), KeyV1{DataID: "settings.yaml"}); !errors.Is(err, ErrDenied) {
		t.Fatal("bad password accepted", err)
	} else {
		conformance.Private(t, err, "bad-secret-canary")
	}
}
func TestAuthenticationRejectsMalformedAndRedirectedResponses(t *testing.T) {
	for _, body := range []string{`{"accessToken":"token","tokenTtl":0}`, `{"accessToken":"token","tokenTtl":1.5}`, `{"accessToken":"token","tokenTtl":2,"tokenTtl":3}`, `{}`} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte(body)) }))
		input := options()
		input.Servers[0].HTTPURL = server.URL
		input.Username, input.Password = "reader", "credential-canary"
		client := openClient(t, input)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if _, err := client.token(ctx, 0); !errors.Is(err, ErrDecode) {
			t.Fatal("invalid login accepted", err)
		}
		cancel()
		server.Close()
	}
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("authentication followed a redirect") }))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, destination.URL, 302)
	}))
	defer server.Close()
	input := options()
	input.Servers[0].HTTPURL = server.URL
	input.Username, input.Password = "reader", "credential-canary"
	if _, err := openClient(t, input).token(context.Background(), 0); err == nil {
		t.Fatal("redirect treated as successful login")
	}
}

func TestDeniedTokenInvalidationDoesNotWaitForAnotherLogin(t *testing.T) {
	client := openClient(t, newFixture(t, false).options())
	client.mu.Lock()
	client.tokens[0] = token{"rejected-token", time.Now().Add(time.Hour)}
	client.mu.Unlock()
	client.authGate <- struct{}{}
	client.forgetToken(0, "rejected-token")
	<-client.authGate
	client.mu.Lock()
	if client.tokens[0].value != "" {
		t.Error("another login hid token revocation")
	}
	client.tokens[0] = token{"new-token", time.Now().Add(time.Hour)}
	client.mu.Unlock()
	client.forgetToken(0, "rejected-token")
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.tokens[0].value != "new-token" {
		t.Error("late denial invalidated a newer token")
	}
}
