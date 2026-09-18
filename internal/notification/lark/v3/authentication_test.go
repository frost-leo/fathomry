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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func TestConcurrentTokenAcquisitionAndSourceIsolation(t *testing.T) {
	peer := newPeer(t, nil)
	var bounds []*bound
	for _, app := range []string{"app_one", "app_two"} {
		options := testOptions(peer)
		options.AppID = app
		options.AppSecret = "secret-" + app
		options.MaxActive = 4
		options.QueuedCalls = 4
		bounds = append(bounds, bindTest(t, options, 8))
	}
	var workers sync.WaitGroup
	for _, bound := range bounds {
		for index := range 8 {
			workers.Go(func() {
				call := "call-" + strconv.Itoa(index)
				receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: call}, Recipient{Type: "open_id", ID: "ou_fixture"}, call, textContent(t))
				if err != nil {
					t.Error(err)
					return
				}
				result, err := receipt.WaitReleased(context.Background())
				if err != nil || result.Err() != nil {
					t.Error("concurrent source call failed")
				}
			})
		}
	}
	workers.Wait()
	authCount := 0
	tokens := map[string]int{}
	for _, request := range peer.snapshot() {
		if strings.Contains(request.path, "/auth/") {
			authCount++
		} else {
			tokens[request.auth]++
		}
	}
	if authCount != 2 || tokens["Bearer token-app_one"] != 8 || tokens["Bearer token-app_two"] != 8 {
		t.Fatal("tokens or refresh traffic crossed source boundaries")
	}
}
func TestRefreshExpiryAndInvalidationWithoutResend(t *testing.T) {
	count := 0
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/auth/") {
			count++
			if count == 1 {
				_, _ = io.WriteString(w, `{"code":99991663,"msg":"expired"}`)
				return
			}
		}
		defaultReply(w, r)
	})
	bound := bindTest(t, testOptions(peer), 2)
	first := sendTest(t, bound.client, "invalid")
	if !errors.Is(first.Err(), ErrAPI) || len(peer.snapshot()) != 2 {
		t.Fatal("invalid token caused inline replay")
	}
	second := sendTest(t, bound.client, "new-call")
	if second.Err() != nil || len(peer.snapshot()) != 4 {
		t.Fatal("next explicit call did not reacquire invalid token")
	}
	// No active calls: force only this source's cache expiry to inspect refresh.
	bound.client.owner.expires = time.Now().Add(-time.Second)
	drain(t, bound.inbox)
	if third := sendTest(t, bound.client, "expired-cache"); third.Err() != nil || len(peer.snapshot()) != 6 {
		t.Fatal("expired cache did not refresh")
	}
}
func TestMalformedAuthenticationNeverSends(t *testing.T) {
	for _, body := range []string{`{"code":0}`, `{"code":0,"tenant_access_token":"","expire":7200}`, `{"code":0,"tenant_access_token":"token","expire":0}`, `{"code":0,"tenant_access_token":"token","expire":999999999999}`, `{"code":10003,"msg":"secret"}`, `{"tenant_access_token":"token","expire":7200}`} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) })
		bound := bindTest(t, testOptions(peer), 1)
		got := sendTest(t, bound.client, "auth")
		if !errors.Is(got.Err(), ErrAuth) || got.Outcome.Value.Effect() != NotAttempted || len(peer.snapshot()) != 1 {
			t.Fatal("invalid token response authorized send")
		}
	}
}
func TestTokenWaitHonorsContext(t *testing.T) {
	peer := newPeer(t, nil)
	bound := bindTest(t, testOptions(peer), 1)
	owned := bound.client.owner
	owned.tokenLock <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	receipt, err := bound.client.Send(ctx, fault.Correlation{Call: "waiting"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "waiting", textContent(t))
	<-owned.tokenLock
	got := resolved(t, receipt, err)
	if !errors.Is(got.Err(), context.DeadlineExceeded) || got.Attempts != (invocation.Attempts{Exact: true}) || len(peer.snapshot()) != 0 {
		t.Fatal("cache wait escaped cancellation")
	}
}
