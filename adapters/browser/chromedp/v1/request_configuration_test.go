/*
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

package chromedp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
)

type pageRequestData struct {
	Header    string `json:"header"`
	HasEmpty  bool   `json:"hasEmpty"`
	UserAgent string `json:"userAgent"`
	Language  string `json:"language"`
	Cookie    string `json:"cookie"`
}

func requestConfigurationServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hasEmpty := r.Header["X-Empty"]
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte("<!doctype html><script>window.fixtureRequest="))
		_ = json.NewEncoder(w).Encode(pageRequestData{Header: r.Header.Get("X-Fixture"), HasEmpty: hasEmpty, UserAgent: r.UserAgent(), Language: r.Header.Get("Accept-Language"), Cookie: r.Header.Get("Cookie")})
		_, _ = w.Write([]byte("</script><p>local request fixture</p>"))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestLivePageRequestConfigurationAndResetArePageScoped(t *testing.T) {
	server := requestConfigurationServer(t)
	owner, retained, dependencies := controlFixture(t, server.URL, 128)
	page := retained.Page()
	baseline := pageActionValue(t, page, dependencies, Evaluate[pageRequestData]("window.fixtureRequest"))
	sibling, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, err := retained.SetCookies(bounded(t), []*network.CookieParam{{Name: "fixture", Value: "local", URL: server.URL}}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	headers := network.SetExtraHTTPHeadersParams{Headers: network.Headers{"X-Fixture": "page-value", "X-Empty": ""}}
	pageActionValue(t, page, dependencies, Command(network.SetExtraHTTPHeaders, headers))
	headers.Headers["X-Fixture"] = "caller-changed"
	pageActionValue(t, page, dependencies, Command(emulation.SetUserAgentOverride, emulation.SetUserAgentOverrideParams{UserAgent: "LocalFixture/1", AcceptLanguage: "fr-CA", Platform: "LocalFixture"}))
	pageActionValue(t, page, dependencies, NavigateResponse(server.URL+"/configured"))
	configured := pageActionValue(t, page, dependencies, Evaluate[pageRequestData]("window.fixtureRequest"))
	if configured.Header != "page-value" || !configured.HasEmpty || configured.UserAgent != "LocalFixture/1" || !strings.HasPrefix(configured.Language, "fr-CA") || configured.Cookie != "fixture=local" {
		t.Fatalf("native request configuration changed: %+v", configured)
	}
	if !pageActionValue(t, page, dependencies, Evaluate[bool](`navigator.userAgent==='LocalFixture/1' && navigator.platform==='LocalFixture' && navigator.language==='fr-CA'`)) {
		t.Fatal("native navigator overrides were lost")
	}
	pageActionValue(t, sibling, dependencies, NavigateResponse(server.URL+"/sibling"))
	peer := pageActionValue(t, sibling, dependencies, Evaluate[pageRequestData]("window.fixtureRequest"))
	if peer.Header != "" || peer.HasEmpty || peer.UserAgent != baseline.UserAgent || peer.Language != baseline.Language || peer.Cookie != "fixture=local" {
		t.Fatalf("page settings crossed into a sibling or context cookie was lost: %+v", peer)
	}
	pageActionValue(t, page, dependencies, Command(network.SetExtraHTTPHeaders, network.SetExtraHTTPHeadersParams{Headers: network.Headers{}}))
	pageActionValue(t, page, dependencies, Command(emulation.SetUserAgentOverride, emulation.SetUserAgentOverrideParams{}))
	pageActionValue(t, page, dependencies, NavigateResponse(server.URL+"/reset"))
	reset := pageActionValue(t, page, dependencies, Evaluate[pageRequestData]("window.fixtureRequest"))
	if reset.Header != "" || reset.HasEmpty || reset.UserAgent != baseline.UserAgent || reset.Language != baseline.Language || reset.Cookie != "fixture=local" {
		t.Fatalf("native reset retained page overrides or cleared context cookies: %+v", reset)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("request configuration prevented shutdown", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedPageRequestConfigurationDetachesWithoutClosingPeer(t *testing.T) {
	server := requestConfigurationServer(t)
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	peerWork, stopPeerWork := context.WithTimeout(peer, 10*time.Second)
	defer stopPeerWork()
	baseline, err := sdk.Run(peerWork, sdk.Evaluate[string]("navigator.userAgent"))
	if err != nil {
		t.Fatal(err)
	}
	pageActionValue(t, retained.Page(), dependencies, Command(network.SetExtraHTTPHeaders, network.SetExtraHTTPHeadersParams{Headers: network.Headers{"X-Fixture": "borrowed-page"}}))
	pageActionValue(t, retained.Page(), dependencies, Command(emulation.SetUserAgentOverride, emulation.SetUserAgentOverrideParams{UserAgent: "BorrowedFixture/1"}))
	if err := sdk.Do(peerWork, sdk.Navigate(server.URL+"/attached")); err != nil {
		t.Fatal(err)
	}
	configured, err := sdk.Run(peerWork, sdk.Evaluate[pageRequestData]("window.fixtureRequest"))
	if err != nil || configured.Header != "borrowed-page" || configured.UserAgent != "BorrowedFixture/1" {
		t.Fatal("shared borrowed page did not observe its active overrides", err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("borrowed request configuration prevented detach", err)
	}
	if err := sdk.Do(peerWork, sdk.Navigate(server.URL+"/detached")); err != nil {
		t.Fatal("borrowed peer was closed", err)
	}
	after, err := sdk.Run(peerWork, sdk.Evaluate[pageRequestData]("window.fixtureRequest"))
	if err != nil || after.Header != "" || after.UserAgent != baseline {
		t.Fatal("detached session retained request overrides", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
