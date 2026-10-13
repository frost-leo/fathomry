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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/storage"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestLiveContextCookiesAreIsolatedAndEvidenceIsDetached(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	prepared, err := Prepare(Settings{Name: "cookies", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(2), MaxPages: new(1), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	first, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	input := &network.CookieParam{Name: "first", Value: "private-cookie-canary", URL: "http://cookie.invalid/", Path: "/"}
	if _, err := first.SetCookies(bounded(t), []*network.CookieParam{input}); err != nil {
		t.Fatal(err)
	}
	input.Value = "caller-changed"
	if _, err := second.SetCookies(bounded(t), []*network.CookieParam{{Name: "second", Value: "peer", URL: "http://cookie.invalid/", Path: "/"}}); err != nil {
		t.Fatal(err)
	}
	cookies, receipt, err := first.Cookies(bounded(t))
	if err != nil || len(cookies) != 1 || cookies[0].Name != "first" || cookies[0].Value != "private-cookie-canary" {
		t.Fatal("context cookies crossed a boundary or kept caller aliases", err)
	}
	cookies[0].Value = "returned-value-changed"
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := snapshot.ValueCopy()
	if !bytes.Contains(value.PayloadCopy(), []byte("private-cookie-canary")) || bytes.Contains([]byte(fmt.Sprintf("%+v", value)), []byte("private-cookie-canary")) {
		t.Fatal("cookie evidence was lost or disclosed by ordinary formatting")
	}
	if _, err := first.ClearCookies(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if cookies, _, err := first.Cookies(bounded(t)); err != nil || len(cookies) != 0 {
		t.Fatal("owned context cookies were not cleared", err)
	}
	if cookies, _, err := second.Cookies(bounded(t)); err != nil || len(cookies) != 1 || cookies[0].Name != "second" {
		t.Fatal("clear affected another context", err)
	}
	if err := first.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.Cookies(bounded(t)); !errors.Is(err, adapters.ErrClosed) {
		t.Fatal("closed context retained cookie authority", err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("context cookie operations prevented shutdown", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedCookiesRequireExplicitSharedGrantAndKeepPeerAlive(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	peerBrowser := sdk.FromContext(peer).Browser
	if _, err := cdp.Call(bounded(t), peerBrowser, storage.SetCookies, storage.SetCookiesParams{Cookies: []*network.CookieParam{{Name: "peer", Value: "retained", URL: "http://cookie.invalid/"}}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := retained.Cookies(bounded(t)); !errors.Is(err, ErrState) {
		t.Fatal("borrowed page silently acquired shared-cookie read access", err)
	}
	if _, err := retained.ClearCookies(bounded(t)); !errors.Is(err, ErrState) {
		t.Fatal("borrowed page silently acquired shared-cookie mutation access", err)
	}
	if result, err := cdp.Call(bounded(t), peerBrowser, storage.GetCookies, storage.GetCookiesParams{}); err != nil || len(result.Cookies) != 1 || result.Cookies[0].Name != "peer" {
		t.Fatal("refused clear affected the independent peer", err)
	}
	_ = retained.Close(bounded(t))
	granted, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(sdk.FromContext(peer).Target.TargetID), SharedStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	if cookies, _, err := granted.Cookies(bounded(t)); err != nil || len(cookies) != 1 || cookies[0].Name != "peer" {
		t.Fatal("explicit shared/default storage grant did not work", err)
	}
	if _, err := granted.ClearCookies(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if result, err := cdp.Call(bounded(t), peerBrowser, storage.GetCookies, storage.GetCookiesParams{}); err != nil || len(result.Cookies) != 0 {
		t.Fatal("shared clear did not have its explicitly granted context-wide effect", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("borrowed source did not shut down")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("17")); err != nil || value != 17 {
		t.Fatal("cookie cleanup destroyed the independent peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
