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
	"errors"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
)

func TestCookieScopeNeverInfersDefaultOrSharedAuthority(t *testing.T) {
	for _, test := range []struct {
		name    string
		origin  browserContext
		want    cdp.BrowserContextID
		allowed bool
	}{
		{"owned", browserContext{browserID: "owned"}, "owned", true},
		{"missing-owned-id", browserContext{}, "", false},
		{"borrowed-without-grant", browserContext{browserID: "borrowed", borrowed: true}, "", false},
		{"borrowed-with-grant", browserContext{browserID: "borrowed", borrowed: true, sharedStorage: true}, "borrowed", true},
		{"missing-borrowed-id", browserContext{borrowed: true, sharedStorage: true}, "", false},
		{"unverified-default", browserContext{borrowed: true, sharedStorage: true, defaultContext: true}, "", false},
		{"explicit-verified-default", browserContext{borrowed: true, sharedStorage: true, defaultContext: true, defaultIdentityObserved: true}, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := unitSession(t)
			session.browserContext = &test.origin
			got, err := session.storageContext()
			if (err == nil) != test.allowed || got != test.want {
				t.Fatal("context scope changed", err)
			}
		})
	}
	if _, err := sessionOptions([]SessionOptionsV1{{SharedStorage: true}}, 1024); !errors.Is(err, ErrInput) {
		t.Fatal("borrow grant was accepted without borrowing", err)
	}
}

func TestCookiePreflightDoesNotReachNativeBrowser(t *testing.T) {
	for _, cookies := range [][]*network.CookieParam{nil, {nil}} {
		session := unitSession(t)
		session.browserID = "owned"
		retained := &Context{contextState: &contextState{session: session, life: session.work}}
		if err := retained.SetCookies(context.Background(), cookies); !errors.Is(err, ErrInput) {
			t.Fatal("malformed cookies reached the absent native browser", err)
		}
	}
	session := unitSession(t)
	session.borrowed = true
	session.browserID = "borrowed"
	retained := &Context{contextState: &contextState{session: session, life: session.work}}
	if _, err := retained.Cookies(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("ungranted read reached the absent native browser", err)
	}
	if err := retained.ClearCookies(context.Background()); !errors.Is(err, ErrState) || session.data.commands != 0 {
		t.Fatal("ungranted clear reached native command admission", err)
	}
}
