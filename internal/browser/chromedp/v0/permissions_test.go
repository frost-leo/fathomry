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

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
)

func TestPermissionScopeDoesNotInheritStorageOrDefaultAuthority(t *testing.T) {
	for _, test := range []struct {
		name    string
		origin  browserContext
		want    cdp.BrowserContextID
		allowed bool
	}{
		{"owned", browserContext{browserID: "owned"}, "owned", true},
		{"missing-owned", browserContext{}, "", false},
		{"borrowed", browserContext{browserID: "borrowed", borrowed: true}, "", false},
		{"storage-grant-only", browserContext{browserID: "borrowed", borrowed: true, sharedStorage: true}, "", false},
		{"permission-grant", browserContext{browserID: "borrowed", borrowed: true, sharedPermissions: true}, "borrowed", true},
		{"missing-borrowed", browserContext{borrowed: true, sharedPermissions: true}, "", false},
		{"unverified-default", browserContext{borrowed: true, sharedPermissions: true, defaultContext: true}, "", false},
		{"verified-default", browserContext{borrowed: true, sharedPermissions: true, defaultContext: true, defaultIdentityObserved: true}, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := unitSession(t)
			session.browserContext = &test.origin
			got, err := session.permissionContext()
			if (err == nil) != test.allowed || got != test.want {
				t.Fatal("permission authority or identity changed", err)
			}
		})
	}
	if _, err := sessionOptions([]SessionOptionsV1{{SharedPermissions: true}}, 1024); !errors.Is(err, ErrInput) {
		t.Fatal("permission grant without explicit borrow was accepted", err)
	}
}

func TestPermissionMalformedAndForeignInputNeverReachesNativeBrowser(t *testing.T) {
	for _, params := range []browser.SetPermissionParams{
		{},
		{Permission: &browser.PermissionDescriptor{Name: "geolocation"}, Setting: browser.PermissionSettingGranted, BrowserContextID: "foreign"},
	} {
		session := unitSession(t)
		session.browserID = "owned"
		retained := &Context{contextState: &contextState{session: session, life: session.work}}
		if err := retained.SetPermission(context.Background(), params); !errors.Is(err, ErrInput) {
			t.Fatal("invalid parameters reached the absent native browser", err)
		}
	}
	session := unitSession(t)
	session.browserID, session.borrowed = "borrowed", true
	retained := &Context{contextState: &contextState{session: session, life: session.work}}
	if err := retained.SetPermission(context.Background(), browser.SetPermissionParams{}); !errors.Is(err, ErrState) {
		t.Fatal("borrowed permission input was prepared without authority", err)
	}
	if err := retained.ResetPermissions(context.Background()); !errors.Is(err, ErrState) || session.data.commands != 0 {
		t.Fatal("borrowed permission reset reached native admission", err)
	}
}
