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
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/chromedp/cdproto/browser"
	sdk "github.com/chromedp/chromedp"
)

func permissionFixture(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><p>isolated permission fixture</p></html>"))
	}))
	t.Cleanup(server.Close)
	return server
}

const queryLocationPermission = "navigator.permissions.query({name:'geolocation'}).then(p => p.state)"

func TestLivePermissionsRemainContextScoped(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	server := permissionFixture(t)
	prepared, err := Prepare(Settings{Name: "permissions", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(2), MaxPages: new(1), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
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
	for _, retained := range []*Context{first, second} {
		if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
			t.Fatal(err)
		}
	}
	initial, _, err := RunAction(bounded(t), first.Page(), Evaluate[string](queryLocationPermission, EvalAwaitPromise()))
	if err != nil {
		t.Fatal(err)
	}
	input := browser.SetPermissionParams{Permission: &browser.PermissionDescriptor{Name: "geolocation"}, Setting: browser.PermissionSettingGranted, Origin: server.URL}
	if _, err := first.SetPermission(bounded(t), input); err != nil {
		t.Fatal(err)
	}
	if input.BrowserContextID != "" || input.Permission.Name != "geolocation" {
		t.Fatal("native permission preparation modified caller parameters")
	}
	input.Setting = browser.PermissionSettingDenied
	if _, err := second.SetPermission(bounded(t), input); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		retained *Context
		want     string
	}{{first, "granted"}, {second, "denied"}} {
		value, receipt, err := RunAction(bounded(t), test.retained.Page(), Evaluate[string](queryLocationPermission, EvalAwaitPromise()))
		if err != nil || value != test.want || receipt == nil {
			t.Fatal("permission overrides crossed context identity", err)
		}
	}
	if _, err := first.ResetPermissions(bounded(t)); err != nil {
		t.Fatal(err)
	}
	restored, _, err := RunAction(bounded(t), first.Page(), Evaluate[string](queryLocationPermission, EvalAwaitPromise()))
	if err != nil || restored != initial {
		t.Fatal("context reset did not restore its native permission baseline", err)
	}
	peer, _, err := RunAction(bounded(t), second.Page(), Evaluate[string](queryLocationPermission, EvalAwaitPromise()))
	if err != nil || peer != "denied" {
		t.Fatal("permission reset affected the independent context", err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("permission commands prevented context cleanup", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedPermissionsNeedGrantAndDetachPreservesPeer(t *testing.T) {
	server := permissionFixture(t)
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := sdk.Do(peer, sdk.Navigate(server.URL)); err != nil {
		t.Fatal(err)
	}
	baseline, err := sdk.Run(peer, sdk.Evaluate[string](queryLocationPermission, sdk.EvalAwaitPromise))
	if err != nil {
		t.Fatal(err)
	}
	input := browser.SetPermissionParams{Permission: &browser.PermissionDescriptor{Name: "geolocation"}, Setting: browser.PermissionSettingGranted, Origin: server.URL}
	if receipt, err := retained.SetPermission(bounded(t), input); !errors.Is(err, ErrState) || receipt == nil {
		t.Fatal("borrowed page obtained context-wide permission authority", err)
	}
	if _, err := retained.ResetPermissions(bounded(t)); !errors.Is(err, ErrState) {
		t.Fatal("borrowed page could reset its peer's permissions", err)
	}
	_ = retained.Close(bounded(t))
	granted, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(sdk.FromContext(peer).Target.TargetID), SharedPermissions: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := granted.SetPermission(bounded(t), input); err != nil {
		t.Fatal("explicit shared permission grant did not work", err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[string](queryLocationPermission, sdk.EvalAwaitPromise)); err != nil || value != "granted" {
		t.Fatal("granted permission was not visible to the independent peer", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("borrowed permission source did not detach")
	}
	value, err := sdk.Run(peer, sdk.Evaluate[string](queryLocationPermission, sdk.EvalAwaitPromise))
	if err != nil || value != baseline {
		t.Fatal("peer was lost or native session cleanup did not reset permission overrides", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
