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

package chromedp_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
)

func TestProfileIsOfflineDetachedAndHonestAboutUnknowns(t *testing.T) {
	prepared, err := browser.Prepare(browser.Settings{Name: "profile", ExecPath: "/not-started/private-path", MaxSubscriptions: new(3)}, browser.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := publicDependencies(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(context.Background()) })
	profile, err := owner.Client().Profile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if profile.ServiceMode.Kind != "declared" || profile.ServiceMode.Value != "owned-process" || profile.ServiceVersion.Kind != "" || profile.Protocol.Kind != "" || profile.Native.Kind != "" {
		t.Fatal("offline profile invented observed support")
	}
	found := false
	for _, option := range profile.Options {
		if option.Name == "max-subscriptions" {
			found = option.Value == "3"
		}
		if strings.Contains(option.Value, "private-path") {
			t.Fatal("profile exposed local path")
		}
	}
	if !found {
		t.Fatal("effective queue policy absent")
	}
	copy := profile.Clone()
	copy.Options[0].Value = "changed"
	again, err := owner.Client().Profile(context.Background())
	if err != nil || again.Options[0].Value == "changed" || profile.Options[0].Value == "changed" {
		t.Fatal("profile aliases mutable containers", err)
	}
	if strings.Contains(fmt.Sprintf("%+v", profile), "owned-process") {
		t.Fatal("ordinary formatting exposes profile data")
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Client().Profile(context.Background()); !errors.Is(err, browser.ErrState) {
		t.Fatal("closed source inspection revived", err)
	}
}
