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
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"testing"
	"time"
)

func TestOptionsValidationAndFrozenOverlay(t *testing.T) {
	base := OptionsV1{Name: "test", Profile: "application", AppID: "app_fixture", AppSecret: "secret"}
	for _, change := range []func(*OptionsV1){
		func(v *OptionsV1) { v.Version = 99 }, func(v *OptionsV1) { v.Name = "" }, func(v *OptionsV1) { v.Profile = "marketplace" },
		func(v *OptionsV1) { v.BaseURL = "http://example.test" }, func(v *OptionsV1) { v.BaseURL = "https://example.test/path" },
		func(v *OptionsV1) { v.AppSecret = "" }, func(v *OptionsV1) { v.TenantToken = "secret" }, func(v *OptionsV1) { v.RootCAPEM = "not a certificate" },
		func(v *OptionsV1) { v.MaxActive = 17 }, func(v *OptionsV1) { v.QueuedCalls = -1 }, func(v *OptionsV1) { v.MaxRequestBytes = 1 },
		func(v *OptionsV1) { v.Timeout = -time.Second }, func(v *OptionsV1) { v.EncryptKey = "without-token" },
		func(v *OptionsV1) { v.Profile = "tenant-token"; v.AppSecret = ""; v.TenantToken = "token" },
	} {
		input := base
		change(&input)
		if _, err := Select(input); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	peer := newPeer(t, nil)
	options := testOptions(peer)
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	options.AppSecret = "mutated"
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "freeze", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	inbox, _ := invocation.NewInbox[Result](1, EvidenceBytesV1(options))
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := sendTest(t, client, "frozen"); got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, inbox)
	if len(peer.snapshot()) != 2 {
		t.Fatal("construction performed I/O or send failed")
	}
	if _, err = Bind(assembly, selected, nil, nil); !errors.Is(err, ErrInput) {
		t.Fatal("missing evidence accepted")
	}
}
func TestUnsupportedProfilesAndExpiredToken(t *testing.T) {
	peer := newPeer(t, nil)
	options := testOptions(peer)
	options.Profile = "tenant-token"
	options.AppSecret = ""
	options.TenantKey = "tenant_fixture"
	options.TenantToken = "manual"
	options.TokenExpiresAt = time.Now().Add(-time.Minute)
	bound := bindTest(t, options, 1)
	got := sendTest(t, bound.client, "expired")
	if !errors.Is(got.Err(), ErrAuth) || got.Outcome.Value.Effect() != NotAttempted || len(peer.snapshot()) != 0 {
		t.Fatal("expired token reached transport")
	}
}
