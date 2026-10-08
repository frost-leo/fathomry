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

package surf

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/enetx/surf/profiles/chrome"
	utls "github.com/refraction-networking/utls"
)

func TestPublicQPACKPreparationAndReplacementBudget(t *testing.T) {
	var effects atomic.Int64
	native := NativeOptions{ListenPacket: func(context.Context, string, string) (net.PacketConn, error) {
		effects.Add(1)
		return nil, errors.New("unexpected packet acquisition")
	}}
	settings := Settings{Name: "qpack-base", Mode: pointer(PreferHTTP3), MaxActive: pointer(1), MaxHTTP3Clients: pointer(1), MaxHTTP3QPACKTableBytes: pointer(int64(65536)), MaxHTTP3QPACKBlockedStreams: pointer(128)}
	base, err := Prepare(settings, native)
	if err != nil {
		t.Fatal(err)
	}
	basePolicy, err := base.Policy()
	if err != nil {
		t.Fatal(err)
	}
	settings.Name = "qpack-larger"
	settings.MaxHTTP3QPACKTableBytes = pointer(int64(65537))
	larger, err := Prepare(settings, native)
	if err != nil {
		t.Fatal(err)
	}
	largerPolicy, err := larger.Policy()
	if err != nil {
		t.Fatal(err)
	}
	combined, err := Compose(base, larger)
	if err != nil || combined.SourceWorkBytes != basePolicy.SourceWorkBytes+largerPolicy.SourceWorkBytes || combined.Budget != basePolicy.Budget {
		t.Fatal("overlap did not retain both exact source declarations", err)
	}
	dependencies := testMechanisms(t, base)
	owner, err := base.Open(testContext(t), dependencies)
	if err != nil {
		t.Fatal("valid base policy refused", err)
	}
	t.Cleanup(func() { _ = owner.Close(testContext(t)) })
	before, _ := dependencies.Runtime.Inspect()
	if replacement, err := larger.Open(testContext(t), dependencies); replacement != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("larger QPACK source enlarged existing Runtime", err)
	}
	after, _ := dependencies.Runtime.Inspect()
	if after.Accepted != before.Accepted || effects.Load() != 0 {
		t.Fatal("insufficient replacement caused native effects or retained work")
	}
	settings.MaxHTTP3Clients = pointer(1024)
	settings.MaxHTTP3QPACKTableBytes = pointer(int64(64 << 20))
	if _, err := Prepare(settings, native); !errors.Is(err, ErrLimit) || effects.Load() != 0 {
		t.Fatal("unrepresentable aggregate QPACK source was admitted", err)
	}
}

func TestPublicQPACKZeroIsFrozenNativeAuthority(t *testing.T) {
	var callbacks, effects atomic.Int64
	profile := chrome.Desktop
	profile.HelloSpec, profile.HelloID, profile.ShuffleExtensions = nil, utls.ClientHelloID{}, false
	profile.ConfigureH3 = func(settings profiles.H3Config) {
		callbacks.Add(1)
		settings.QpackMaxTableCapacity(1).QpackBlockedStreams(1)
	}
	table, blocked := int64(0), 0
	prepared, err := Prepare(Settings{Name: "frozen-zero", Mode: pointer(PreferHTTP3), MaxHTTP3QPACKTableBytes: &table, MaxHTTP3QPACKBlockedStreams: &blocked}, NativeOptions{
		Profile: &profile,
		ListenPacket: func(context.Context, string, string) (net.PacketConn, error) {
			effects.Add(1)
			return nil, errors.New("unexpected packet acquisition")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	table, blocked = 65536, 128
	dependencies := testMechanisms(t, prepared)
	owner, err := prepared.Open(testContext(t), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(testContext(t)) })
	selection, err := owner.Client().Profile(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	actual := make(map[string]string)
	for _, field := range selection.Options {
		actual[field.Name] = field.Value
	}
	if actual["declared-qpack-table-bytes"] != "0" || actual["declared-qpack-blocked-streams"] != "0" || callbacks.Load() != 0 || effects.Load() != 0 {
		t.Fatal("explicit zero was defaulted, aliased, or evaluated offline", actual)
	}
	input, _ := http.NewRequest("GET", "https://127.0.0.1:1/", nil)
	receipt, direct := owner.Client().Do(testContext(t), testContext(t), input)
	value, final := reviewPublicSettled(t, dependencies, receipt)
	if !errors.Is(direct, ErrLimit) || !errors.Is(direct, sdk.ErrFathomryProfileLimit) || !errors.Is(final, sdk.ErrFathomryProfileLimit) || value.Complete() || callbacks.Load() != 1 || effects.Load() != 0 {
		t.Fatal("public zero ceiling did not refuse lazy native nonzero before effects", direct, final, effects.Load())
	}
}
