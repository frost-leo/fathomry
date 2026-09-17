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

package mail

import (
	"context"
	"testing"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOptionsFrozenAndResolvedLimits(t *testing.T) {
	peer := newPeer(t, peerOptions{})
	options := testOptions(peer)
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	options.Host = "changed.invalid"
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "frozen", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	inbox, _ := invocation.NewInbox[Result](1, defaults(options).evidenceReservation())
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := sendTest(t, client, "frozen", plainMessage(t, "frozen")); got.Err() != nil {
		t.Fatal("selection did not freeze options")
	}
	drain(t, inbox)
	limits := LimitsV1(testOptions(peer))
	limits.Bytes = 1
	insufficient, err := Select(testOptions(peer))
	if err != nil {
		t.Fatal(err)
	}
	insufficient = resource.WithLimits(insufficient, limits)
	other, err := resource.Assemble(context.Background(), context.Background(), "insufficient", insufficient)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(context.Background())
	if _, err = Bind(other, insufficient, inbox, nil); err == nil {
		t.Fatal("insufficient byte envelope accepted")
	}
}
