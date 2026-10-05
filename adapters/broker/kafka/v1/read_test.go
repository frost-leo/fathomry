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

package kafka

import (
	"errors"
	"testing"
)

func TestExactReadPreservesTombstoneAndUnavailable(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	written := send(t, owner.Client(), Message{Topic: "records", Value: nil})
	ack(t, inbox)
	position := written.WritesCopy()[0].Position
	found, err := owner.Client().ReadExact(testContext(t), position)
	if err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	if got := found.ReadsCopy()[0]; got.State != ReadFound || got.Record.ValueCopy() != nil {
		t.Fatal("tombstone became absence")
	}
	position.Offset++
	future, err := owner.Client().ReadExact(testContext(t), position)
	ack(t, inbox)
	if !errors.Is(err, ErrUnavailable) || future.ReadsCopy()[0].State != ReadUnavailable {
		t.Fatal("unavailable offset became empty success", err)
	}
}
