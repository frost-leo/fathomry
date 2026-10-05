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
	"bytes"
	"testing"
)

func TestProduceCopiesAndPreservesPartialNativeACKs(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	deps, inbox := testDependencies(t, options)
	owner := testOpen(t, options, deps)
	payload := []byte("copied")
	receipt, err := owner.Client().Produce(testContext(t), []Message{{Topic: "records", Value: payload}, {Topic: "records", Partition: 99, Value: []byte("refused")}})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	snapshot, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	writes := value.WritesCopy()
	if !present || snapshot.Primary() == nil || len(writes) != 2 || writes[0].State != WriteAcknowledged || !writes[0].IdentityChecked || writes[1].Err == nil || writes[1].State == WriteAcknowledged {
		t.Fatal("partial native evidence collapsed")
	}
	if !bytes.Equal(observeNative(t, peer, writes[0].Position).Value, []byte("copied")) {
		t.Fatal("input was not frozen")
	}
	writes[0].Position.Offset = 999
	if value.WritesCopy()[0].Position.Offset == 999 {
		t.Fatal("mutable result alias")
	}
	ack(t, inbox)
}
