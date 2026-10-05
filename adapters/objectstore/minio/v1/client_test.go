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

package minio

import (
	"strings"
	"testing"
)

func TestClientCorrelationIsFrozenWithoutNativeIDs(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	before := peer.requests()
	for _, id := range []string{strings.Repeat("x", 257), "invalid\nvalue", string([]byte{0xff})} {
		if _, err := test.client.WithID(id); err == nil {
			t.Fatal("invalid correlation accepted")
		}
	}
	copied, err := test.client.WithID("opaque-\u4e1a\u52a1-id")
	if err != nil {
		t.Fatal(err)
	}
	if peer.requests() != before {
		t.Fatal("facade copying performed I/O")
	}
	first := test.result(copied.Put(ctx, ctx, WriteRequest{Key: "owned/correlation", Size: 1}, strings.NewReader("x")))
	second := test.result(test.client.Stat(ctx, Address{Key: "owned/correlation"}))
	if first.Attribution().ID != "opaque-\u4e1a\u52a1-id" || second.Attribution().ID != "" || first.Attribution().Sequence == second.Attribution().Sequence {
		t.Fatal("correlation aliased or overwritten")
	}
}
