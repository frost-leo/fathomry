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

func TestTagInputsAndResultsAreCopied(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	test.result(test.client.Put(ctx, ctx, WriteRequest{Key: "owned/tags", Size: 1}, strings.NewReader("x")))
	tags := map[string]string{"purpose": "original"}
	receipt, err := test.client.SetTags(ctx, Address{Key: "owned/tags"}, tags)
	tags["purpose"] = "mutated"
	test.result(receipt, err)
	result := test.result(test.client.GetTags(ctx, Address{Key: "owned/tags"}))
	copied := result.TagsCopy()
	if copied["purpose"] != "original" {
		t.Fatal("input was borrowed past return")
	}
	copied["purpose"] = "changed"
	if result.TagsCopy()["purpose"] != "original" {
		t.Fatal("result storage aliased")
	}
}
