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
	"github.com/frost-leo/fathomry/internal/fault"
	"testing"
)

func TestReactionLifecycle(t *testing.T) {
	peer := newPeer(t, nil)
	bound := bindTest(t, testOptions(peer), 1)
	ctx := context.Background()
	id := fault.Correlation{Call: "reaction"}
	receipt, err := bound.client.AddReaction(ctx, id, "om_fixture", "THUMBSUP")
	got := resolved(t, receipt, err)
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, bound.inbox)
	receipt, err = bound.client.ListReactions(ctx, id, "om_fixture", Page{})
	got = resolved(t, receipt, err)
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, bound.inbox)
	receipt, err = bound.client.DeleteReaction(ctx, id, "om_fixture", "reaction_fixture")
	got = resolved(t, receipt, err)
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	capture := peer.snapshot()
	if len(capture) != 4 || capture[3].method != "DELETE" || capture[3].path != "/open-apis/im/v1/messages/om_fixture/reactions/reaction_fixture" {
		t.Fatal("wrong reaction lifecycle")
	}
}
