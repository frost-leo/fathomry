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
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestMergedForwardRetainsPartialEvidence(t *testing.T) {
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/") {
			defaultReply(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"message":{"message_id":"om_merged"},"invalid_message_id_list":["om_invalid"]}}`)
	})
	bound := bindTest(t, testOptions(peer), 1)
	ids := []string{"om_valid", "om_invalid"}
	receipt, err := bound.client.MergeForward(context.Background(), fault.Correlation{Call: "merged"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "merge-uuid", ids)
	got := resolved(t, receipt, err)
	if got.Err() != nil || got.Outcome.Value.Effect() != Partial || got.Outcome.Value.MessageID() != "om_merged" {
		t.Fatal("partial merged result lost")
	}
	ids[0] = "changed"
	related := got.Outcome.Value.RelatedIDs()
	invalid := got.Outcome.Value.InvalidMessageIDs()
	if related[0] != "om_valid" || len(invalid) != 1 || invalid[0] != "om_invalid" {
		t.Fatal("input or rejection association lost")
	}
	invalid[0] = "changed"
	if got.Outcome.Value.InvalidMessageIDs()[0] != "om_invalid" {
		t.Fatal("rejection aliases caller storage")
	}
	requests := peer.snapshot()
	if requests[1].path != "/open-apis/im/v1/messages/merge_forward" || !strings.Contains(requests[1].query, "uuid=merge-uuid") {
		t.Fatal("native deduplication route lost")
	}
}
func TestMalformedMessagePagesCannotBecomeSuccessfulReads(t *testing.T) {
	for _, body := range []string{
		`{"code":0,"data":{"items":null,"has_more":false}}`,
		`{"code":0,"data":{"items":[],"has_more":null}}`,
		`{"code":0,"data":{"items":[],"has_more":true}}`,
	} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/auth/") {
				defaultReply(w, r)
				return
			}
			_, _ = io.WriteString(w, body)
		})
		bound := bindTest(t, testOptions(peer), 1)
		receipt, err := bound.client.ListMessages(context.Background(), fault.Correlation{Call: "invalid"}, "chat", "oc_fixture", 0, 0, Page{})
		if got := resolved(t, receipt, err); got.Err() == nil || got.Outcome.Value.Effect() != Unknown {
			t.Fatal("malformed pagination treated as complete")
		}
	}
}
