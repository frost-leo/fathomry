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

func TestExplicitRecipientResolutionAndIsolation(t *testing.T) {
	for _, scenario := range []struct {
		body          string
		found, failed bool
	}{
		{`{"code":0,"data":{"user_list":[{"email":"receiver@example.test","user_id":"ou_receiver"}]}}`, true, false},
		{`{"code":0,"data":{"user_list":[]}}`, false, false},
		{`{"code":0,"data":{"user_list":[{"email":"other@example.test","user_id":"ou_other"}]}}`, false, true},
		{`{"code":0,"data":{"user_list":[{"user_id":"ou_unassociated"}]}}`, false, true},
		{`{"code":99991672,"msg":"permission denied"}`, false, true},
	} {
		peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/auth/") {
				defaultReply(w, r)
				return
			}
			_, _ = io.WriteString(w, scenario.body)
		})
		bound := bindTest(t, testOptions(peer), 1)
		receipt, err := bound.client.ResolveEmail(context.Background(), fault.Correlation{Call: "resolve"}, "receiver@example.test")
		got := resolved(t, receipt, err)
		recipient, found := got.Outcome.Value.ResolvedRecipient()
		if (got.Err() != nil) != scenario.failed || found != scenario.found || found && (recipient.Type != "open_id" || recipient.ID != "ou_receiver") {
			t.Fatal("recipient resolution crossed identity")
		}
		requests := peer.snapshot()
		if len(requests) != 2 || requests[1].path != "/open-apis/contact/v3/users/batch_get_id" || strings.Contains(string(requests[1].body), "mobiles") {
			t.Fatal("lookup expanded into directory search")
		}
	}
}
