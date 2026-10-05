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
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func childID(name, parent string) fault.Correlation {
	return fault.Correlation{Call: name, Parent: parent}
}

func claimRoot(t *testing.T, inbox *invocation.Inbox[Result]) {
	t.Helper()
	record, err := inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := record.Receipt().WaitReleased(deadline(t)); err != nil {
			t.Error(err)
		}
		if err := record.Release(); err != nil {
			t.Error(err)
		}
	})
}

func TestOwnedSessionsRequireNestedLeaseAllowance(t *testing.T) {
	server, options := newPeer(t)
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	limits := LimitsV1(options)
	limits.MaxLeases = 1
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "single-lease", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	inbox, err := invocation.NewInbox[Result](2, 2*BudgetV1(options).EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	before := server.count()
	if session, receipt, err := client.BeginMultipart(deadline(t), deadline(t), deadline(t), correlation("root"), WriteRequest{Key: "owned/key", Size: 1}); session != nil || receipt != nil || !errors.Is(err, ErrInput) {
		t.Fatal("multipart admitted without a child lease")
	}
	if cursor, receipt, err := client.Enumerate(deadline(t), deadline(t), correlation("cursor"), ListRequest{Prefix: "owned/"}); cursor != nil || receipt != nil || !errors.Is(err, ErrInput) {
		t.Fatal("iterator admitted without a child lease")
	}
	if server.count() != before || inbox.Usage().Outstanding != 0 {
		t.Fatal("refusal acquired native work")
	}
}
