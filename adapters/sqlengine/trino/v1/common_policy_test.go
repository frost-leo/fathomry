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

package trino_test

import (
	"context"
	"net/http"
	"testing"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
)

func TestSQLenginePolicyMatchesSourceReservations(t *testing.T) {
	peer := newWirePeer(t, func(http.ResponseWriter, *http.Request, []byte) { t.Error("unexpected application dispatch") })
	settings := peer.settings()
	policy, err := trino.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	var declared sqlengine.Policy = policy
	if declared.SourceWorkBytes <= 0 || declared.SourceEvidenceBytes <= 0 ||
		declared.Runtime.MaxWorkBytes != declared.SourceWorkBytes+int64(declared.Runtime.MaxActive-1)*declared.Budget.WorkBytes ||
		declared.Evidence.MaxBytes != declared.SourceEvidenceBytes+int64(declared.Evidence.Capacity-1)*declared.Budget.EvidenceBytes {
		t.Fatal("SQL-engine source costs are missing or counted twice")
	}
	owner, inbox, runtime := openPublic(t, settings, 0)
	work, err := runtime.Inspect()
	if err != nil || work.WorkBytes != policy.SourceWorkBytes {
		t.Fatal("idle Trino source charge changed", err)
	}
	custody, err := inbox.Inspect()
	if err != nil || custody.Bytes != policy.SourceEvidenceBytes {
		t.Fatal("source evidence charge changed", err)
	}
	delivery, err := inbox.Next(boundedContext(t))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := receipt.WaitReleased(boundedContext(t)); err != nil {
		t.Fatal(err)
	}
	work, _ = runtime.Inspect()
	custody, _ = inbox.Inspect()
	if work.WorkBytes != 0 || custody.Bytes != policy.SourceEvidenceBytes {
		t.Fatal("source release erased required evidence charge")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	custody, _ = inbox.Inspect()
	if custody.Bytes != 0 {
		t.Fatal("source evidence remains charged after Ack")
	}
}
