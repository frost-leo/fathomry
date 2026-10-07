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

package duckdb

import (
	"context"
	"testing"

	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

func TestSQLenginePolicyMatchesSourceReservations(t *testing.T) {
	selected := testSettings()
	policy, err := Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := native.PrepareV1(options(selected))
	if err != nil {
		t.Fatal(err)
	}
	if policy.SourceWorkBytes != prepared.Budget().SourceBytes+publicMetadataBytes || policy.SourceEvidenceBytes != publicMetadataBytes ||
		policy.Runtime.MaxWorkBytes != policy.SourceWorkBytes+int64(policy.Runtime.MaxActive-1)*policy.Budget.WorkBytes ||
		policy.Evidence.MaxBytes != policy.SourceEvidenceBytes+int64(policy.Evidence.Capacity-1)*policy.Budget.EvidenceBytes {
		t.Fatal("SQL-engine source charges diverged from effective construction")
	}
	owner, inbox, runtime := testOwner(t, selected, 0)
	work, err := runtime.Inspect()
	if err != nil || work.WorkBytes != policy.SourceWorkBytes {
		t.Fatal("idle source work is not charged", err)
	}
	custody, err := inbox.Inspect()
	if err != nil || custody.Bytes != policy.SourceEvidenceBytes {
		t.Fatal("source terminal evidence reservation changed", err)
	}
	delivery, err := inbox.Next(context.Background())
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
	if _, err := receipt.WaitReleased(context.Background()); err != nil {
		t.Fatal(err)
	}
	work, _ = runtime.Inspect()
	custody, _ = inbox.Inspect()
	if work.WorkBytes != 0 || custody.Bytes != policy.SourceEvidenceBytes {
		t.Fatal("source work release erased independent evidence")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	custody, _ = inbox.Inspect()
	if custody.Bytes != 0 {
		t.Fatal("source evidence charge outlived acknowledgement")
	}
}
