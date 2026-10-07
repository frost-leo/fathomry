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
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

// Recommend uses the same validated resolved metadata as Open. It covers one
// source and its concurrent roots, each with a terminal and one chunk record.
// Consumers acknowledge chunks incrementally. Combine policies explicitly for
// overlapping replacement generations; these reservations are not an RSS cap.
func Recommend(value Settings) (sqlengine.Policy, error) {
	prepared, err := native.PrepareV1(options(value))
	if err != nil {
		return sqlengine.Policy{}, translate(err, "recommend")
	}
	return policyFor(prepared), nil
}

const publicMetadataBytes int64 = 64 << 10

func policyFor(prepared native.Preparation) sqlengine.Policy {
	nativeBudget := prepared.Budget()
	evidence := max(nativeBudget.EvidenceBytes, nativeBudget.ReaderEvidenceBytes, nativeBudget.ReaderTerminalBytes)
	budget := sqlengine.Budget{
		WorkBytes:     max(nativeBudget.WorkBytes, nativeBudget.ReaderWorkBytes) + 2*evidence + publicMetadataBytes,
		EvidenceBytes: evidence + publicMetadataBytes,
	}
	capacity := 1 + 2*nativeBudget.Active + nativeBudget.Queued
	sourceWork := nativeBudget.SourceBytes + publicMetadataBytes
	sourceEvidence := publicMetadataBytes
	return sqlengine.Policy{
		Budget: budget, SourceWorkBytes: sourceWork, SourceEvidenceBytes: sourceEvidence,
		Runtime: adapters.Options{
			MaxActive: 1 + nativeBudget.Active, MaxQueued: nativeBudget.Queued,
			MaxWorkBytes:   sourceWork + int64(nativeBudget.Active)*budget.WorkBytes,
			MaxQueuedBytes: int64(nativeBudget.Queued) * budget.WorkBytes,
		},
		Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: sourceEvidence + int64(capacity-1)*budget.EvidenceBytes},
	}
}
