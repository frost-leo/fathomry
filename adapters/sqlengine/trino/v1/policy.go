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

package trino

import (
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
)

// Recommend uses the exact validated preparation used by Open. The source cost
// includes readiness work, not only idle configuration. Combine policies for
// concurrent/retiring generations; declared allowances are not process RSS caps.
func Recommend(value Settings) (sqlengine.Policy, error) {
	prepared, err := native.PrepareV1(options(value))
	if err != nil {
		return sqlengine.Policy{}, translate(err, "recommend")
	}
	return policyFor(prepared), nil
}

const publicMetadataBytes int64 = 64 << 10

func policyFor(prepared native.Preparation) sqlengine.Policy {
	requirements := prepared.Budget()
	evidence := max(requirements.EvidenceBytes, requirements.ReaderEvidenceBytes, requirements.ReaderTerminalBytes)
	budget := sqlengine.Budget{
		WorkBytes:     2*max(requirements.WorkBytes, requirements.ReaderWorkBytes) + 2*evidence + publicMetadataBytes,
		EvidenceBytes: evidence + publicMetadataBytes,
	}
	capacity := 1 + 2*requirements.Active
	sourceWork := requirements.SourceBytes + publicMetadataBytes
	sourceEvidence := budget.EvidenceBytes
	return sqlengine.Policy{
		Budget: budget, SourceWorkBytes: sourceWork, SourceEvidenceBytes: sourceEvidence,
		Runtime: adapters.Options{MaxActive: 1 + requirements.Active,
			MaxWorkBytes: sourceWork + int64(requirements.Active)*budget.WorkBytes},
		Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: sourceEvidence + int64(capacity-1)*budget.EvidenceBytes},
	}
}
