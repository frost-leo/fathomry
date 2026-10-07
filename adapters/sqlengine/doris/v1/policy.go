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

package doris

import (
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
)

// Recommend derives one source's policy from the authoritative native resolution.
// Source contributions are already included in the totals. Compose overlapping
// generations explicitly, charging roots at the budget selected by their facade.
// The caller still owns admission and evidence mechanisms.
func Recommend(value Settings) (sqlengine.Policy, error) {
	_, policy, err := prepare(value)
	return policy, err
}
func prepare(value Settings) (native.Preparation, sqlengine.Policy, error) {
	prepared, err := native.PrepareV1(options(value))
	if err != nil {
		return native.Preparation{}, sqlengine.Policy{}, translate(err, "validate")
	}
	nativeBudget := prepared.Reservation()
	limits := nativeBudget.Limits
	budget := sqlengine.Budget{WorkBytes: nativeBudget.WorkBytes + 2*nativeBudget.EvidenceBytes + 64<<10, EvidenceBytes: nativeBudget.EvidenceBytes + 64<<10}
	capacity := 1 + 2*limits.Active + limits.Queued
	return prepared, sqlengine.Policy{Budget: budget,
		SourceWorkBytes: nativeBudget.SourceWorkBytes, SourceEvidenceBytes: nativeBudget.SourceEvidenceBytes,
		Runtime: adapters.Options{MaxActive: 1 + limits.Active, MaxQueued: limits.Queued, MaxTasks: 2, MaxDepth: 2, MaxHolds: 2,
			MaxWorkBytes:   nativeBudget.SourceWorkBytes + int64(limits.Active)*budget.WorkBytes,
			MaxQueuedBytes: int64(limits.Queued) * budget.WorkBytes},
		Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: nativeBudget.SourceEvidenceBytes + int64(capacity-1)*budget.EvidenceBytes}}, nil
}
