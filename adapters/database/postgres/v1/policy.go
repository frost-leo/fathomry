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

package postgres

import (
	"github.com/frost-leo/fathomry/adapters/database/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/pgx/v5"
)

const familyRecords = MaxPreparedStatements + MaxSavepoints + 2

// Recommend validates settings and derives budgets from the supported native
// reservation model. The extra 64 KiB covers bounded public attribution.
func Recommend(value Settings) (database.Policy, error) {
	if err := Validate(value); err != nil {
		return database.Policy{}, err
	}
	nativeOptions := options(value)
	limits := native.LimitsV1(nativeOptions)
	evidence := native.EvidenceBytesV1(nativeOptions)
	budget := database.Budget{WorkBytes: limits.Bytes/int64(limits.Active) + 64<<10, EvidenceBytes: evidence + 64<<10}
	capacity := 1 + limits.Active*familyRecords + limits.Queued
	return database.Policy{
		Budget: budget,
		Runtime: adapters.Options{MaxActive: 1 + limits.Active, MaxQueued: limits.Queued,
			MaxWorkBytes:   sourceWorkBytes + int64(limits.Active)*budget.WorkBytes,
			MaxQueuedBytes: int64(limits.Queued) * budget.WorkBytes},
		Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: sourceEvidenceBytes + int64(capacity-1)*budget.EvidenceBytes},
	}, nil
}
