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
	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

// Recommend supplies ceilings for one source generation at its native capacity.
// For Follow overlap, request ForGenerations before constructing shared owners.
func Recommend(value Settings) (objectstore.Policy, error) {
	if err := Validate(value); err != nil {
		return objectstore.Policy{}, err
	}
	nativeBudget := native.BudgetV1(options(value))
	budget := objectstore.Budget{WorkBytes: nativeBudget.WorkBytes + 3*nativeBudget.EvidenceBytes + 256<<10, EvidenceBytes: nativeBudget.EvidenceBytes + 64<<10}
	capacity := 1 + 2*nativeBudget.Active + nativeBudget.Queued
	return objectstore.Policy{Budget: budget, Runtime: adapters.Options{
		MaxActive: 1 + nativeBudget.Active, MaxQueued: nativeBudget.Queued,
		MaxWorkBytes:   sourceWorkBytes + int64(nativeBudget.Active)*budget.WorkBytes,
		MaxQueuedBytes: int64(nativeBudget.Queued) * budget.WorkBytes, MaxTasks: 2, MaxDepth: 2, MaxHolds: 4,
	}, Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: sourceEvidenceBytes + int64(capacity-1)*budget.EvidenceBytes}}, nil
}
