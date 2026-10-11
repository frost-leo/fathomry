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

package iceberg

import (
	"github.com/frost-leo/fathomry/adapters/tableformat/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func Recommend(settings Settings) (tableformat.Policy, error) {
	p, err := Prepare(settings, NativeOptions{})
	if err != nil {
		return tableformat.Policy{}, err
	}
	return p.Policy(), nil
}
func (p Prepared) Policy() tableformat.Policy {
	b := p.native.Budget()
	budget := tableformat.Budget{WorkBytes: b.WorkBytes + 2*b.EvidenceBytes + 128<<10, EvidenceBytes: b.EvidenceBytes + 64<<10, SourceWorkBytes: b.SourceBytes, SourceEvidenceBytes: 64 << 10}
	capacity := 1 + 2*b.Active
	return tableformat.Policy{Budget: budget, Runtime: adapters.Options{MaxActive: 1 + b.Active, MaxWorkBytes: budget.SourceWorkBytes + int64(b.Active)*budget.WorkBytes, MaxTasks: 1, MaxDepth: 1, MaxHolds: 2},
		Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: budget.SourceEvidenceBytes + int64(capacity-1)*budget.EvidenceBytes}}
}
