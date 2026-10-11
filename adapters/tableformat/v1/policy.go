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

package tableformat

import "github.com/frost-leo/fathomry/adapters/v1"

// Budget declares bytes for one operation root family, one independent result,
// one retained source and that source's terminal record. Children share root work
// but reserve independent evidence. Source work remains charged through actual
// native release, including idle and retiring generations; source evidence stays
// charged until its record is acknowledged. Zero means undeclared, not free.
// These envelopes cover supported provider work, not every native allocation or
// caller-retained copy; a concrete provider defines the supported accounting.
type Budget struct {
	WorkBytes           int64 `json:"work_bytes"`
	EvidenceBytes       int64 `json:"evidence_bytes"`
	SourceWorkBytes     int64 `json:"source_work_bytes"`
	SourceEvidenceBytes int64 `json:"source_evidence_bytes"`
}

// Policy is a provider's explicit recommendation from validated effective
// settings, not a runtime constructor, resize, readiness check or reservation.
// Runtime and Evidence already include the declared source charges: do not add
// them twice. Overlapping sources/generations require deliberate aggregate limits
// and sufficient per-family budgets. Providers reject insufficient declarations
// before entry; this common package supplies no private defaults or SDK formulas.
type Policy struct {
	Budget   Budget
	Runtime  adapters.Options
	Evidence adapters.EvidenceOptions
}
