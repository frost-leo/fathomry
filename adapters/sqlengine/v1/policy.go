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

package sqlengine

import "github.com/frost-leo/fathomry/adapters/v1"

// Budget declares one root family's work bytes and one result's evidence bytes.
// Children share root work but reserve independent evidence. It covers supported
// bridge/native work, not caller-retained copies or every native allocation.
type Budget struct {
	WorkBytes     int64 `json:"work_bytes"`
	EvidenceBytes int64 `json:"evidence_bytes"`
}

// Policy describes one source's recommended public admission and custody.
// Concrete providers derive it from their validated effective settings; this
// package supplies no private defaults, native budget formulas or constructors.
//
// Runtime and Evidence already include the source reservation below: do not add
// it twice. SourceWorkBytes remains charged until actual source release, including
// idle owners and still-retiring generations. SourceEvidenceBytes remains charged
// until the source's terminal record is handled and acknowledged, even after
// native release. Source construction may be pure, embedded I/O or coordinator
// readiness; none of those profiles is assumed by the common contract.
//
// Combining providers or overlapping generations requires deliberate aggregate
// limits and sufficient root budgets. A zero source charge means no declaration,
// not proof that owning a source or its errors costs nothing. This data does not
// resize a runtime, allocate resources, certify readiness or impose an RSS limit.
type Policy struct {
	Budget              Budget
	Runtime             adapters.Options
	Evidence            adapters.EvidenceOptions
	SourceWorkBytes     int64
	SourceEvidenceBytes int64
}
