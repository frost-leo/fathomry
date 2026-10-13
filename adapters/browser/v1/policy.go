/*
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

package browser

import "github.com/frost-leo/fathomry/adapters/v1"

// Budget describes logical source, retained-context and independent evidence costs.
// These are not hard limits on browser RSS, network traffic, or caller-owned copies.
type Budget struct {
	WorkBytes           int64 `json:"work_bytes"`
	EvidenceBytes       int64 `json:"evidence_bytes"`
	SourceWorkBytes     int64 `json:"source_work_bytes"`
	SourceEvidenceBytes int64 `json:"source_evidence_bytes"`
}

// Policy includes the retained source and its terminal evidence once.
type Policy struct {
	Budget   Budget
	Runtime  adapters.Options
	Evidence adapters.EvidenceOptions
}
