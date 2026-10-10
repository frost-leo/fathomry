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

package telemetry

import "github.com/frost-leo/fathomry/adapters/v1"

// Budget declares one operation's work and independently retained evidence.
// It is not a measured heap/RSS bound, export quota or acknowledgement.
type Budget struct {
	WorkBytes     int64 `json:"work_bytes"`
	EvidenceBytes int64 `json:"evidence_bytes"`
}

// Policy accounts for overlapping sources and their operation/evidence envelopes.
// Providers derive and validate costs; this data neither constructs sources nor
// reserves a hidden export, flush or shutdown slot.
type Policy struct {
	Budget              Budget
	Runtime             adapters.Options
	Evidence            adapters.EvidenceOptions
	SourceWorkBytes     int64
	SourceEvidenceBytes int64
}
