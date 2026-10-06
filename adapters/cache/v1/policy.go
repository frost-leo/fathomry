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

package cache

import (
	"github.com/frost-leo/fathomry/adapters/v1"
)

// Budget covers a root family including private receipt custody. Children share
// work but each reserve evidence independently.
type Budget struct {
	WorkBytes     int64 `json:"work_bytes"`
	EvidenceBytes int64 `json:"evidence_bytes"`
}

// Policy covers all explicitly composed concurrent source generations. Resident
// source work remains charged at idle until actual shutdown, not just setup.
// These declared envelopes are not hard heap/RSS or distributed limits.
type Policy struct {
	Budget          Budget
	Runtime         adapters.Options
	Evidence        adapters.EvidenceOptions
	SourceWorkBytes int64
}
