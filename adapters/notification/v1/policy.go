/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package notification

import (
	"github.com/frost-leo/fathomry/adapters/v1"
	"time"
)

// Budget declares one root's work and one independent evidence record, in bytes.
// It is not a measured RSS ceiling, remote capacity or cross-worker quota.
type Budget struct {
	WorkBytes        int64         `json:"work_bytes"`
	EvidenceBytes    int64         `json:"evidence_bytes"`
	AdmissionTimeout time.Duration `json:"admission_timeout_ns"`
}

// Policy includes explicitly overlapping source owners through actual release.
// Composition never resizes the caller's Runtime or Inbox.
type Policy struct {
	Budget                               Budget
	Runtime                              adapters.Options
	Evidence                             adapters.EvidenceOptions
	SourceWorkBytes, SourceEvidenceBytes int64
}
