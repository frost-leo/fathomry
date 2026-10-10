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

package orchestration

import "github.com/frost-leo/fathomry/adapters/v1"

// Budget declares one process-operation or Worker family and its independent
// evidence. It is not a remote execution quota or a measured heap/RSS limit.
type Budget struct {
	WorkBytes     int64 `json:"work_bytes"`
	WorkerBytes   int64 `json:"worker_bytes"`
	EvidenceBytes int64 `json:"evidence_bytes"`
}

// Policy accounts for sources, operation families, Worker families and separately
// drained operation/Worker/task evidence. Providers derive these values from
// their frozen selections. This data grants no native or resource authority.
type Policy struct {
	Budget                               Budget
	Runtime                              adapters.Options
	Evidence, Workers, Tasks             adapters.EvidenceOptions
	SourceWorkBytes, SourceEvidenceBytes int64
	NativeWorkBytes, NativeWorkerBytes   int64
}
