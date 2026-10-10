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

package otel

import "github.com/frost-leo/fathomry/adapters/telemetry/v1"

const publicMetadataBytes int64 = 64 << 10
const exportLoopBytes int64 = 4096

// Budget covers one operation, including its native bridge and detached public
// observation. It is a declared envelope, not a measured heap/RSS limit.
type Budget = telemetry.Budget

// Policy explicitly reserves overlapping native sources and public roots.
// Active span calls consume normal root slots; no hidden Flush slot is promised.
type Policy = telemetry.Policy

// Recommend covers exactly one source. Compose covers all simultaneously owned
// generations; a retired source remains charged until actual native release.
func Recommend(value Settings) (Policy, error) {
	prepared, err := Prepare(value)
	if err != nil {
		return Policy{}, err
	}
	return prepared.Policy()
}

// Policy returns this exact source's public envelope without acquiring resources.
func (prepared Prepared) Policy() (Policy, error) { return Compose(prepared) }

// Compose sums source residence/concurrency and covers the largest call
// envelope. Repeat a preparation for equal concurrent owners. It never mutates
// a caller-owned Runtime or substitutes smaller native limits.
func Compose(values ...Prepared) (Policy, error) {
	if len(values) < 1 || len(values) > 16 {
		return Policy{}, fail(ErrInput, "policy")
	}
	var result Policy
	for _, prepared := range values {
		value := prepared.metadata
		if value.Limits.Active < 1 {
			return Policy{}, fail(ErrInput, "policy")
		}
		result.Budget.WorkBytes = max(result.Budget.WorkBytes, value.WorkBytes+2*value.EvidenceBytes+publicMetadataBytes)
		result.Budget.EvidenceBytes = max(result.Budget.EvidenceBytes, value.EvidenceBytes+publicMetadataBytes)
		result.SourceWorkBytes += value.SourceBytes + publicMetadataBytes + exportLoopBytes
		result.SourceEvidenceBytes += publicMetadataBytes
		result.Runtime.MaxActive += 1 + value.Limits.Active
		result.Runtime.MaxQueued += value.Limits.Queued
		result.Evidence.Capacity += 1 + value.Limits.Active + value.Limits.Queued
	}
	result.Runtime.MaxTasks, result.Runtime.MaxDepth, result.Runtime.MaxHolds = 1, 1, 1
	result.Runtime.MaxWorkBytes = result.SourceWorkBytes + int64(result.Runtime.MaxActive-len(values))*result.Budget.WorkBytes
	result.Runtime.MaxQueuedBytes = int64(result.Runtime.MaxQueued) * result.Budget.WorkBytes
	result.Evidence.MaxBytes = result.SourceEvidenceBytes + int64(result.Evidence.Capacity-len(values))*result.Budget.EvidenceBytes
	if result.Runtime.MaxActive > 1024 || result.Runtime.MaxQueued > 4096 || result.Evidence.Capacity > 65536 ||
		result.Runtime.MaxWorkBytes > 1<<40 || result.Runtime.MaxQueuedBytes > 1<<40 || result.Evidence.MaxBytes > 1<<40 {
		return Policy{}, fail(ErrLimit, "policy")
	}
	return result, nil
}
