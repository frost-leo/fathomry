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

package nethttp

import "github.com/frost-leo/fathomry/adapters/httpclient/v1"

const publicMetadataBytes int64 = 64 << 10

// Recommend covers data-only preparation; native-aware consumers use Prepare and
// Prepared.Policy. No default or native budget formula is duplicated here.
func Recommend(value Settings) (httpclient.Policy, error) {
	prepared, err := Prepare(value, NativeOptions{})
	if err != nil {
		return httpclient.Policy{}, err
	}
	return prepared.Policy()
}
func (prepared Prepared) Policy() (httpclient.Policy, error) { return Compose(prepared) }

// Compose covers exactly the listed overlapping source owners, including retired
// generations. Repeat a preparation for equal concurrent instances. It preserves
// root envelopes, adds private bridge/child records and rejects unrepresentable
// aggregates rather than silently shrinking concurrency or enlarging a Runtime.
func Compose(values ...Prepared) (httpclient.Policy, error) {
	if len(values) < 1 || len(values) > 16 {
		return httpclient.Policy{}, fail(ErrInput, "policy")
	}
	var result httpclient.Policy
	for _, prepared := range values {
		value := prepared.metadata
		if value.Limits.Active < 1 {
			return httpclient.Policy{}, fail(ErrInput, "policy")
		}
		result.Budget.WorkBytes = max(result.Budget.WorkBytes, value.WorkBytes+2*value.EvidenceBytes+publicMetadataBytes)
		result.Budget.EvidenceBytes = max(result.Budget.EvidenceBytes, value.EvidenceBytes+publicMetadataBytes)
		result.SourceWorkBytes += value.SourceBytes + publicMetadataBytes
		result.SourceEvidenceBytes += publicMetadataBytes
		result.Runtime.MaxActive += 1 + value.Limits.Active
		result.Runtime.MaxQueued += value.Limits.Queued
		result.Evidence.Capacity += 1 + 2*value.Limits.Active + value.Limits.Queued
	}
	result.Runtime.MaxTasks, result.Runtime.MaxDepth, result.Runtime.MaxHolds = 2, 2, 2
	result.Runtime.MaxWorkBytes = result.SourceWorkBytes + int64(result.Runtime.MaxActive-len(values))*result.Budget.WorkBytes
	result.Runtime.MaxQueuedBytes = int64(result.Runtime.MaxQueued) * result.Budget.WorkBytes
	result.Evidence.MaxBytes = result.SourceEvidenceBytes + int64(result.Evidence.Capacity-len(values))*result.Budget.EvidenceBytes
	if result.Runtime.MaxActive > 1024 || result.Runtime.MaxQueued > 4096 || result.Evidence.Capacity > 65536 ||
		result.Runtime.MaxWorkBytes > 1<<40 || result.Runtime.MaxQueuedBytes > 1<<40 || result.Evidence.MaxBytes > 1<<40 {
		return httpclient.Policy{}, fail(ErrLimit, "policy")
	}
	return result, nil
}
