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

package redis

import (
	"github.com/frost-leo/fathomry/adapters/cache/v1"
)

const sourceEvidenceBytes int64 = 64 << 10

func Recommend(value Settings) (cache.Policy, error) {
	prepared, err := Prepare(value)
	if err != nil {
		return cache.Policy{}, err
	}
	return Compose(prepared)
}
func (prepared Prepared) Policy() (cache.Policy, error) { return Compose(prepared) }

// Compose declares aggregate capacity for exactly the listed concurrent source
// instances, including retired generations. List the same preparation repeatedly
// if several native owners coexist. It never resizes an existing runtime.
// Private bridge storage adds two native receipts plus public wrapper metadata;
// the native work/evidence/source formulas are owned solely by Internal.
func Compose(values ...Prepared) (cache.Policy, error) {
	if len(values) == 0 || len(values) > 16 {
		return cache.Policy{}, fail(ErrInput, "policy")
	}
	policy := cache.Policy{}
	for _, value := range values {
		meta := value.metadata
		if meta.Limits.Active < 1 {
			return cache.Policy{}, fail(ErrInput, "policy")
		}
		policy.Budget.WorkBytes = max(policy.Budget.WorkBytes, meta.WorkBytes+2*meta.EvidenceBytes+64<<10)
		policy.Budget.EvidenceBytes = max(policy.Budget.EvidenceBytes, meta.EvidenceBytes+64<<10)
		policy.SourceWorkBytes += meta.SourceBytes
		policy.Runtime.MaxActive += 1 + meta.Limits.Active
		policy.Runtime.MaxQueued += meta.Limits.Queued
		policy.Evidence.Capacity += 1 + 2*meta.Limits.Active + meta.Limits.Queued
	}
	policy.Runtime.MaxTasks, policy.Runtime.MaxDepth, policy.Runtime.MaxHolds = 2, 2, 2
	policy.Runtime.MaxWorkBytes = policy.SourceWorkBytes + int64(policy.Runtime.MaxActive-len(values))*policy.Budget.WorkBytes
	policy.Runtime.MaxQueuedBytes = int64(policy.Runtime.MaxQueued) * policy.Budget.WorkBytes
	policy.Evidence.MaxBytes = int64(len(values))*sourceEvidenceBytes + int64(policy.Evidence.Capacity-len(values))*policy.Budget.EvidenceBytes
	if policy.Runtime.MaxActive > 1024 || policy.Runtime.MaxWorkBytes > 1<<40 || policy.Runtime.MaxQueuedBytes > 1<<40 || policy.Evidence.MaxBytes > 1<<40 {
		return cache.Policy{}, fail(ErrLimit, "policy")
	}
	return policy, nil
}
