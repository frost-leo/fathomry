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

package kafka

import (
	"github.com/frost-leo/fathomry/adapters/broker/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
)

const sourceEvidenceBytes int64 = 64 << 10

// Recommend derives explicit source/work/evidence reservations for one source.
func Recommend(value Settings) (broker.Policy, error) { return Compose(value) }

// Compose reserves the listed source instances concurrently, including retired
// Fixed/Follow generations that can still be borrowed. List overlapping
// generations separately. It never enlarges an already created Runtime.
func Compose(values ...Settings) (broker.Policy, error) {
	if len(values) == 0 || len(values) > 16 {
		return broker.Policy{}, fail(ErrInput, "policy")
	}
	policy := broker.Policy{}
	for _, value := range values {
		if err := Validate(value); err != nil {
			return broker.Policy{}, err
		}
		budget := native.BudgetV1(options(value))
		work := budget.WorkBytes + 2*budget.EvidenceBytes + 64<<10
		policy.Budget.WorkBytes = max(policy.Budget.WorkBytes, work)
		policy.Budget.EvidenceBytes = max(policy.Budget.EvidenceBytes, budget.EvidenceBytes+64<<10)
		policy.SourceWorkBytes += budget.SourceBytes
		policy.Runtime.MaxActive += 1 + budget.Active
		policy.Runtime.MaxQueued += budget.Queued
		capacity := 2*budget.Active + budget.Queued
		policy.Evidence.Capacity += 1 + capacity
	}
	policy.Runtime.MaxTasks = 2
	policy.Runtime.MaxWorkBytes = policy.SourceWorkBytes + int64(policy.Runtime.MaxActive-len(values))*policy.Budget.WorkBytes
	policy.Runtime.MaxQueuedBytes = int64(policy.Runtime.MaxQueued) * policy.Budget.WorkBytes
	policy.Evidence.MaxBytes = int64(len(values))*sourceEvidenceBytes + int64(policy.Evidence.Capacity-len(values))*policy.Budget.EvidenceBytes
	policy.Runtime.MaxDepth = 2
	policy.Runtime.MaxHolds = 2
	return policy, nil
}
