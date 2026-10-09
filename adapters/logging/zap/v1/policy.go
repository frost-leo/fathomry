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

package zap

import (
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

const (
	publicMetadataBytes int64 = 64 << 10
	maxPolicies               = 16
	maxFamilies               = 4
	maxChildren               = 16
	maxDerivedViews           = 128
	derivedBytes        int64 = 8 << 20
)

// Budget reserves one finite call. Shared vocabulary does not unify logger APIs.
type Budget = logging.Budget

// Policy separates physical residence, finite work and retained derived families.
// It is a declared envelope, not measured RSS or a physical disk quota.
type Policy struct {
	Budget                               Budget
	Runtime                              adapters.Options
	Evidence                             adapters.EvidenceOptions
	SourceWorkBytes, SourceEvidenceBytes int64
	FamilyWorkBytes, FamilyEvidenceBytes int64
}

// Recommend covers one physical source, up to 16 live immutable policies and
// four retained derived families. Ordinary native writes still share ONE allowance.
func Recommend(value Settings) (Policy, error) {
	prepared, err := Prepare(value)
	if err != nil {
		return Policy{}, err
	}
	return prepared.Policy()
}
func (prepared Prepared) Policy() (Policy, error) { return Compose(prepared) }

// Compose reserves overlapping physical sources. Level-only Adopt reuses one
// physical source; different-directory Open and retired physical sources overlap.
func Compose(values ...Prepared) (Policy, error) {
	if len(values) < 1 || len(values) > 16 {
		return Policy{}, fail(ErrInput, "policy")
	}
	var result Policy
	for _, prepared := range values {
		value := prepared.metadata
		if value.Limits.Active != 1 {
			return Policy{}, fail(ErrInput, "policy")
		}
		result.Budget.WorkBytes = max(result.Budget.WorkBytes, value.WorkBytes+2*value.EvidenceBytes+publicMetadataBytes)
		result.Budget.EvidenceBytes = max(result.Budget.EvidenceBytes, value.EvidenceBytes+publicMetadataBytes)
		result.SourceWorkBytes += value.SourceBytes + (maxPolicies-1)*value.PolicyBytes + (1+maxPolicies)*publicMetadataBytes
		result.SourceEvidenceBytes += publicMetadataBytes
		result.Runtime.MaxActive += 2 + maxFamilies
		result.Runtime.MaxQueued += value.Limits.Queued
	}
	result.FamilyWorkBytes = 2*derivedBytes + maxChildren*result.Budget.WorkBytes
	result.FamilyEvidenceBytes = publicMetadataBytes
	result.Runtime.MaxTasks = maxChildren + 1
	result.Runtime.MaxDepth = 2
	result.Runtime.MaxHolds = 1
	count := int64(len(values))
	result.Runtime.MaxWorkBytes = result.SourceWorkBytes + count*(result.Budget.WorkBytes+maxFamilies*result.FamilyWorkBytes)
	result.Runtime.MaxQueuedBytes = int64(result.Runtime.MaxQueued) * max(result.Budget.WorkBytes, result.FamilyWorkBytes)
	perSource := 1 + maxPolicies + maxFamilies*(1+maxChildren) + 1
	result.Evidence.Capacity = len(values)*perSource + result.Runtime.MaxQueued
	result.Evidence.MaxBytes = result.SourceEvidenceBytes + count*int64(maxPolicies+maxFamilies)*publicMetadataBytes + int64(len(values)*(maxFamilies*maxChildren+1)+result.Runtime.MaxQueued)*result.Budget.EvidenceBytes
	if result.Runtime.MaxWorkBytes > 1<<40 || result.Runtime.MaxQueuedBytes > 1<<40 || result.Evidence.MaxBytes > 1<<40 {
		return Policy{}, fail(ErrLimit, "policy")
	}
	return result, nil
}

func checkPolicy(policy Policy, deps Dependencies) error {
	options, err := deps.Runtime.Options()
	if err != nil {
		return err
	}
	evidence, err := deps.Evidence.Options()
	if err != nil {
		return err
	}
	want := policy.Runtime
	if options.MaxActive < want.MaxActive || options.MaxQueued < want.MaxQueued || options.MaxWorkBytes < want.MaxWorkBytes || options.MaxQueuedBytes < want.MaxQueuedBytes || options.MaxTasks < want.MaxTasks || options.MaxDepth < want.MaxDepth || options.MaxHolds < want.MaxHolds || evidence.Capacity < policy.Evidence.Capacity || evidence.MaxBytes < policy.Evidence.MaxBytes {
		return fail(ErrLimit, "policy")
	}
	return nil
}
