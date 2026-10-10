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

package temporal

import (
	"github.com/frost-leo/fathomry/adapters/orchestration/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
)

const publicRecordBytes int64 = 8192
const publicUseBytes int64 = 4096

// Budget bounds the selected source profile admitted through a resource binding.
// It is a declared working/evidence envelope, not native heap or a remote quota.
type Budget = orchestration.Budget

// Policy covers one source and independently draining operation/Worker/task
// receivers. Concurrent generations require explicit composition of these costs.
type Policy = orchestration.Policy

func (prepared Prepared) nativeLimits() source.Limits {
	meta := prepared.metadata
	work := max(int64(prepared.settings.FamilyLimit)*meta.WorkBytes, prepared.settings.WorkerWorkBytes)
	return source.Limits{Active: meta.MaxActive, Queued: meta.QueuedCalls,
		Bytes: int64(meta.MaxActive) * work, QueuedBytes: int64(meta.QueuedCalls) * work,
		MaxLeases: prepared.settings.FamilyLimit}
}

// Recommend prepares the data-only profile. For native extensions use Prepare
// followed by Policy, so budgets and construction refer to exactly the same input.
func Recommend(value Settings) (Policy, error) {
	prepared, err := Prepare(value, NativeOptions{})
	if err != nil {
		return Policy{}, err
	}
	return prepared.Policy()
}
func (prepared Prepared) Policy() (Policy, error) {
	meta := prepared.metadata
	if meta.WorkBytes < 1 || meta.MaxActive < 1 {
		return Policy{}, fail(ErrState, "policy")
	}
	count := prepared.settings.InnerEvidenceCapacity
	family := int64(prepared.settings.FamilyLimit)
	nativeWork := family * meta.WorkBytes
	bridge := family * (2*meta.EvidenceBytes + publicRecordBytes)
	work := nativeWork + bridge
	worker := prepared.settings.WorkerWorkBytes + bridge
	sourceBytes := meta.SourceBytes + int64(prepared.settings.MaxUses)*publicUseBytes +
		int64(count)*(3*meta.EvidenceBytes+meta.RPCEvidenceBytes) + publicRecordBytes
	value := Policy{Budget: Budget{WorkBytes: work, WorkerBytes: worker, EvidenceBytes: publicRecordBytes},
		NativeWorkBytes: nativeWork, NativeWorkerBytes: prepared.settings.WorkerWorkBytes,
		SourceWorkBytes: sourceBytes, SourceEvidenceBytes: publicRecordBytes,
		Runtime: adapters.Options{MaxActive: 1 + meta.MaxActive, MaxQueued: meta.QueuedCalls,
			MaxWorkBytes:   sourceBytes + int64(meta.MaxActive)*max(work, worker),
			MaxQueuedBytes: int64(meta.QueuedCalls) * max(work, worker), MaxTasks: prepared.settings.FamilyLimit, MaxDepth: 8, MaxHolds: 2},
		Evidence: adapters.EvidenceOptions{Capacity: 1 + count, MaxBytes: int64(1+count) * publicRecordBytes},
		Workers:  adapters.EvidenceOptions{Capacity: count, MaxBytes: int64(count) * publicRecordBytes},
		Tasks:    adapters.EvidenceOptions{Capacity: count, MaxBytes: int64(count) * publicRecordBytes}}
	if value.Runtime.MaxActive > 1024 || value.Runtime.MaxWorkBytes > 1<<40 || value.Runtime.MaxQueuedBytes > 1<<40 {
		return Policy{}, fail(ErrLimit, "policy")
	}
	return value, nil
}
