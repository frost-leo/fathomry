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

package objectstore

import "github.com/frost-leo/fathomry/adapters/v1"

// Budget bounds a root family and each independent result in declared bytes,
// not measured heap. Concrete Using facades reject larger source generations.
type Budget struct {
	WorkBytes     int64 `json:"work_bytes"`
	EvidenceBytes int64 `json:"evidence_bytes"`
}

// Policy is a provider's explicit recommendation, including source owners,
// retained sessions, active and queued work, incremental children and evidence.
// It never changes an existing caller-owned runtime. Long sessions require
// incremental reception rather than an ever-growing evidence inbox.
type Policy struct {
	Budget   Budget
	Runtime  adapters.Options
	Evidence adapters.EvidenceOptions
}

// ForGenerations multiplies source/root/queue/evidence ceilings by count (1..16)
// for overlapping equivalent generations. Individual root bounds are unchanged.
// It checks arithmetic limits, not provider compatibility or measured memory.
func (policy Policy) ForGenerations(count int) (Policy, error) {
	if count < 1 || count > 16 || policy.Budget.WorkBytes < 1 || policy.Budget.WorkBytes > 1<<40 || policy.Budget.EvidenceBytes < 1 || policy.Budget.EvidenceBytes > 1<<40 ||
		policy.Runtime.MaxActive < 1 || policy.Runtime.MaxActive > 1024/count || policy.Runtime.MaxQueued < 0 || policy.Runtime.MaxQueued > 4096/count ||
		policy.Runtime.MaxWorkBytes < 1 || policy.Runtime.MaxWorkBytes > (1<<40)/int64(count) || policy.Runtime.MaxQueuedBytes < 0 || policy.Runtime.MaxQueuedBytes > (1<<40)/int64(count) ||
		policy.Evidence.Capacity < 1 || policy.Evidence.Capacity > 65536/count || policy.Evidence.MaxBytes < 1 || policy.Evidence.MaxBytes > (1<<40)/int64(count) {
		return Policy{}, adapters.ErrOptions
	}
	policy.Runtime.MaxActive *= count
	policy.Runtime.MaxQueued *= count
	policy.Runtime.MaxWorkBytes *= int64(count)
	policy.Runtime.MaxQueuedBytes *= int64(count)
	policy.Evidence.Capacity *= count
	policy.Evidence.MaxBytes *= int64(count)
	return policy, nil
}
