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

package lark

import (
	"time"

	"github.com/frost-leo/fathomry/adapters/notification/v1"
)

// Policy reuses shared accounting and adds the frozen receiver lifetime. Compose
// selects the strictest positive lifetime, deliberately shortening longer or
// unbounded source profiles in that explicit composition.
type Policy struct {
	notification.Policy
	ReceiverLifetime time.Duration
}

const publicMetadataBytes int64 = 64 << 10

// Recommend is offline. Native-aware construction uses Prepare.Policy.
func Recommend(value Settings) (Policy, error) {
	prepared, err := Prepare(value, NativeOptions{})
	if err != nil {
		return Policy{}, err
	}
	return prepared.Policy()
}

// Compose covers exactly these overlapping sources, including retired generations.
// The same Evidence declaration can size the separate Incoming inbox for Listen.
// Budgets are declared envelopes; composition never resizes caller-owned resources.
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
		work := value.WorkBytes + 2*value.EvidenceBytes + publicMetadataBytes
		evidence := value.EvidenceBytes + publicMetadataBytes
		tasks := 1
		if value.WebSocket {
			if value.ReceiverLifetime > 0 && (result.ReceiverLifetime == 0 || value.ReceiverLifetime < result.ReceiverLifetime) {
				result.ReceiverLifetime = value.ReceiverLifetime
			}
			work += int64(value.Pending+2) * (2*value.EventBytes + 2*value.AckBytes + 2*publicMetadataBytes)
			evidence = max(evidence, int64(value.Pending+2)*(value.EventBytes+value.AckBytes)+publicMetadataBytes)
			tasks = 4*value.Pending + 4
			result.Evidence.Capacity += 4*value.Pending + 4
		}
		result.Runtime.MaxTasks = max(result.Runtime.MaxTasks, tasks)
		result.Budget.WorkBytes = max(result.Budget.WorkBytes, work)
		result.Budget.EvidenceBytes = max(result.Budget.EvidenceBytes, evidence)
		if result.Budget.AdmissionTimeout == 0 || value.Timeout < result.Budget.AdmissionTimeout {
			result.Budget.AdmissionTimeout = value.Timeout
		}
		result.SourceWorkBytes += value.SourceBytes + publicMetadataBytes
		result.SourceEvidenceBytes += publicMetadataBytes
		result.Runtime.MaxActive += 1 + value.Limits.Active
		result.Runtime.MaxQueued += value.Limits.Queued
		result.Evidence.Capacity += 1 + 2*value.Limits.Active + value.Limits.Queued
	}
	result.Runtime.MaxDepth, result.Runtime.MaxHolds = 2, 2
	result.Runtime.MaxWorkBytes = result.SourceWorkBytes + int64(result.Runtime.MaxActive-len(values))*result.Budget.WorkBytes
	result.Runtime.MaxQueuedBytes = int64(result.Runtime.MaxQueued) * result.Budget.WorkBytes
	result.Evidence.MaxBytes = result.SourceEvidenceBytes + int64(result.Evidence.Capacity-len(values))*result.Budget.EvidenceBytes
	if result.Runtime.MaxActive > 1024 || result.Evidence.Capacity > 65536 || result.Runtime.MaxWorkBytes > 1<<40 || result.Runtime.MaxQueuedBytes > 1<<40 || result.Evidence.MaxBytes > 1<<40 {
		return Policy{}, fail(ErrLimit, "policy")
	}
	return result, nil
}
