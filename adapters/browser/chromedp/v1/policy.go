/*
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

package chromedp

import (
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

func policyFor(b native.Budget) browser.Policy {
	budget := browser.Budget{WorkBytes: b.WorkBytes + int64(2*b.Pages)*b.EvidenceBytes, EvidenceBytes: b.EvidenceBytes + 65536, SourceWorkBytes: b.SourceBytes, SourceEvidenceBytes: 65536}
	capacity := 1 + (2*b.Pages+b.Streams+2*b.Subscriptions+b.Bindings+b.BindingCalls+b.ObjectGroups+1)*b.Active
	policy := browser.Policy{Budget: budget, Runtime: adapters.Options{MaxActive: 1 + b.Active, MaxQueued: b.Queued, MaxWorkBytes: budget.SourceWorkBytes + int64(b.Active)*budget.WorkBytes, MaxQueuedBytes: int64(b.Queued) * budget.WorkBytes, MaxTasks: 2*b.Pages + b.Streams + 2*b.Subscriptions + b.Bindings + b.BindingCalls + b.ObjectGroups + 3, MaxDepth: 3, MaxHolds: b.Pages + b.Streams + 3}, Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: budget.SourceEvidenceBytes + int64(capacity-1)*budget.EvidenceBytes}}
	return policy
}
func Recommend(settings Settings) (browser.Policy, error) {
	p, err := Prepare(settings, NativeOptions{})
	return p.policy, err
}
func (p Prepared) Policy() browser.Policy { return p.policy }
