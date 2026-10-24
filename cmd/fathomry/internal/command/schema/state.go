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

package schema

// State reports bounded manifest consistency, not authenticated origin or business
// readiness. Indeterminate is an unavailable observation, never an empty target.
type State string

const (
	Indeterminate State = "indeterminate"
	NotInstalled  State = "not_installed"
	Current       State = "current"
	Incompatible  State = "incompatible"
	Drifted       State = "drifted"
	Incomplete    State = "incomplete"
	Unmanaged     State = "unmanaged"
	Changing      State = "changing"
)

// Status is detached data. Reason is a fixed machine token without native SQL,
// target address, credentials, journal rows or payload differences.
type Status struct {
	State    State  `json:"state"`
	Family   string `json:"family"`
	Dialect  string `json:"dialect"`
	Revision string `json:"revision"`
	Reason   string `json:"reason"`
}

// Step identifies an original embedded SQL asset without exposing mutable SQL.
type Step struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
}

// Plan is a detached observation, not a future installation reservation. Current
// state is applicable with no steps; non-current refusal is not an empty catalog.
type Plan struct {
	Status     Status `json:"status"`
	Applicable bool   `json:"applicable"`
	Steps      []Step `json:"steps"`
}

// Effect separates observed facts from the original attempt's acknowledgement.
type Effect string

const (
	NotStarted Effect = "not_started"
	Unchanged  Effect = "unchanged"
	Applied    Effect = "applied"
	RolledBack Effect = "rolled_back"
	Unknown    Effect = "unknown"
)

// Change survives cleanup/error independently. LastAcknowledgedStep is empty when
// no SQL asset's successful response was observed; journal position is not used
// to infer that response. CleanupComplete describes local retained-work release.
type Change struct {
	Before               Status `json:"before"`
	After                Status `json:"after"`
	Effect               Effect `json:"effect"`
	LastAcknowledgedStep string `json:"last_acknowledged_step"`
	CleanupComplete      bool   `json:"cleanup_complete"`
}
