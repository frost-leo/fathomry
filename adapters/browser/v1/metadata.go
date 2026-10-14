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

package browser

import "github.com/frost-leo/fathomry/adapters/v1"

// HostPath is a path on the browser's host, not necessarily the Go caller's host.
// It does not upload or download bytes, resolve a local path, or transfer files.
type HostPath string

// Effect is an observation, never a retry instruction.
type Effect uint8

const (
	NotSubmitted Effect = iota
	Unknown
	Acknowledged
)

// Observation separates a returned operation from page load, HTTP status and cleanup.
type Observation struct {
	Returned        bool
	Effect          Effect
	ContextDisposed bool
	ContextDetached bool
	ProcessExited   bool
	Transferred     bool
	EOF             bool
	StreamClosed    bool
}

// Attribution identifies the generation actually retained, not the latest Follow value.
type Attribution struct {
	Runtime  string
	Sequence uint64
	Parent   uint64
	Source   adapters.Source
}
