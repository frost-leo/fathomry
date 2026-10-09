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

package otel

import "github.com/frost-leo/fathomry/resource/v1"

// StableDestination reports whether this capability cannot follow replacement
// sources between calls: a direct captured source or a Fixed resource binding.
// It acquires no lease, work, evidence or ownership. True does not imply that a
// source exists yet, remains alive, or can admit an operation; explicit owners
// must preserve those conditions. Closing a source does not change this routing
// fact. A nil, zero or Follow client returns false.
func (client *Client) StableDestination() bool {
	if client == nil || client.lifetime == nil {
		return false
	}
	if client.source == nil {
		return client.direct.state != nil
	}
	status, err := client.source.Inspect()
	return err == nil && status.Policy == resource.Fixed
}
