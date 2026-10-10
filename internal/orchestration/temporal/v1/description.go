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

import "context"

// Capture only bounded target facts, not the operation's native error graph.
// Getter evidence must not reinterpret an omitted target as a known intention.
func descriptionIdentity(ctx context.Context, evidence *Execution) Execution {
	identity := Execution{WorkflowID: evidence.WorkflowID, ActivityID: evidence.ActivityID,
		NexusOperationID: evidence.NexusOperationID, RunID: evidence.RunID, IdentityOmitted: evidence.IdentityOmitted}
	if authority, ok := ctx.Value(nativeCallKey{}).(*nativeCall); ok && authority.identity != nil {
		intention := identity
		if authority.identity.apply(&identity) {
			identity = intention
			identity.IdentityOmitted = true
		}
	}
	return identity
}
