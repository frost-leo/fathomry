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

package client

import (
	"context"

	"go.temporal.io/sdk/internal"
)

// FathomryScopeWorkflowUpdateHandleV1 is a local opt-in ownership extension, not
// an upstream API. guard controls each Get, including cached decoding, and must
// call next only while its dependency scope remains owned. Identity getters are
// immutable snapshots. Native Nexus synchronous/asynchronous classification is
// preserved. No Workflow command, error, retry or service protocol is changed.
func FathomryScopeWorkflowUpdateHandleV1(handle WorkflowUpdateHandle, guard func(context.Context, any, func(context.Context, any) error) error) (WorkflowUpdateHandle, error) {
	return internal.FathomryScopeWorkflowUpdateHandleV1(handle, guard)
}
