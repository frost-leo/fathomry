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

package internal

import (
	"context"
	"errors"
)

// FathomryScopeWorkflowUpdateHandleV1 is a local, opt-in ownership bridge. It
// preserves the completion classification used by Temporal-backed Nexus helpers
// without exposing the unguarded handle through the returned object.
func FathomryScopeWorkflowUpdateHandleV1(handle WorkflowUpdateHandle, guard func(context.Context, any, func(context.Context, any) error) error) (WorkflowUpdateHandle, error) {
	if handle == nil || guard == nil {
		return nil, errors.New("temporal: update handle and ownership guard are required")
	}
	return &fathomryScopedUpdateHandle{
		workflowID: handle.WorkflowID(), runID: handle.RunID(), updateID: handle.UpdateID(),
		completed: IsUpdateWorkflowCompleted(handle),
		get:       func(ctx context.Context, output any) error { return guard(ctx, output, handle.Get) },
	}, nil
}

type fathomryScopedUpdateHandle struct {
	workflowID, runID, updateID string
	completed                   bool
	get                         func(context.Context, any) error
}

func (handle *fathomryScopedUpdateHandle) WorkflowID() string { return handle.workflowID }
func (handle *fathomryScopedUpdateHandle) RunID() string      { return handle.runID }
func (handle *fathomryScopedUpdateHandle) UpdateID() string   { return handle.updateID }
func (handle *fathomryScopedUpdateHandle) Get(ctx context.Context, output any) error {
	return handle.get(ctx, output)
}
