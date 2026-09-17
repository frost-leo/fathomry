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

import "go.temporal.io/sdk/internal"

// FathomryScopeActivityDescriptionV1 scopes full native getter lifetimes, including
// payload retrieval and caches. Foreign guards can only add restrictions. Keep
// owner private and use one admitted-call identity for immediate scope promotion.
func FathomryScopeActivityDescriptionV1(value *ActivityExecutionDescription, owner *FathomryScopeOwnerV1, guard func(func() error) error) *ActivityExecutionDescription {
	return internal.FathomryScopeActivityDescriptionV1(value, owner, guard)
}

// FathomryScopeWorkflowDescriptionV1 scopes full native getter lifetimes, including
// payload retrieval and caches. Foreign guards can only add restrictions. Keep
// owner private and use one admitted-call identity for immediate scope promotion.
func FathomryScopeWorkflowDescriptionV1(value *WorkflowExecutionDescription, owner *FathomryScopeOwnerV1, guard func(func() error) error) *WorkflowExecutionDescription {
	return internal.FathomryScopeWorkflowDescriptionV1(value, owner, guard)
}

// FathomryScopeNexusDescriptionV1 scopes full native getter lifetimes, including
// payload retrieval and caches. Foreign guards can only add restrictions. Keep
// owner private and use one admitted-call identity for immediate scope promotion.
func FathomryScopeNexusDescriptionV1(value *NexusOperationExecutionDescription, owner *FathomryScopeOwnerV1, guard func(func() error) error) *NexusOperationExecutionDescription {
	return internal.FathomryScopeNexusDescriptionV1(value, owner, guard)
}
