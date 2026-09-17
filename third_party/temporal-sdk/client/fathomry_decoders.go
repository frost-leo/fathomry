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

// FathomryScopeOwnerV1 is an opaque local owner capability, not durable data.
type FathomryScopeOwnerV1 = internal.FathomryScopeOwnerV1

// NewFathomryScopeOwnerV1 creates a private owner capability. Other owners can
// add restrictions but cannot remove or replace this owner's decoder guard.
func NewFathomryScopeOwnerV1() *FathomryScopeOwnerV1 { return internal.NewFathomryScopeOwnerV1() }

// FathomryScopeErrorV1 is a local opt-in native lazy-decoder ownership extension.
// It preserves native error types, known cause graphs and original Failure data.
// Unknown custom errors remain caller-owned; custom error types with borrowed
// runtime decoders must implement FathomryScopeDecodersV1(func(func() error) error)
// error. Independently retained original aliases cannot be revoked by this copy.
func FathomryScopeErrorV1(cause error, owner *FathomryScopeOwnerV1, guard func(func() error) error) error {
	return internal.FathomryScopeErrorV1(cause, owner, guard)
}
