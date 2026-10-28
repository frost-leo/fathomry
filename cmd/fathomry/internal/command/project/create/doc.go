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

// Package create owns the official tool's offline project-new leaf.
// It renders one fixed six-file profile and creates an absent child of the
// caller's trusted, stable cwd. It neither resolves dependencies nor loads
// configuration, registers workflows or starts services.
//
// Creation is serial and cancellation cooperative. Exclusive claims preserve
// existing entries; acknowledged partial effects survive failure. File completion,
// handle cleanup and output delivery are separate facts. No rollback, crash
// durability or hostile namespace-rebinding protection is promised.
//
// The generated executable uses the existing public CLI, not this package.
// The profile is qualified on Linux local filesystems; new platforms and
// template/dependency pairs require explicit qualification.
package create
