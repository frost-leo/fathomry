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

// Package cli provides the first-party Fathomry command entry points.
//
// Run borrows explicit invocation inputs and never exits or changes signal policy.
// Main owns process arguments, streams, signals and exit. Only root/help is shipped;
// command construction and extension are private. Independent Run calls have fresh
// parser state; callers remain responsible for shared streams and callbacks.
// Runtime acceptance is Go 1.27 on Linux. Arbitrary blocked I/O is not interruptible
// by Run, and a delivery failure says nothing about remote effects.
package cli
