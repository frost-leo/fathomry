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

// Package sqlengine owns the public budget and observation contracts shared by
// SQL-engine adapters. Concrete providers keep their own requests, rows, cursors,
// transactions, mutation effects and load evidence; there is no universal SQL
// client or native execution/cleanup authority in this package.
//
// Concrete provider Recommend functions return Policy for owned generations and
// operations. Applications compose public adapters/v1 runtimes and evidence
// inboxes with resource/v1 bindings. Source work and source evidence have separate
// lifetimes; neither disappearance of a waiter nor native source release is an
// acknowledgement of evidence. Declared bytes are not a hard process RSS limit.
//
// Info, Attribution, Fact and Profile are process-local observations with
// restricted presentation and no persistence contract. Returned mutable metadata
// belongs to its caller; Clone methods detach the nested slices. This package
// imports no database-layer contracts, Internal package, SDK or concrete provider.
// Metadata methods return values; normalize optional typed-nil metadata pointers
// to untyped nil before logging an absent observation.
package sqlengine
