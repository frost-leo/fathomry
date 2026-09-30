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

// Package failure defines numeric, component-owned public failures.
//
// Code is a uint32 customer-failure identity with an allocated subsystem Facility
// and local Number. Domains classify capability bands such as database, cache and
// object storage, independently of code layers. Before adding error numbers, read
// the allocation rules on Code, Facility and Domains; do not infer numbering from
// package layout. Identifier is a readable symbol. Declare Definition values in
// the owning component, then use New or NewDetailed to
// construct an occurrence with an operation/instance Location and original causes.
// Catalog preparation is explicit and is not required for construction or matching.
//
// Error owns frozen metadata; Detailed uses a component-owned copy contract for
// its extensible data rather than restricting Go field types. Native causes
// remain the exact borrowed Go objects: errors.Is/As can inspect them, but ordinary
// diagnostics never format them. Inspect selects only a directly supplied
// occurrence; it does not guess a primary error inside wrappers or joins.
//
// Prepare builds an immutable, collision-checked definition atlas with numeric,
// symbolic and component queries. Definitions contain safe developer explanations,
// not per-call payloads or localized text. Code text/JSON is lossless hexadecimal.
// Runtime errors refuse JSON; a native error graph is not a durable wire schema.
//
// This package imports only the standard library. It performs no SDK work,
// settings lookup, localization, retry, logging, discovery or global registration.
// Component contracts own detail meanings, native-cause exposure and recovery.
package failure
