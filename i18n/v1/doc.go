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

// Package i18n prepares immutable, explicit catalogs of feature-owned resources.
// Prepare validates the complete English/translation set atomically. Inspect and
// Lookup return owned definitions; Resolve selects an opaque resource for Render.
// Locale is explicit per call, never ambient. Callers may share catalogs and
// selections concurrently but must not overwrite handles or mutate inputs during
// calls. Render arguments are not retained.
//
// Optional WithModules groups exact resource owners explicitly. PrepareBindings
// checks owner-declared failure/input contracts against message contracts;
// BoundSelection renders approved scalar projections without extracting errors.
// Neither plain resource use nor failure construction requires registration.
//
// Version 1 supports strict UTF-8 JSON scalar-cardinal/v1 resources, named builtin
// scalars and one optional uint64 count. It uses pinned x/text language/cardinal
// rules, not native message formatting. Plain text needs channel-owned escaping.
// Failures are locale-independent failure/v1 occurrences with no input payloads.
// Resources, message semantics, engine/data revisions and the Go API have
// independent versions. No durable protocol or ambient global registry is provided.
package i18n
