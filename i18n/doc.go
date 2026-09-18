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

// Package i18n prepares immutable, resource-first plain-text catalogs for SDK
// consumers. Prepare validates caller-supplied JSON resources; Catalog.Render
// selects an explicit locale and renders with named, bounded scalar arguments.
// English source is mandatory; optional translations may fall back to English
// with explicit metadata. All authored wording belongs in resource files.
//
// Catalogs own their prepared state and support concurrent readers without
// resource I/O, global registration or background work. Callers must not mutate
// resource bytes during Prepare or argument maps during Render. Localized text
// is not escaped for HTML, terminals or notification channels.
//
// Resource profile, message contracts, source fingerprints, resource snapshots
// and actual renderer builds are separate compatibility axes. This package does
// not provide workflow integration, historical output reconstruction or remote
// resource loading. See docs/reference/i18n/interface.md for the resource profile.
package i18n
