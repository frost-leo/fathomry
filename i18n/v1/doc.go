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

// Package i18n gathers component-owned message resources and error definitions into
// immutable, explicit catalogs. Prepare reads only supplied filesystems; components
// choose their base locale. Adding a locale file needs no engine-language switch.
//
// Catalog queries, coverage and Explain use an explicit locale without settings or
// runtime details. Presenter reads typed preferences from settings, or uses a bound
// override, without changing business function signatures. Present freezes text,
// retains the exact original error, and reports localization failures separately.
//
// Detail types remain component-owned. Optional projectors explicitly select safe
// builtin scalar arguments; the engine never reflects arbitrary error fields or
// formats native causes. Output is plain text requiring channel-owned escaping.
//
// The bounded scalar-cardinal/v1 profile is qualified against x/text v0.41.0 and
// CLDR 32. Resource/API/detail revisions are separate. No global catalog, implicit
// filesystem discovery, locale environment lookup, SDK client or Watch is installed.
// Runtime handles are not a durable error protocol. Do not consult mutable process
// preferences in deterministic Temporal Workflow code.
package i18n
