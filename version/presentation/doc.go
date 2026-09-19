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

// Package presentation owns optional version summaries and failure messages.
// Compose Resources with caller-owned i18n resources using i18n.Prepare, then
// call Summary or Error with an explicit locale. There is no global catalog or
// locale. Returned resources are independent copies; prepared catalogs support
// concurrent readers. Rendering failures never modify machine version data.
//
// Messages are plain text, not escaped for a terminal, HTML or another channel.
// The summary deliberately describes application declarations and Go targets,
// not a dependency inventory, an official release or verified provenance.
package presentation
