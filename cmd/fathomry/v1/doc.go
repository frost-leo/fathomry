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

// Package cli exposes the retained minimal Fathomry CLI foundation.
// Run supports help, explicit language/output selection, bounded diagnostics,
// cancellation and resource-cleanup ownership. It constructs no service clients.
//
// Framework, project generation/build inspection, workflow, database/schema and
// catalog-query commands are withdrawn. This package has no project registration,
// application Boot, command plugin or replacement execution protocol.
//
// Contexts, streams and optional prepared catalogs are borrowed for one invocation.
// Callers must synchronize shared writers. No settings or process globals are set.
package cli
