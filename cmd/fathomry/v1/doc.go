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

// Package cli exposes shared Fathomry command entries.
// Run supports help, explicit language/output selection, bounded diagnostics,
// cancellation and resource-cleanup ownership. It constructs no service clients.
//
// RunProject composes configuration checks for a compiled project schema through
// private command groups/leaves and Framework's configuration-only Resource binding.
// It adds no alternate invocation owner, cleanup engine or process proxy.
// ConfigurationCheckError preserves accepted data on a later failure; its cause
// retains the original ExecutionError and cleanup continuation.
// Project generation/build inspection, workflow, database/schema and catalog-query
// commands remain unavailable here. This package has no project registration,
// application Boot, command plugin or replacement execution protocol.
//
// Contexts, streams and optional prepared catalogs are borrowed for one invocation.
// Callers must synchronize shared writers. No settings or process globals are set.
package cli
