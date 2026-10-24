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

// Package cli exposes the fixed Fathomry command set to the official executable
// and independent projects. Run uses the existing internal command engine; Handle
// lets a project keep its application startup path while handling the same commands
// and the offline inspection protocol before configuration Boot.
// Project vet defaults to all registered workflows and can select an exact
// Namespace/code/version/mode/revision. Build inspection always checks the full
// registration set, including supplied catalog metadata. Explicit Bundle input
// retains the existing offline workflow-vet contract.
//
// This is not a command-registration or plugin SDK. Projects supply data-only
// workflow declarations, not callbacks to the builder. No concrete Adapter catalog
// is imported here. A nil Errors/Messages pair selects the core and CLI catalogs;
// the official executable can supply its explicitly prepared wider catalog.
//
// Contexts, streams, project data and prepared catalogs are borrowed. Do not mutate
// inputs during a call; synchronize shared writers. No process globals, application
// settings, service clients, or runtime are initialized by command assembly.
// Project.Control is a lazy DatabaseResolver returning DatabaseConfiguration for explicit online schema
// commands. Those commands own Framework resources through the invocation scope.
// ExecutionError retains mutation facts and unfinished cleanup on failure.
// Private control.go composes this ownership and injects lazy database operations into the
// leaf commands; it is neither another command group nor an exported plugin hook.
package cli
