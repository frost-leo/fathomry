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

// Package version describes software releases and consuming Go builds, not
// business Workflow versions, compatibility policy or authenticated provenance.
//
// Parse produces immutable comparable version values. Inspect reads the running
// application and validates the optional four-field linker declaration.
// FromBuildInfo normalizes caller-supplied runtime/debug or debug/buildinfo data;
// it never consults the running process or its linker declaration. WithDeclaration
// attaches an explicit caller claim without replacing native reported facts.
//
// Build owns immutable data. Snapshot returns independently mutable slices;
// callers must not mutate borrowed requests or BuildInfo during normalization.
// No operation runs Git, discovers CI variables, reads a clock or starts goroutines.
// Machine records have no persistence protocol. The optional presentation
// subpackage owns resource-first localized summaries and error messages.
package version
