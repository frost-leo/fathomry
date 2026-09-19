/*
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

// Package configuration implements framework-owned configuration loading for
// independent business projects. Load accepts project/schema and
// acquisition contracts; adapters depend on this package, never the reverse.
//
// The project owns its data schema and selected sources. Fathomry performs
// explicit environment capture, strict layered preparation, immutable publication
// and safe provenance. Internal factories, SDKs and resource ownership do not
// become business-project assembly boilerplate.
//
// Go API compatibility and project SchemaVersion are different concerns.
// A declared schema mismatch is refused; there is no implicit data migration,
// wire snapshot or durable Run format.
//
// Loading is finite and synchronous, with caller-owned contexts. It does not
// discover projects, start watchers or initialize business clients/Workers.
// Providers and validators are trusted cooperative Go code, not a sandbox.
package configuration
