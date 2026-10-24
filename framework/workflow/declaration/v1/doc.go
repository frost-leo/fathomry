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

// Package declaration validates and compiles project-owned workflow declarations.
// Compile accepts concrete Go data; Parse verifies a complete portable bundle.
// Both use the same closed finite-DAG semantics, exact control values and
// canonical comparison rules. Check compares explicitly supplied bundles only.
// ContentsCopy detaches canonical inner documents; ParseContents reconstructs a
// selected set with exact parents and additionally verifies stored-byte canonicality.
//
// Compilation creates no clients, settings, workers or process registries and
// calls no business functions. Callers must not mutate declarations during Compile;
// successful bundles own immutable content and expose detached bytes and reports.
// Contexts are caller-owned; operations are finite and spawn no goroutines.
//
// Acceptance proves static declaration consistency, not source freshness, complete
// project export, handler correctness, effect evidence, registration or readiness.
// Business versions, wire formats, semantics and fingerprint profiles are distinct.
// No runtime scheduling, override resolution or artifact qualification is provided.
package declaration
