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

// Package compatibility reports available consuming-binary facts and assesses
// explicitly supplied, exact-combination test evidence. It owns no SDK selection,
// service probe, resource, global registry, deployment identity or retry policy.
// Build metadata is neither an integrity attestation nor a compatibility test.
//
// # Facts, records and decisions
//
// [Inspect] reads the running executable; [FromBuildInfo] normalizes explicitly
// supplied metadata and treats nil as unavailable. [Build] separates the main
// application, framework, selected SDKs and replacements. Local replacement paths
// are not exposed, and absent build facts are not inferred from this repository's
// dependency declarations.
//
// Derive a safe [Profile] from the same effective settings used to construct the
// resource. [Assess] combines its authoritative source/limits with [Requirement]
// and reviewed [Record] inputs. [Report.Require] applies explicit [Policy] without
// rewriting facts; a permissive policy cannot turn known incompatibility into
// support. Keep the report when the policy refuses the combination.
//
// Returned metadata owns its copied slices, but caller-supplied records and build
// descriptions are not authenticated. These bounded in-process projections refuse
// JSON persistence and do not define deployment attestation, Workflow versioning
// or a default service-support catalog.
package compatibility
