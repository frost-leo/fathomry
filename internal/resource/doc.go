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

// Package resource prepares explicitly supplied configuration and manages named
// resource instances at the framework composition boundary. It imports no service
// SDKs and performs no environment/file discovery or business disposition.
//
// # Preparation and resource ownership
//
// Use [Prepare] to validate and freeze a [Schema] and [Input] before construction.
// [Select] preserves the exact settings/capability types; [WithLimits] attaches
// admission policy to that selection. [Assemble] receives separate initialization
// and cleanup contexts and validates the selection set before running factories.
// [Bind] returns the selected capability but does not wrap its operations;
// integrations use [AccessFor] and [Access.Acquire] for controlled native entry.
//
// [Borrow] and [Delegate] refer to the same authoritative resource record rather
// than create another client or independent allowance. A retained [Lease] protects
// its resource until all descendant uses end. SDK operations, callbacks and
// returned capabilities have their own copying and concurrent-use obligations.
//
// # Completion and diagnostics
//
// [ReleaseResult] distinguishes quiescence, release and cleanup errors. Cancellation
// alone establishes none of those facts. An incomplete release supplies an explicit
// continuation; callers retain a non-nil [Assembly] even when construction or Close
// returns an error. Cleanup is cooperative, and earlier cleanup causes remain
// inspectable after eventual completion.
//
// [Prepared] values and configuration provenance use independent storage. Native
// handles and errors are not generically deep-copied. Source/scope labels must
// exclude secrets. The sibling invocation package builds typed completion and
// independent evidence handoff on these records; neither package selects business
// outcomes, discovers configuration sources or defines a durable runtime format.
package resource
