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

// Package invocation implements process-local controlled calls and technical
// evidence handoff for framework integrations. It reuses resource's authoritative
// borrowing records and fault's technical context. It imports no SDK, exporter,
// orchestration or business-terminal policy. It is not Temporal Workflow code.
//
// # Admission, use and completion
//
// [Begin] reserves independent evidence capacity before resource admission;
// [BeginNested] retains an explicitly associated parent's resource use. An accepted
// [Call] remains owned even if its caller stops waiting on the [Receipt]. Use [Budget]
// for the actual native phase and retain a [Scope] with a [Guard] before handing
// work to a callback that may outlive the initiating function.
//
// Reporting and ending local use are separate facts: [Call.Resolve] records the
// primary result, [Call.Finish] finalizes technical/cleanup reporting, and
// [Call.Release] confirms the producer's own use ended. [Call.Complete] combines
// the required facts when both are known. [Call.Execute] additionally owns the
// supplied callback until it returns. A deadline does not prove remote rollback
// or authorize release of resources still needed by native work.
//
// # Evidence and policy boundaries
//
// [Inbox] retains bounded deliveries independently of direct caller error handling.
// Optional [Observer] events may be dropped and do not replace required evidence.
// Generic result values and native causes retain their declared borrowing semantics;
// integrations must freeze and bound them before handing them to another owner.
//
// Logical calls, observed native attempts, final reporting, release and business
// effects are not interchangeable counts. This package does not retry SDK calls,
// persist receipts, infer Run/Item disposition or implement a distributed quota.
// Runtime handles and results are not approved serialization/history formats.
package invocation
