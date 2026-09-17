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

// Package temporal integrates native Temporal Clients, managed Workers and
// process-side task evidence with named resources and bounded admission.
// Workflow definitions execute through the unchanged deterministic SDK APIs.
//
// Select prepares explicit configuration, composition adds resource.WithLimits
// and assembles it, and Bind joins an independently owned evidence Inbox. Native
// WorkflowService and OperatorService methods remain typed, with explicit grants
// rather than blanket administrative authority. RPC acknowledgement, remote
// execution completion and external business effects remain different facts.
//
// BindExecutions supplies Workflow and Standalone Activity operations and owned
// Worker startup. Workers register native Workflow/Activity definitions and Nexus
// services, keep dependencies until actual native cleanup, and independently
// record Activity, Local Activity and Nexus callback outcomes. Process-local
// admission and evidence functions must never run in deterministic Workflow code.
// Native combined starts and result handles distinguish request acknowledgement,
// Update outcomes and execution-chain identities. Identity getters perform no I/O;
// result waits are separately admitted, and canceling one does not cancel remote work.
// Schedule operations preserve native time/concurrency semantics; synchronous
// pagination and update callbacks retain ownership until they return, not until
// their caller stops waiting. An update response does not certify applied state.
// SelectWithRuntime borrows a native SDK logger; concrete logging adapters and
// delivery policy belong above this package. Logger dependencies must outlive
// native use. The SDK retains Workflow replay suppression and optional log methods.
// Optional transport extensions, contrib backends and service feature gates have
// explicit profile limits; native API availability is not service qualification.
package temporal
