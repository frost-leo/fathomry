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

// Package zap integrates Zap v1 through bounded native cores under resource
// ownership/admission and invocation's independent evidence protocol. It has no
// public framework API, business disposition, global logger or retry policy.
//
// Prepare OptionsV1 with Select, apply resource.WithLimits, assemble and Bind
// with a composition-owned evidence inbox. Log and Sync return read-only receipts;
// accepted write errors belong to those receipts and the independent inbox.
// With and Named derive non-owning facades with frozen supported native fields.
//
// JSON stdout/stderr and exclusive Linux local directories support multiple
// sinks. Files use size rotation, optional synchronous gzip and bounded retention.
// Maintenance failure seals a file sink; it is not reported as successful cleanup.
// Shutdown owns no detached workers and never closes borrowed streams/extensions.
//
// StructuredSink is an explicit trusted composition dependency for future
// observability: typed native entries/fields and call context are preserved.
// No exporter/backend is installed. Synchronous sinks can block beyond a deadline;
// cancellation is not evidence of absent writes or permission to release a lease.
// These process-local operations do not belong in Temporal Workflow logic.
package zap
