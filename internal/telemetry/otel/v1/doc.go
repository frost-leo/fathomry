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

// Package otel integrates bounded logs, traces and metrics with the native
// OpenTelemetry Go SDK and OTLP HTTP/protobuf exporters. Select prepares explicit
// OptionsV1, resource.Assemble owns construction and shutdown, and Bind connects
// a non-owning Client to an independent invocation.Inbox.
//
// Emit queues native log records; Start/End delimit owned spans; MeasureInt64
// and MeasureFloat64 use a fixed synchronous instrument manifest. Flush exports
// explicit batches and cumulative metric snapshots. Queue acceptance, receiver
// acknowledgement, required framework evidence and backend durability are
// distinct facts. No background exporter, global provider, environment discovery,
// workflow runtime, service deployment or public capability is installed.
//
// Callers drain evidence, end every span and arrange dependency lifetimes before
// closing the assembly. Contexts propagate W3C identifiers rather than SDK
// handles. This is process-local Activity/worker code, never deterministic
// Temporal Workflow logic. Logs API/SDK 0.22.0 remain beta despite this v1 path.
package otel
