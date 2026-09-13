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

// Package zerolog integrates the selected zerolog v1 SDK as a bounded synchronous
// JSON logging capability for internal framework composition. It does not import
// Zap, define a common logging API, implement slog.Handler, or own an exporter,
// execution ledger, process termination policy or Temporal Workflow behavior.
//
// Select freezes OptionsV1 and explicit resource layers before opening outputs.
// Composition supplies resource.WithLimits, resource.Assemble and an independently
// owned invocation.Inbox; Bind returns a non-owning Logger. Borrowed byte and
// structured sinks are never closed. Owned files use an existing private dedicated
// directory, exclusive ownership, size/manual rotation, count retention and optional
// synchronous gzip. Normal close releases handles; failed/crashed file ownership
// requires explicit reconciliation rather than silently resuming uncertain files.
//
// Log validates a closed subset of slog.Attr values and canonical group structure,
// freezes technical source/correlation metadata and passes per-call context to
// RecordWriter. It exposes no native logger, pooled event, hook or writer handle.
// Accepted operations retain per-sink outcomes independently of caller error
// handling and optional payload-free observation. Inspect the receipt's Result.Err
// and Result.SinksCopy; nil setup error is not delivery success.
// Shared SDK attempts remain unobserved; per-sink entries/acceptance and local
// maintenance results are not SDK-attempt counts or bounds on opaque retries.
//
// One admitted operation serializes the selected sinks. Input, output, queue,
// evidence and file bounds are explicit; original native causes and externally
// retained records remain their owners' responsibility. There are no background
// logging workers, automatic retries or hidden output defaults. Context cancellation
// stops waiting and later native entry, not a blocked io.Writer or kernel file I/O.
// Native global Disabled and non-JSON builds are refused explicitly, never reported
// as delivery. Log acceptance and Sync do not establish durable business effects.
package zerolog
