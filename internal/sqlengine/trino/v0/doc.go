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

// Package trino provides independently selected, bounded SQL statements and
// finite direct-protocol results using the official trino-go-client major 0.
// It imports no other engine or table-format integration.
//
// Select freezes OptionsV1; composition attaches LimitsV1, assembles the Source,
// owns an invocation.Inbox[Result], and binds a non-owning Client. Assembly
// readiness executes SELECT version(), not the native no-network Ping.
// Query, Execute and Insert synchronously return receipts whose technical and
// cleanup failures also remain in the independent evidence inbox.
//
// Each statement owns a fresh native logical connection, statement and HTTP
// transport. No mutable SQL session, database/sql pool, raw SDK handle, callback,
// or live Rows escapes. Contexts are caller-owned: work and cleanup are separate.
// Results preserve bounded direct JSON rows and exact type metadata rather than
// silently applying lossy native datetime or numeric conversion. Consumers own
// copies after return; slow consumers hold no native resources.
//
// One statement is one physical batch, never automatically split or replayed.
// DDL, CTAS and ordinary DML are not restricted to append-only, but connector
// support and privileges require separate qualification. Managed transactions,
// session-changing SQL, spooling, arbitrary headers/redirects, Kerberos and
// per-statement credentials are excluded. Single-table commit semantics do not
// establish cross-statement atomicity or safe retry after a lost acknowledgement.
// This process-local integration is not a Temporal Workflow or durable ledger.
package trino
