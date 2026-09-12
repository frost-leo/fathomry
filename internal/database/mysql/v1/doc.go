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

// Package mysql provides bounded MySQL 8 protocol access with the unmodified
// go-sql-driver/mysql v1 SDK and pinned database/sql connections.
//
// Select freezes explicit OptionsV1; composition attaches LimitsV1 (or a matching
// resolved policy), assembles resource ownership, and Bind joins an independently
// owned invocation.Inbox. Construction does not establish readiness or authorize
// remote writes. No public API, schema, ORM, migrations or generic database facade
// is supplied.
//
// The per-connection transport owns verified TLS upgrade and MySQL framing before
// native allocation. Packet/response/row/metadata bounds apply while draining too;
// LOCAL INFILE requests are rejected before SDK registry dispatch. TLS handshake
// memory, OS buffers and total process RSS are not the MySQL packet-byte bound.
// Authentication remains native; no SDK fork, global registry mutation or raw
// handle/callback surface is exposed.
//
// database/sql.DB owns native pooling, including idle/lifetime expiration. Stats
// exposes counters, not pool authority. Query/Exec use native direct dispatch,
// with native preparation for parameters; Prepare retains a reusable statement.
// All native statements run once under a pinned connection's Raw lock, without
// database/sql's statement retry loops. No raw handle escapes. Results are frozen.
// Setup errors have no receipt. Accepted failures/partial data are retained in the
// receipt and independent inbox; receivers must inspect and release deliveries.
// Copies deliberately expose SQL data; ordinary diagnostics are restricted.
//
// Begin's caller context owns the entire bounded transaction lifetime, including
// automatic rollback. One retained guard joins the integration's lifetime worker;
// native finalizer results are captured before database/sql discards them.
// Finalization requires no additional admission or evidence slot. Statement
// errors retain native semantics: an owned Ping checks server transaction status
// before allowing continued work, rather than inventing PostgreSQL-style abortion.
// Lost commit replies remain unknown and mutations are never replayed by this API.
//
// SQL is structurally bounded, not filtered by keywords or a SQL sandbox.
// Database roles and composition own authorization and InnoDB table, trigger,
// implicit-commit and session-state constraints. No migrations, multi-statements,
// stored-procedure result sets, LOAD DATA, replication, failover or XA are supplied.
// Release joins live constructors and connection retirement, including work
// already removed from pool statistics. SDK/stdlib private helper goroutines have
// no exported join handles and can finish their non-I/O bookkeeping afterward.
// No immediate remote cancellation, external rollback or durability is inferred.
package mysql
