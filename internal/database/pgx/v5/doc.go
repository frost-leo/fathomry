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

// Package pgx supplies bounded PostgreSQL access using the pgx v5 native API
// and its native puddle pool dependency. It owns no business schema, public API,
// workflow definition, migration, registry, retry policy or durable ledger.
//
// # Composition and lifetime
//
// Select validates versioned OptionsV1 and optional resource layers. Composition
// applies resource.WithLimits, assembles selections, and calls Bind with a separate
// invocation.Inbox. LimitsV1 supplies a recommended policy for unoverridden
// options. Borrowed selections share the original pool, identity and allowance.
//
// Construction opens no connections and proves no server readiness. A subsequent
// explicitly authorized Query can check the service. Pool growth uses synchronous
// native CreateResource under a context-aware gate, rather than pgxpool's detached
// acquisition and error-discarding destructor. Warm connections execute concurrently;
// there is no periodic refill, ping, expiry, statement cache or background janitor.
//
// Query and Exec consume results before returning receipts. Their setup errors
// mean no call was accepted; accepted failures and partial data are in the receipt
// and independently in Inbox. Receivers must inspect and release every delivery.
// Raw text cells are immutable internally; copying getters expose deliberate
// sensitive data, never native Rows, Conn, TypeMap, scanners or rewriters.
//
// Begin retains one connection/root allowance. Statements use bounded nested calls;
// Commit and Rollback use the existing root evidence slot even at saturation.
// A transaction needs explicit finalization: canceling BEGIN does not roll it back.
// Concurrent transaction operations are refused. There is no automatic transaction
// retry, no savepoint API and no inference that a lost COMMIT response means failure.
//
// # Supported inputs and shutdown limits
//
// Only explicitly supplied IP/port/database/password and required SCRAM authentication
// are selected. TLS requires explicit roots and a verified server name; plaintext
// requires opt-in for isolated tests. PG-prefixed ambient settings are refused.
// ParserHome explicitly acknowledges native metadata probes, not file contents.
// Environment must remain static during preparation/construction; it is never rewritten.
//
// Only SELECT/INSERT/UPDATE/DELETE/WITH entries and a bounded set of plain argument
// types are accepted. Extended protocol enforces a single statement. Authorized SQL
// remains trusted application input, not a permission sandbox: session-changing
// functions, external-effect functions and persisted session state are outside
// this profile. Business authorization belongs to database roles/composition.
//
// Statement budgets bound cooperative native execution, not all cleanup time.
// Dead connections are joined through pgconn.CleanupDone while still owned;
// native asynchronous cleanup has its own approximately 15-second deadline.
// Root use is retained until cleanup finishes. Assembly.Close cannot close a live
// transaction or call; finalize them, then explicitly continue Close. A timed-out
// pool close retains one owned join operation and its continuation and prior errors.
//
// Count, retained-byte, protocol-message and evidence reservations are separate.
// No hard RSS, total server-traffic, arbitrary cause-graph or distributed quota
// guarantee is supplied. Compatibility profiles/build facts are not tested support.
// Protocol peers and opt-in real PostgreSQL tests are distinct evidence classes.
package pgx
