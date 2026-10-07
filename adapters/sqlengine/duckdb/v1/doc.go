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

// Package duckdb supplies an explicitly owned, local embedded DuckDB capability.
// Configuration and Recommend prepare strict settings and public operation/evidence
// policies without I/O. Open initializes the real engine and may create an explicitly
// authorized database file and WAL. A non-nil Owner always retains cleanup authority;
// Client and resource Handle are non-owning facades.
//
// Execute, Query, ExecuteMany, Append and ordered Transaction preserve finite native
// progress, exact provider-owned values, partial effects and independent evidence.
// They borrow requests until return and accept a separate rollback cleanup context.
// Read owns one SELECT-shaped result through bounded Next/Close, pins its source
// generation and never re-executes SQL. Setup cancellation does not revoke accepted
// reader ownership. Only observed EOF means complete result consumption.
//
// Required public evidence must be received and acknowledged incrementally, even
// when convenience results are ignored. Owner/runtime shutdown joins actual work
// using already-reserved cleanup evidence. Incomplete cleanup remains reachable.
// Framework Fixed/Follow composition uses these same public mechanisms; changing
// an in-memory source creates a different database, not migrated state.
//
// CGO and the selected native library are required. Incremental Go consumption does
// not eliminate native result materialization or establish a hard RSS bound. Raw
// JSON, composite and TIME result decoding, multi-statement SQL, EXPLAIN, external
// catalogs, extensions and automatic retries remain outside this supported profile.
package duckdb
