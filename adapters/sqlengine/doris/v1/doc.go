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

// Package doris exposes the complete selected Doris SQL/HTTP profile using public
// operation/resource contracts and shared SQL-engine metadata. API major 1 is
// independent of the MySQL driver, configuration format and Doris service versions.
//
// Validate/Recommend prepare typed Settings without service I/O. Open owns one
// local source; close every non-nil Owner, including partial construction. Client,
// Handle and Using are non-owning. Framework Fixed/Follow callers retain the same
// public mechanisms; active cursors never migrate to a replacement generation.
//
// Query, Exec, StreamLoad and InspectLabel return receipts plus admission errors.
// After acceptance, inspect receipt value, primary, cleanup and actual release;
// independently receive and acknowledge required evidence. Handled errors never
// acknowledge evidence. QueryCursor separates setup and owning lifetime, Next
// reports bounded pages, and Close reuses the root reservation even at saturation.
//
// SQL acknowledgement, load commit/visibility, row quality, duplicate labels,
// unknown effects and local cleanup are distinct. A visible label does not verify
// this payload. No mutation replay, session/transaction engine or backend SDK is
// added. Exact copied values and deliberate native-error inspection are sensitive;
// runtime JSON is refused. Real-service checks require explicit isolated fixture
// authority; local peers alone do not qualify deployment TLS, catalogs, failover
// or service performance.
package doris
