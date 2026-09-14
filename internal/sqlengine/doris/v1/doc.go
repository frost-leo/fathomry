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

// Package doris provides bounded native Stream Load and single-command SQL using
// go-sql-driver/mysql major 1 and standard HTTP. The path does not version Doris.
// Select, resource.WithLimits, resource.Assemble and Bind attach explicit source
// ownership, admission and an independently owned invocation.Inbox.
//
// StreamLoad sends one caller-labeled JSON batch without mutation retries.
// InspectLabel observes retained label state, not perpetual idempotency.
// Accepted, committed, visible and row-quality evidence remain distinct; UNKNOWN,
// malformed replies and cancellation never establish that a load had no effect.
//
// Query fully retains bounded copied rows. Exec accepts authorized SQL without
// inferring native-table or external-catalog durability from an OK packet.
// Each SQL call owns one fresh framed connection; no preparation, retained
// sessions, multi-statements, transactions, pool, or raw SDK handles are supplied.
// Packet, metadata, row and response bounds precede unsafe native allocation.
// SQL permissions and exact server/catalog qualification remain caller duties.
// Catalog access stays behind Doris SQL; this package does not use a backing
// table-format SDK, object-store client, Java process, or catalog administration.
//
// Calls run synchronously with caller-owned finite contexts. Resource admission
// is the only queue; no uploader, callback or background mutation worker exists.
// Receipts and required inbox deliveries preserve partial/unknown evidence and
// local cleanup independently of optional diagnostics. Results are immutable;
// explicit copy/inspection methods can disclose data and are not log-safe.
// No Workflow I/O, universal engine API, automatic staging or maintenance exists.
package doris
