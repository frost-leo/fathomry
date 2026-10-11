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

// Package iceberg integrates the maintained Apache Iceberg Go SDK major 0 with
// native-semantic REST/S3 scopes and the compatible legacy bounded batch API.
// The v0 path denotes SDK major, not table format or configuration version.
//
// PrepareManaged freezes native-semantic settings and borrowed dependencies.
// ManagedPreparation.Select and BindManaged reuse resource/invocation admission.
// ManagedClient.Use admits one serial callback family containing controlled
// Catalog, Table, Transaction, Scan and file scopes. It supports native V1/V2/V3
// metadata, type/evolution, streaming, writes, maintenance and optional explicit
// credential vending. All managed handles expire and actual work joins before
// release. The public Adapter owns consumer error/locale and generation mapping.
// The following paragraphs describe the retained legacy Select/Bind path only.
//
// Select freezes explicit Catalog, namespace, S3-subtree and static-credential
// settings. Assemble owns REST/S3 clients and their transport;
// Bind joins authoritative resource admission and an independently drained
// invocation inbox. No raw Table, transaction, FileIO, iterator or owning Arrow
// reference escapes. This package is ordinary process-local I/O, not Workflow code.
//
// Writes distinguish uploaded files, successful staging, a validated Catalog
// acknowledgement and a subsequent reload. Missing replies remain unknown;
// no business retry or automatic file compensation runs. Reload verifies the
// metadata object through the selected store, not a server-advertised endpoint.
// Server-vended credentials are not used. No other internal integration is imported.
//
// Batches use flat scalar Arrow input, bounded coalescing into a single native
// write record and copied IPC output. Row mutations and compaction are currently
// unpartitioned, full-snapshot rewrites and delete-file-free. Snapshot expiration, orphan deletion,
// row-delta writes, arbitrary imports, branch management, views and multi-table
// commits are not supplied. See the package contract for the capability matrix.
//
// Bounds cover admission, calls, file sizes, I/O, metadata and returned data;
// they are not a hard process-RSS or hostile compressed-input sandbox. SDK global
// configuration must not be mutated; a worker setting other than the inspected
// default is refused. Native error graphs retain their inspection semantics.
// Current service qualification and review results are recorded separately.
package iceberg
