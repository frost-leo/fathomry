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

// Package duckdb provides finite native DuckDB SQL, many-row queries, prepared
// parameter batches and transactional Appender writes. It is process-local
// infrastructure, not a public engine interface or Temporal Workflow API.
// The v2 import path denotes the Go driver's SDK major, not OptionsV1, the v0
// bindings API, or the native core version. The selected driver is v2.10505.0,
// with bindings v0.10505.0 and core v1.5.5; no unreleased main is used.
//
// Select freezes version-1 settings; resource.WithLimits and resource.Assemble
// own the native database. Bind supplies a non-owning Database with a required
// invocation.Inbox. Run executes one request; Transaction executes a bounded
// sequence on one connection. Every accepted call completes its receipt only
// after native statements, appenders, results and connection have been closed.
// Resource borrowing/delegation uses resource, never raw SDK handles.
//
// Inputs are borrowed until the synchronous call returns and must not be mutated
// concurrently. Result.Snapshot returns independent scalar data and progress.
// SQL order is preserved only where the caller specifies ORDER BY. Limits apply
// to admitted work and retained results, not all native allocations: DuckDB can
// materialize results and its memory_limit is not a hard RSS ceiling.
//
// The selected stable driver requires CGO. Preparation, connection creation and
// destruction are not interruptible. Execution cancellation is cooperative and
// the stable driver can add approximately 500 ms after interruption. No goroutine
// detaches live native work. TIME results require an explicit VARCHAR cast to
// avoid the stable decoder's 24:00 ambiguity; timestamp infinity is unsupported.
// EXPLAIN is rejected because ANALYZE can execute hidden transaction commands.
// Composite/JSON result decoding, Arrow, extensions,
// external file I/O and external catalogs are unsupported by this profile.
package duckdb
