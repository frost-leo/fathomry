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

// Package iceberg exposes controlled native-semantic Apache Iceberg REST/S3 use.
//
// Prepare freezes Settings and borrowed NativeOptions offline. Recommend or
// Prepared.Policy supplies explicit public runtime/evidence envelopes. Open owns
// physical cleanup; Owner.Client and Handle only lend use. Using captures a
// Fixed/Follow generation for each Use callback, not for each nested table call.
//
// A Use callback is one bounded admitted family. Its Session, Table, Transaction,
// Scan and OrphanPlan wrappers are serial and expire when the callback returns.
// Readers, Arrow batches and encryption/file callbacks are cooperative: callers
// must not wait on their own source shutdown. Arrow visitor records are borrowed;
// an explicit Retain transfers an additional caller-owned reference. Whole Arrow
// table owners share a bounded scope reservation and must be closed even when
// returned with an error; family termination also closes ignored owners.
//
// Native schemas, expressions, metadata builders, format codecs and compaction
// plans remain native SDK data. No raw native Catalog, Table or FileIO owner is
// exposed. Staging, uploads, commit acknowledgement, unknown outcome and later
// reload are separate facts. Unknown results are not resent or compensated.
// Physical deletion requires explicit grants and orphan cleanup additionally
// requires an external-writer quiescence declaration.
//
// API v1, settings format1, SDK v0.7.0, table formats V1/V2/V3 and Catalog/server
// versions are independent. This package requires the maintained SDK source
// documented in third_party/iceberg-go; root replace directives are not inherited
// by independent consumers. Full live and distribution qualification is tracked
// separately from implemented code.
package iceberg
