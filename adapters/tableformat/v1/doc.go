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

// Package tableformat owns public admission and observation contracts shared by
// table-format adapters. Concrete providers retain their native catalogs, table
// metadata, schemas, scans, transactions, file operations and cleanup authority;
// this package defines neither a universal table engine nor a transaction API.
//
// Providers derive Policy from exact prepared settings. Source work stays charged
// until actual release, while source evidence stays charged until its independent
// record is acknowledged. Root families and retained source generations have
// distinct lifetimes. Declared bytes are not a hard native allocation or RSS cap.
//
// Effect and Phases separate staged intent, uploaded files, Catalog submission,
// acknowledgement, later read-back and local release. None proves downstream
// visibility, durability, retry safety or receipt acknowledgement. Facts from
// different actions must not be collapsed into one operation-wide success bit.
//
// Info and Attribution are process-local observations with restricted ordinary
// presentation, not durable DTOs. Clone detaches mutable provenance storage;
// callers must not mutate a value concurrently with cloning it. Optional typed-nil
// metadata pointers should be normalized to nil before logging. This package
// imports no SDK, Internal package, Framework or concrete provider.
package tableformat
