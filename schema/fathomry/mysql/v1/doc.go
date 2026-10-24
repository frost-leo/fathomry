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

// Package mysql exposes the read-only MySQL assets for the first Fathomry
// control-schema compatibility line. Assets includes an ordered manifest and
// complete single-statement SQL templates. It does not implement installation.
//
// A consumer must verify checksums, quote the explicit existing target database,
// and own a bounded physical session, migration lock and installation history.
// MySQL DDL implicitly commits: these assets do not make installation atomic or
// authorize retrying an incomplete migration. Importing the package performs no I/O.
//
// InspectionQueries and NormalizeInspection provide detached read-only metadata
// SQL and comparison normalization, not a connection, readiness check or installer.
// Their consumer owns session-profile validation, locking, bounds and interpretation.
package mysql
