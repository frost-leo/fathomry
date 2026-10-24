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

// Package postgresql exposes the read-only PostgreSQL assets for the first
// Fathomry control-schema compatibility line. Assets includes the ordered
// manifest and complete single-statement SQL templates, not an installer.
// Each table asset atomically creates its table and multilingual comments in
// one static DO block; it must never be split at semicolons by a consumer.
//
// A consumer must verify manifest checksums, quote the explicit existing target
// schema, and apply the declared statement order under its own bounded connection,
// migration lock and history ownership. Importing or reading this package performs
// no database, configuration or other service work.
//
// InspectionQueries and NormalizeInspection share read-only metadata SQL and
// canonical comparison rules with consumers. They neither execute those queries
// nor establish transaction, lock, privilege, profile or schema-readiness evidence.
package postgresql
