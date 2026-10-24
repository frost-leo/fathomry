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

// Package schema supplies the schema command namespace and its shared restricted
// SQL execution. Each user-facing leaf is a separate package wired by the CLI
// entry. These helpers borrow resource-backed public Adapter clients and reviewed
// embedded assets; they are not a Framework capability or a database resource owner.
//
// Constructors are offline. Status and Plan acquire bounded transaction/session
// ownership and exclusion but make no persistent writes. Apply admits only empty
// or verified current state. Acknowledged effects, subsequent observation and
// uncertain effects remain distinct from cleanup and release. Incomplete MySQL
// installation is never resumed or adopted.
//
// Entry composition owns source/runtime/evidence reception and reserves WorkBytes plus the
// selected source's row-copy envelopes. Methods refuse overlap. Each operation
// is finite, but a timed-out cleanup wait retains the same runner for Close.
// The runner does not close its borrowed source. These are process-local APIs,
// not deterministic Temporal Workflow code or durable DTOs.
package schema
