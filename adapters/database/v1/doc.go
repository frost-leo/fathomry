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

// Package database defines provider-independent database attribution and budget
// contracts. Concrete PostgreSQL and MySQL adapters retain their distinct SQL,
// result and transaction semantics. This package owns no native engine or pool.
//
// Provider Recommend functions produce Policy values for explicit admission and
// evidence composition. Info, Attribution, Attempts and Profile are process-local
// observations, not durable DTOs. Metadata copies belong to their callers; Info
// also offers Clone to isolate its provenance slices. No source construction,
// database probing, background worker or shutdown authority is provided here.
package database
