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

// Package settings stores project-owned configuration data, independently of its
// sources, codecs, validation rules and consumers. It does not load files or
// environment variables, own SDK clients, run Watch loops or manage resources.
//
// Producers validate their data, call New with their explicit copy function, then
// Publish to a Store. Readers Capture a coherent View; As preserves the exact root
// type and Read copies only a selected typed subtree. Configure can explicitly
// install a populated Reader as the one process default. Separate domains keep
// their own readers. No read performs I/O or silently selects a default.
//
// Copy functions define data ownership: isolate mutable fields, do not mutate
// inputs, and document intentional immutable/borrowed sharing. Snapshots do not
// infer deep-copy rules for arbitrary Go types. Runtime handles hide payloads in
// formatting/logging and reject JSON persistence; explicit copies may be sensitive.
package settings
