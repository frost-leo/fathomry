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

// Package failure defines version 1 of a public, in-process error contract.
// It depends only on the standard library, not private mechanisms or SDKs.
//
// Declare code-owned Conditions, construct an Error with New and explicitly
// select its public causes. Inspect selects only the supplied occurrence;
// errors.Is and errors.As separately traverse deliberately exposed causes.
// Capability-owned extensions implement Occurrence and own their typed facts,
// copying, bounds, presentation and concurrency contracts.
//
// Error owns immutable condition and cause-slice storage. Foreign cause objects
// are retained, not cloned. Package-owned diagnostics and supported fmt/slog output never
// traverse them. Code grammar is not proof of namespace authority or privacy:
// conditions must never be built from secrets or per-request identifiers.
//
// Runtime errors are not wire DTOs. JSON encoding/reconstruction is refused;
// arbitrary foreign codecs, wrappers and raw inspection are outside this safety
// boundary. In particular, Temporal's default failure converter is not a v1
// bridge. This package supplies no retry, effect, localization or durable policy.
//
// The entire Go contract is versioned, including ownership, zero semantics,
// matching and extension method sets. Conditions, capability details, root-module
// builds and future durable schemas have independent compatibility responsibilities.
package failure
