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

// Package failure supplies extensible public error identities and immutable,
// in-process occurrences, independently of Providers, localization and execution
// policy. Define a namespaced Code, construct an Error with New, and match it with
// errors.Is. Inspect describes only the explicitly supplied occurrence; errors.As
// searches causes and must not be used to choose an operation's primary failure.
//
// Optional diagnostics are bounded and copied. Stable machine details belong to
// capability-owned typed errors, with their own validation, bounds and copying
// contracts; they are not diagnostic attributes. A typed error can embed an
// unexported alias of Error to inherit safe formatting and runtime JSON refusal.
//
// New borrows only a deliberately exposed cause, never formats or traverses it,
// and treats a direct typed nil as absent. Native evidence withheld at the public
// boundary remains separately owned by that boundary. Error values and their owned
// data are safe for concurrent reading; borrowed causes follow their own contracts.
//
// Construction and owned inspection are deterministic and in-memory: no clock,
// random identity, environment, translation lookup, logging or background work.
// External error methods are arbitrary Go callbacks, not a sandboxed extension.
// Errors do not establish external effects, safe retry, or Item/Run disposition.
// No durable error schema or Temporal semantic conversion is supplied.
package failure
