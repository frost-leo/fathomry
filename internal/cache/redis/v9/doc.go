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

// Package redis integrates go-redis v9 through named resource ownership,
// shared admission and independently reserved invocation evidence.
//
// Composition first calls DisableNativeLogging during single-threaded process
// startup: the upstream SDK logger is global, not a named-source setting.
// Select prepares OptionsV1, resource.WithLimits attaches the shared policy,
// and Bind returns a non-owning Client. Execute and Pipeline preserve native
// command replies; Watch, Dedicated and Subscribe own bounded sessions.
// No native clients, hooks, connection handles or mutable Cmder values escape.
// Results are immutable runtime values, not durable records or business outcomes.
//
// Redis acknowledgements do not establish rollback, durability, freshness or
// idempotency. A lost reply leaves effects unknown. Caller cancellation does
// not necessarily end native automatic batching or reverse an external write.
// See the package interface documentation for topology and experimental limits.
package redis
