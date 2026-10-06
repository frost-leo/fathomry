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

// Package redis exposes bounded Redis commands, mixed-capability pipelines,
// pinned sessions/transactions, Streams, pull subscriptions and opt-in native
// caching/batching through the shared public operation and resource mechanisms.
//
// Composition explicitly disables native global logging at single-threaded
// process startup, prepares Settings, composes capacity, supplies an operation
// Runtime and required Inbox, and retains Owner until actual shutdown. Cache and
// Messaging views share a source; they neither create quota nor own its close.
// Results preserve decoded value presence separately from replies and effects.
// This package does not implement durable delivery, business retries or quotas.
package redis
