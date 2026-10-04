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

// Package mysql provides the bounded public MySQL database capability.
// Sources have explicit owners; transactions and preparations retain one pool
// generation. Required evidence is caller-owned and never silently drained.
//
// Validate loadable Settings, obtain explicit policies with Recommend, then Open
// a source with a caller-owned Runtime and Inbox[Result]. Open is local ownership;
// Client.Ping explicitly checks readiness. Receive released evidence incrementally,
// finalize retained handles, and keep the Owner through incomplete Close attempts.
// Prepare uses a setup context; Begin's context owns the transaction lifetime and
// native automatic rollback. Using retains public generations, not raw SDK handles.
// These are process-local APIs, not deterministic Workflow code or durable DTOs.
package mysql
