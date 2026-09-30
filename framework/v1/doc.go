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

// Package framework composes public operation and resource ownership for supported
// Framework scenarios. New creates no service clients or global settings. Bind
// selected resources explicitly through Resources and operation evidence through
// Operations; Close stops work before joining resource cleanup.
//
// StartReceiver delivers released evidence without blocking behind live owners.
// Sink failures retain custody and retry only delivery. ErrorLog binds presentation
// once at an explicit slog boundary; technical calls never select a locale.
//
// This is neither an application host nor a service locator, SDK registry, second
// resource engine or durable Workflow runtime. Caller-owned source declarations,
// bounded callbacks and shutdown responsibilities remain explicit.
package framework
