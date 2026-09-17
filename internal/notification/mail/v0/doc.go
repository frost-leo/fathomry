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

// Package mail supplies bounded outbound SMTP notifications with go-mail v0.
// Select freezes explicit named configuration, resource.WithLimits attaches shared
// admission, and Bind exposes a concurrent non-owning Client. Send and SendBatch
// reserve independent invocation evidence before MIME allocation or network I/O.
// Only resource.Assembly owns shutdown; borrowed clients share the same capacity.
//
// NewMessage freezes headers, plain/HTML bodies, CID resources and attachments.
// Presentation, content sanitization and chart rendering belong to callers. Relay
// acceptance is not inbox placement, client rendering, reading or a durable Run
// outcome. Unknown effects never trigger automatic resending. Native handles,
// file paths/readers, callbacks and debug logging are not exposed.
package mail
