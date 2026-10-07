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

// Package tlsclient exposes the selected tls-client SDK through public admission,
// evidence and source-generation ownership. Prepare freezes an explicit profile
// and data/native inputs offline. Owner alone closes the source; Client/Handle
// cannot expose an SDK client, transport or mutable tracker. fhttp requests retain
// native metadata/order semantics. Method contexts replace Request.Context.
// Do retains finite data; Open returns a response stream requiring Close after EOF.
// Cleanup waiting, native work completion and evidence acknowledgement are separate.
package tlsclient
