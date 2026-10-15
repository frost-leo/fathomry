/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

// Package lark provides managed notification-domain Lark operations, authenticated
// HTTP callbacks and source/connection-bound WebSocket acknowledgements. Prepare
// freezes offline source selection; Open owns it; Client/Handle only borrow. Public
// Runtime/Inbox and resource Fixed/Follow own admission, evidence and generations.
// Calls never retry notification mutations. Local acknowledgement/write evidence
// does not establish human receipt, rendering or business completion.
package lark
