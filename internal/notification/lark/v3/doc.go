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

// Package lark supplies independently owned Lark/Feishu notification capabilities
// using the official Go SDK v3, not its whole-platform Client. Select freezes
// configuration; resource.WithLimits and Bind compose shared admission and a
// mandatory invocation inbox. Only the assembly owns transport/cache shutdown.
//
// Messages, native Card JSON 2.0 (including VChart and tables), CardKit updates,
// media and signed webhooks have distinct operations and evidence. No operation
// retries a remote mutation. API acknowledgement is not recipient rendering,
// reading, durable event processing or a Fathomry Run/Item terminal decision.
// Runtime values are not durable DTOs. No SDK, HTTP, cache or callback handle
// escapes the facade. This package is not Temporal Workflow code.
//
// With explicit WebSocket options, BindReceiver composes a non-owning Receiver.
// Listen runs under caller-owned cancellation, with bounded reassembly and
// separate event/ACK evidence. Applications explicitly acknowledge after handling;
// timeout or disconnection never fabricates successful processing.
package lark
