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

// Package httpcloak supplies independently configured HTTPcloak Providers for
// controlled finite requests and decoded response streams. It is an internal Go
// integration, not a public HTTP facade or Temporal Workflow implementation.
//
// Select freezes one explicit named preset, strict JSON preset or native preset.
// Compose LimitsV1 / resource.WithLimits / resource.Assemble, then Bind with a
// required invocation Inbox. Aliases borrow the same identity and all allowances.
// Neither the Client nor its results expose an SDK client, transport or shutdown
// handle. Open streams must be closed; Do uses a separate cleanup-wait context.
//
// RequestOptionsV1 freezes dynamic TCP proxy routing, CONNECT headers and native
// header controls before admission. Request readers and GetBody callbacks remain
// borrowed through receipt release; Close must unblock Read. Native callbacks,
// cookie jars, root certificates and key-log writers must obey their concurrent-use
// contracts, must not reenter the same operation and may not start unmanaged work.
// Redirect callbacks receive metadata-only copies and may veto, not mutate, a hop.
// The explicit call context supplies native context values; request.Context also
// cancels admission and work. Cancellation cannot forcibly terminate user code.
//
// Version 1 supports explicit H1, H2 and direct H3. It does not silently race or
// downgrade protocols, support UDP/MASQUE proxies, expose native Session setters,
// or supply business retries, Cookie/token acquisition, refresh, rotation or Redis.
// Native exchanges may retry; attempt observations are not an exact wire count.
// Bounded exclusive checkouts reuse native connections but do not multiplex
// separate admitted operations on one H2/H3 client. Native capacity exhaustion
// rejects rather than adding an unbounded second queue.
//
// Complete requires response framing/decoding and input-consumption evidence,
// without a primary error. Request return, stopped waiting, technical completion,
// local call release and source release are different events. Neither Complete,
// 200, EOF nor Close establishes business success, peer receipt or non-effect.
// Errors retain primary, cleanup and native callback causes independently. Runtime
// settings/results refuse serialization; module, SDK, local compatibility, API
// and configuration versions are separate axes.
package httpcloak
