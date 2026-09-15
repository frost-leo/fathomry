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

// Package tlsclient supplies independently configured tls-client Providers under
// Fathomry's resource, invocation, fault and compatibility mechanisms. The v1
// import path versions this integration, not the SDK or local SDK patch.
//
// Select requires an externally chosen native profile and performs no I/O.
// Apply resource.WithLimits, Assemble and Bind with an independent invocation.Inbox.
// LimitsV1 describes unoverridden Go options; layered settings need matching
// composition limits. No environment proxy, global client or business profile is
// discovered. Borrowed aliases retain the original named source and shared limits.
//
// Do retains bounded response bytes; Open returns a Stream that must be closed
// even after EOF. The explicit context replaces Request.Context. Request metadata
// is copied before admission. Accepted Body/GetBody inputs are borrowed through
// receipt release; readers must support Close interrupting Read and supply fresh,
// independently owned replay readers. Outbound trailers are fixed snapshots, not
// mutable Body-produced EOF metadata. Native incoming trailers are copied at EOF.
// ProxyFromProvider, ProxyDirect and ProxyAddress choose an immutable route for
// the whole logical call. RoutingLocked can prohibit runtime changes. CONNECT
// headers, including native ContextKeyHeader input, partition authenticated pools.
// Each managed H2 CONNECT tunnel owns its physical proxy connection, so native
// socket deadlines cannot interrupt another tunnel. Origin pooling remains enabled.
//
// HTTP1Only, Negotiated H1/H2 and HTTP3Racing retain native protocol behavior.
// Profiles, headers, TLS transport options, pins, jars and metadata-only hooks
// have explicit copying/borrowing contracts in NativeOptionsV1. Unsupported
// combinations are refused, not silently ignored. In particular, H3 cannot use
// TCP-only dial/proxy factories, IP-family filters, pinning, key logging or disabled
// keep-alives. H3's native TLS fingerprint is not the profile's TCP ClientHello.
// Explicit factory failures are preserved instead of selecting another preset.
// Native special modes that ignore an explicit factory, and randomized ALPN
// modes that ignore HTTP1Only, are refused without restricting ordinary profiles.
//
// Active/queued calls, cached and retiring route/authority bindings, TCP handles,
// H3 transports, bytes, replays and exchanges have separate ceilings. Native H2
// parser limits come from the selected profile and must fit MaxNativeHeaderBytes;
// retained metadata is independently bounded by MaxHeaderBytes. H1/H3 receive
// limits are positive, potentially changing native H3 SETTINGS. Byte reservations
// are not exact RSS, native QUIC stream counts or kernel-memory guarantees.
//
// A receipt and its independent Inbox delivery survive early caller wait return.
// Receiving headers, ignoring an error or canceling a context does not certify
// complete data, no remote effect or full native shutdown. Streams and accepted
// reader/callback work retain their call reservation until joined. Pool workers
// and canceled native racing drains remain source-owned; late drain failures are
// also retained by source cleanup. Assembly release cancels and joins managed
// native work and confirms resource release, retaining unresolved cleanup.
//
// Native callbacks are cooperative, trusted Go dependencies, not a sandbox.
// Do not retain callback arguments, start unaccounted work, mutate native objects
// concurrently or reenter shutdown from callbacks. Opaque dependencies remain
// borrowed through assembly release, even after a particular call is released.
// Returned errors must remain immutable and must not retain owning handles.
//
// Results expose explicit copies and private errors preserve errors.Is/As.
// Complete means final response EOF with verified length/byte limits and no
// primary failure, not business success.
// HTTP/3 encoded framing is checked before transparent gzip decoding; decoded
// byte limits remain a separate Provider check. Hook panics retain native hook
// containment and private notices, not a general arbitrary-callback sandbox.
// Exchanges count observed SDK submissions, not exact physical attempts; native
// racing and reconnect retries may repeat effects. Evidence is local, not a
// durable workflow ledger.
//
// No raw SDK client, setters, arbitrary RoundTripper, httptrace handle, WebSocket,
// direct owning connection, application retry, credential refresh or Provider
// rotation API is supplied. Build/Profile distinguish actual build facts from
// declared choices and missing service evidence. The local upstream-license
// publication gate remains separate; see third_party/tls-client/FATHOMRY.md.
package tlsclient
