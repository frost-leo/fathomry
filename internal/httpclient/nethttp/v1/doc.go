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

// Package nethttp supplies bounded standard-library HTTP requests, response
// streams and direct connections under Fathomry's internal mechanisms.
// The v1 import path versions this integration, not the Go standard library.
// The consuming Go toolchain identifies the native implementation; OptionsV1 and
// NativeOptionsV1 separately version configuration and runtime extension contracts.
// RequestOptionsV1 separately versions per-call access input.
//
// # Composition and request ownership
//
// Select validates one externally configured named instance without I/O.
// Composition applies resource.WithLimits, assembles it and calls Bind with an
// independent invocation.Inbox. LimitsV1 is a convenience for unoverridden Go
// options, not a layered configuration loader. Sources remain independently
// selectable; no other HTTP SDK, environment proxy or default client is imported
// or discovered. Borrowed aliases retain the original identity and allowance.
//
// Do retains a bounded response body; Open returns a controlled Stream. HTTP
// statuses remain data, and callers supply native requests, headers and bodies.
// ProxyFromProvider, ProxyAddress and ProxyDirect are distinct runtime choices.
// Configured routing is a default unless RoutingLocked forbids other selections.
// An explicit address is frozen before admission across redirects/native retries;
// failed selection never falls back to direct access or another configured route.
// The explicit context replaces Request.Context. Metadata is copied; Body and
// GetBody storage are borrowed until receipt release. Readers must support Close
// interrupting Read and immutable, independent replay readers. Close is required
// even after EOF. Request-body failure, response completeness and cleanup remain
// separate evidence; receiving headers or handling an error cannot release work.
//
// A non-nil receipt identifies an accepted operation and is also retained in the
// Inbox. Native entry and cleanup each have an owned worker; early context/wait
// return does not discard their resource reservation or late results. Required
// evidence capacity is reserved before native work. Receivers must inspect and
// release every delivery. This is process-local evidence, not a durable ledger.
//
// # Native capabilities and lifetime
//
// H1, TLS H2 and explicit prior-knowledge cleartext H2 use the standard transport.
// Native TLS, HTTP2 configuration, dial/proxy/redirect hooks and jars remain
// available with explicit copying and borrowed-dependency contracts. Routing uses
// separately retained native transports for each proxy/authentication binding,
// including direct access, to prevent cross-route H2 pool reuse. The source bounds
// cached/retiring bindings and all sockets; only unused bindings may be retired.
// This is technical resource cleanup, not Provider eviction policy. Redirect
// callbacks receive metadata without owning bodies. Raw RoundTripper injection,
// native httptrace handles, 101 upgrades, raw ClientConn authority and server-only
// TLS ECH keys are not supported. No rendered-browser or H3 behavior is supplied.
// Outbound trailers are fixed metadata snapshots; Body-populated EOF trailers are
// not supported. Native response trailers remain available after complete reads.
//
// Connect creates a separate, source-bounded direct connection. Its transport has
// no native pool limit because direct connections bypass that accounting. A
// Connection owns one session reservation, admits one child stream at a time,
// requires exact URL authority and closes its native connection and any child.
// Connect can select a runtime proxy; children retain that connection's route.
// Native callbacks are cooperative, not a Go-code sandbox; time/cache/diagnostic
// hooks additionally must return promptly. Shutdown fences captured dependencies,
// closes owned sockets and preserves incomplete cleanup and original causes.
//
// # Evidence and limits
//
// Working/evidence bytes, body bytes, header metadata, exchanges, connections and
// callback entry limits are different controls. An over-limit read may consume one
// witness byte but never hands that byte off as within-limit output. Exchanges and
// observed attempts do not count every native retry or prove peer receipt.
// Profile and Build report available facts, not tested support for arbitrary
// origins, native hooks, service versions or configurations. No hard RSS limit,
// arbitrary native-goroutine census, distributed quota or remote rollback is implied.
//
// Runtime values and native causes refuse ordinary JSON persistence and use safe
// diagnostics; deliberate headers/data/native-cause inspection can expose secrets.
// The package owns no business request preparation, Cookie/token acquisition or
// injection policy, refresh, retry strategy, Provider rotation/eviction, Redis
// coordination, public Adapter, Workflow commands or business terminal decisions.
package nethttp
