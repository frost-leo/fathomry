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

// Package nacos integrates the official Nacos SDK's generated gRPC clients and
// native configuration messages with explicit resource and session ownership.
// It does not start the SDK's global configuration client, cache or RPC engine.
//
// # Bootstrap, acquisition and preparation
//
// [Open] validates and freezes [OptionsV1] before creating local transport
// ownership; its context governs the entire client lifetime. [ServerV1] selects
// HTTP authentication and gRPC configuration endpoints independently. Successful
// construction does not prove reachability, authentication or session readiness.
//
// [Client.Read] and [Client.ReadAll] acquire original UTF-8 documents for preselected
// [KeyV1] values. Each finite read/batch owns a fresh Nacos session, and no required
// failure exposes a usable prefix or substitutes an earlier cached value.
// [Document.RawCopy] feeds existing resource preparation after composition assigns
// layer identity and precedence. Native MD5/time fields are observations, not
// authenticity, preparation revisions or a common-time snapshot.
//
// # Observation and shutdown
//
// [Client.Watch] owns session registration/recovery and a bounded invalidation queue.
// [Subscription.Next] reports [Change] metadata, not new application settings;
// resynchronization requires re-reading every selected key. Pushes and periodic
// hash/presence comparisons can coalesce or duplicate notifications. Reconnection
// requires new Nacos setup/listen registration, not just another gRPC TCP connection.
//
// [Subscription.Close] and [Client.Close] cancel owned work and wait for local
// completion. A timed-out close retains responsibility and must be retried.
// Native dial/TLS work has a separate bounded allowance; DNS or caller callbacks
// cannot be forcibly terminated by a Go context. No timeout proves remote rollback,
// instantaneous unsubscribe or successful release.
//
// # Compatibility and evidence limits
//
// This is a selected native protocol-component profile, not a transparent facade
// over the high-level SDK. The upstream-upgrade TODO is tracked at the SDK pin in
// go.mod and GH-21; safeguards are retired only after the relevant native fixes
// and regression/service gates pass. The package exposes no management writes,
// service discovery, public application loader or automatic configuration reload.
//
// Runtime formatting and JSON reconstruction are restricted. [RemoteError.Message],
// document copies and explicit identity getters deliberately expose potentially
// sensitive native data. Local TLS tests and isolated single-server service tests
// are distinct from production TLS, multi-node or deployed-artifact certification.
package nacos
