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

// Package nuki implements a process-local HTTP Provider using the distinct
// github.com/nukilabs/tlsclient SDK. It is not the bogdanfinn integration or a
// public business HTTP API. Network work belongs outside Temporal Workflows.
//
// Select prepares one explicitly named instance and requires an external native
// profile. resource.WithLimits and resource.Assemble establish authoritative
// ownership; Bind joins it to a separately bounded invocation.Inbox. Borrowed
// aliases and runtime proxy choices do not create independent allowances.
//
// Do returns a finite receipt and Consume a callback-scoped streaming receipt.
// Request metadata is copied; readers and replay factories remain borrowed until
// release. Native response EOF, encoded/decoded integrity, final technical
// reporting, caller waiting and local cleanup are distinct facts. No business
// retry, Cookie refresh, Provider rotation, eviction or durable DTO is supplied.
//
// Native callbacks are trusted cooperative Go code, not a sandbox. They must
// obey their declared lifetimes and cannot expose owning handles or start hidden
// work. Source shutdown retains unresolved native cleanup and causal failures.
//
// Local QUIC/QPACK corrections preserve stream-scoped cancellation and bounded
// decoding. Loopback tests are not production-site or arbitrary-profile
// qualification. Go 1.27.0, integration major 1,
// configuration/native/request contracts 1, SDK v1.8.8 and local correction v1
// are separate version axes.
package nuki
