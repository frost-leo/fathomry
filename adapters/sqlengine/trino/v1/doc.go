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

// Package trino exposes bounded direct-JSON Trino capabilities independently and
// through public resource Fixed/Follow bindings. Configure explicit settings,
// Recommend caller-owned admission/evidence, Open a source Owner, then use its
// non-owning Client for Query, Execute, one-physical-statement Insert or Read.
//
// Open performs coordinator readiness SQL, not connector/transaction certification.
// Read freezes inputs and owns one query with bounded page transfer and lifetime;
// its pages remain provisional until successful terminal evidence. It never
// rewrites/replays SQL, resumes a durable cursor or accumulates all result rows.
//
// Finite operations and retained readers preserve primary/cleanup causes, exact
// type metadata, incomplete prefixes and Unknown mutation effects. Explicit
// cleanup contexts own DELETE authority. Required evidence survives ignored
// returns, canceled waiters and failed sinks; callers receive and Ack it after
// necessary handling. Source and reader shutdown join actual work, retaining the
// same cleanup authority after a wait timeout. Runtime values are not durable DTOs.
package trino
