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

// Package temporal exposes the selected native Temporal SDK through explicit
// source ownership, retained use, bounded process admission and independent
// operation, Worker and user-callback evidence.
//
// Prepare freezes Settings and NativeOptions without dialing or invoking borrowed
// callbacks. Policy supplies one-source capacity; Open reserves that capacity
// before native construction. Owner.Client is a retained direct use. Borrow
// creates another use; Using(...).Retain selects one Fixed/Follow generation.
// Each retained Client must Close. Owner.Close seals all local uses and Workers,
// but physical cleanup waits for actual descendants and native join.
//
// Workflow, Activity and Nexus operations preserve native option types. Native
// deterministic workflow helpers remain in go.temporal.io/sdk/workflow and must
// not invoke this process-side Adapter from Workflow decisions. User callbacks
// use the SDK-provided scoped Client; their authority ends on callback return,
// including Activity ErrResultPending and asynchronous Nexus returns.
//
// Ordinary errors are safe public occurrences. NativeError deliberately exposes
// the captured scoped native return for exact FailureConverter semantics; an
// ordinary wrapper is not wire-transparent. Native payloads, metadata and injected
// observers remain explicit caller-owned sensitive boundaries.
//
// Consumers must select the documented local SDK compatibility replacement;
// another module does not inherit this repository's replace directives. SDK,
// Adapter, configuration and Server versions are independent. Deprecated,
// experimental and optional profiles do not promise unavailable Server features.
package temporal
