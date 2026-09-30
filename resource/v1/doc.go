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

// Package resource owns explicitly configured runtime component instances and
// their fixed or configuration-following lifetimes. It is a public mechanism,
// independent of Internal foundations, concrete SDKs and Framework orchestration.
//
// # Declare, apply and use
//
// New creates an owner before fallible assembly. Bind registers named typed
// selectors and factories. Apply accepts one coherent settings.View; Watch can
// feed that same path from an explicit channel of accepted views. Declarations
// seal on admitted input. Selectors, copying and equality are pure bounded owner
// callbacks; no process-default settings or source parser is consulted.
//
// Fixed bindings retain their first selection. Follow bindings coalesce relevant
// changes, preserve allowed last-good instances after construction failure, and
// fence obsolete/late candidates. Update.Wait reports historical adoption, not
// current remote health or atomic replacement across multiple bindings.
//
// Acquire borrows one active generation. Keep a lease for the complete operation
// or transaction, then Release it. New operations can use a replacement while old
// borrows retain their original instance. A raw value must not outlive its lease;
// the component defines its own concurrent-use and long-session rebind behavior.
//
// # Construction and cleanup ownership
//
// Build receives a generation lifetime context. It survives successful return
// and caller waiting cancellation; components use child contexts for phase budgets.
// A non-nil Instance transfers ownership even on error. Failed or superseded
// candidates are cleaned, never silently discarded.
//
// Parent cancellation or Close stops new admission and begins shutdown. Retained
// borrows delay cleanup; native cleanup has a fresh bounded context. ReleaseResult
// distinguishes completion from errors. Incomplete cleanup retains the same owner;
// Retry or another Close continues it. Foreign work ignoring cancellation cannot
// be forcibly terminated. Count bounds do not certify process memory or service quotas.
//
// # Evidence and presentation
//
// Status exposes per-binding adoption and cleanup facts separately from native
// readiness. Definitions and Resources compose explicitly with failure and i18n;
// runtime handles never select a locale or invoke native diagnostic methods.
// Source revisions, local target tokens and construction generations are distinct.
// No control-database migration, automatic replay, global service locator,
// dependency-graph migration or durable runtime-object serialization is implied.
package resource
