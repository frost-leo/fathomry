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

// Package adapters provides independent public operation mechanisms for concrete
// Adapter authors: bounded admission, actual-work ownership, typed outcomes,
// required evidence custody and optional diagnostics. It is not a universal SDK
// client, instance holder, retry engine or durable ledger.
//
// Construct a Runtime and Inbox, then Bind a typed Endpoint with its Copy
// contract. Run/Start admit before dispatch; a Call retains its submitting stack,
// explicit Guards and bounded Child operations. Resolve publishes an outcome,
// not work completion. Receipt.WaitReleased joins actual work; a canceled wait
// neither cancels the operation nor acknowledges its evidence.
//
// Using/StartUsing borrow an explicitly selected public resource.Ref through the
// entire operation family. Instance construction/replacement remains resource's
// responsibility. Runtime.Close seals admission, requests stop and joins actual
// work; timed-out owners remain reachable. The independent Inbox retains facts
// until Delivery.Ack. DeliverOne retries custody on sink failure, never SDK work.
// NextReleased can receive completed records without waiting behind live owners;
// Next preserves admission-order claims.
//
// Component-owned copying and producer/sink callbacks must be bounded,
// non-panicking and lifetime-correct. No callback runs under runtime locks.
// Reservations bound declared bytes, not arbitrary Go/native memory. Providers
// retain responsibility for readiness, native cleanup, partial effects and safe
// non-owning consumer facades.
//
// Errors use public failure definitions and preserve original causes without
// formatting them. Resources compose explicitly with i18n at a presentation/log
// boundary. Operations never select a global locale, settings store or instance.
// This runtime is process-local and does not belong in deterministic Workflow
// execution or imply crash durability/exactly-once effects.
package adapters
