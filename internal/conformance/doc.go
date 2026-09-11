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

// Package conformance supplies internal standard-testing assertions and bounded
// evidence reception for Fathomry's mechanism and Provider integration tests. It
// is not an external Provider-extension SDK or a production dependency. It creates
// no SDK, test service, worker or resource owner.
//
// # Independent contract checks
//
// [Expected] describes facts established by a separate workload, native-effect or
// lifetime oracle. [Result] and [Receive] compare typed result and delivery facts;
// [Accounting] checks declared local allowances. Deriving the expected result from
// the implementation's own output cannot establish conformance.
//
// [Cause] checks intentional native error inspection. [Private] tests diagnostic
// projections using caller-supplied canaries; [Runtime] additionally checks the
// selected runtime serialization/reconstruction boundary. [Facade] checks supported
// method-only surfaces, not arbitrary indexing, callbacks or ownership hidden in
// another capability shape.
//
// Pair each important assertion with a deliberately broken control that fails for
// its intended diagnostic, not a compile error, panic or unrelated timeout.
// Passing these helpers alone does not establish real-service semantics,
// durability, native-memory limits or correct framework business attribution.
// Tests still own their native resources, producer termination and cleanup.
package conformance
