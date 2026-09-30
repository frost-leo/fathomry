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

// Package configsource owns strict, bounded configuration preparation independent
// of native acquisition and Internal mechanisms. Prepare checks every original
// layer, overlays defaults/base/environment/local/variables, then validates a
// detached candidate. Prepared values provide isolated copies and public settings
// snapshots. Native Viper Decode is a different, deliberately weak profile.
// Source/Observer define a selected complete raw acquisition profile; immutable
// batches preserve missing/empty/failure distinctions without importing providers.
//
// Documents are explicit UTF-8 JSON or restricted YAML. Fields use exact json
// names; objects/maps overlay, lists replace, absence inherits, and null clears
// only pointers/maps/lists. Preparation performs no I/O, discovery, publication,
// polling, instance reconstruction or process-environment mutation.
//
// Schema callbacks must be bounded, concurrency-safe and non-panicking. Context
// cancellation is cooperative and cannot forcibly stop user validation. Error
// causes are explicit sensitive inspection data; ordinary diagnostics are safe.
package configsource
