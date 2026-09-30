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

// Package viper exposes the supported native Viper configuration profile to
// independent applications and Framework consumers. New binds caller-owned public
// operation admission and evidence once. Load uses explicit files or borrowed
// readers; no discovery, merging or process mutation is implicit.
//
// Documents preserve original bytes separately from live native queries.
// Capture freezes native values; Decode uses native weak conversions. Strict
// original-document layering belongs to adapters/configsource/v1, not Decode.
//
// Watch retains one native subscription until actual cleanup. Canceling Next
// ends only that wait. A timed-out Close retains the same reachable owner.
// The common runtime and evidence Inbox remain caller-owned. Evidence reception
// never repeats source operations. This is not deterministic Workflow code.
package viper
