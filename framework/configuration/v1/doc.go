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

// Package configuration composes public raw Sources and strict preparation into
// typed accepted-data domains. Load returns a published State after complete
// acceptance. Watch owns one source ingress and one serial validation path;
// obsolete/invalid observations preserve last-good data and Close fences publishing.
//
// The project owns T, defaults, strong validation, selected sources/layer policy
// and explicit environment bindings. Bootstrap is resolved before acquisition.
// Application and business-variable declarations use independent stores/readers;
// no mandatory application envelope, all-SDK settings union or implicit default
// installation is introduced. Optional public resource adoption is separate from
// acceptance and readiness. The package imports no root Internal or concrete SDK.
//
// Callbacks must be bounded, concurrency-safe and non-panicking. Cancellation
// requests stop but never substitutes for actual cleanup/validation completion.
// This process-local code must not execute inside deterministic Workflows.
package configuration
