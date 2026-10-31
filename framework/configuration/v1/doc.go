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

// Package configuration loads an application's bootstrap and main configuration.
// It is not a general business-configuration loader or an application host.
// Schema, Variable and Encoding are application-facing contracts owned here;
// technical preparation types are used only behind the loading boundary.
//
// LoadBootstrap resolves the selected literal dotenv file and explicit overrides
// into a source plan and phase budgets. LoadConfiguration uses that bootstrap and
// the project's typed schema to return a Session after first acceptance. Watch
// continues in that session when selected. The same loading path serves an
// independent config check and later application startup; neither starts business
// services, installs process defaults or mutates configuration sources.
//
// Session owns source work, not its borrowed Runtime/Inboxes. Close must run even
// when LoadConfiguration returns both a Session and an error. InitialView retains
// the first accepted snapshot; Reader may observe later updates. Invalid updates
// preserve last-good data. Status/Next expose source and acceptance facts separately
// from resource readiness. A timeout retains the owner for continued cleanup.
//
// PrepareInput and BindConfiguration integrate the two loaders with an existing
// Resource scope. This optional composition owns the configuration dependencies it
// creates, including failed construction and required evidence delivery. It is
// not a second lifecycle engine or the formal application-start entry. Binding
// returns a Resource Ref and a separate read-only settings Reader of Loaded values;
// accepted data remains obtainable without a lease, including after cancellation.
//
// Business-specific configuration, including credential observations, can use
// public configuration-source Adapters, strict preparation and settings directly
// without importing this application's bootstrap policy.
package configuration
