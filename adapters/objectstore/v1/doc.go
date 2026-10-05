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

// Package objectstore defines SDK-independent object-storage budgets and evidence.
// Concrete adapters use these actual shared contracts while retaining their own
// addressing, protocol conditions, results and retained-session semantics.
//
// This package imports public operation mechanisms only. It owns no SDK, native
// bridge, client, source registry, worker or shutdown authority. Provider
// recommendations account for complete source/session/evidence envelopes; public
// resource composition owns Fixed/Follow adoption and borrowing.
//
// Info and Attribution are sensitive process-local observations, not durable DTOs.
// Shared Effect values never imply rollback, object absence or persistence.
// No universal storage client or assumed parity among future SDKs is provided.
package objectstore
