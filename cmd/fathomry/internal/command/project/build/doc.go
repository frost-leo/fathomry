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

// Package build owns native project compilation, offline declaration inspection,
// and publication of the inspected executable. Both the official CLI and a
// project-root CLI use the same command handler and Run implementation. This
// package is private; cmd/fathomry/v1 owns the fixed public command entry.
//
// Callers own contexts and streams. Definition data is borrowed for the call and
// must not be mutated concurrently. Build executes trusted project code; init and
// declaration assembly must be free of service and business side effects. The
// gate qualifies supplied declarations, not handler semantics or Temporal access.
// Native go build remains outside this gate.
package build
