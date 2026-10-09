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

// Package slog supplies a deliberately restricted, provider-neutral slog ingress.
// It preserves attribute/group chronology and original record time/PC without
// evaluating arbitrary LogValuer, Stringer, error or marshaler presentation.
// This is not an unrestricted standard-Handler compatibility claim.
//
// The provider supplies bounded callbacks and a retained-view Release hook.
// Execution is synchronous through the provider's existing Runtime. Supplied
// context values remain associated; its cancellation does not suppress attempts.
// Owner lifetime and explicit timeout control work. Close affects the complete
// handler family and releases its borrowed view, never the resource owner.
package slog
