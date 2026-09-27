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

package configuration

import "log/slog"

// Explicit pointer methods avoid dereferencing a typed nil through a promoted
// method. Values retain fmt redaction and JSON refusal through private.
func (*Schema[T]) LogValue() slog.Value     { return private{}.LogValue() }
func (*Plan) LogValue() slog.Value          { return private{}.LogValue() }
func (*Snapshot[T]) LogValue() slog.Value   { return private{}.LogValue() }
func (*State[T]) LogValue() slog.Value      { return private{}.LogValue() }
func (*Live[T]) LogValue() slog.Value       { return private{}.LogValue() }
func (*Presentation) LogValue() slog.Value  { return private{}.LogValue() }
func (*Cursor) LogValue() slog.Value        { return private{}.LogValue() }
func (*Input) LogValue() slog.Value         { return private{}.LogValue() }
func (*Variable) LogValue() slog.Value      { return private{}.LogValue() }
func (*LayerDocument) LogValue() slog.Value { return private{}.LogValue() }
