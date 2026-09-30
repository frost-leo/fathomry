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

package framework

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string                      { return "framework[restricted]" }
func (private) GoString() string                    { return "framework[restricted]" }
func (private) Format(state fmt.State, _ rune)      { restricted(state) }
func (private) MarshalJSON() ([]byte, error)        { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error         { return fail(ErrSerialization, "unmarshal") }
func restricted(state fmt.State)                    { _, _ = state.Write([]byte("framework[restricted]")) }
func (*Runtime) Format(state fmt.State, _ rune)     { restricted(state) }
func (*Runtime) LogValue() slog.Value               { return slog.StringValue("framework[restricted]") }
func (*ErrorLog) Format(state fmt.State, _ rune)    { restricted(state) }
func (*ErrorLog) LogValue() slog.Value              { return slog.StringValue("framework[restricted]") }
func (*Emission) Format(state fmt.State, _ rune)    { restricted(state) }
func (*Emission) LogValue() slog.Value              { return slog.StringValue("framework[restricted]") }
func (*Receiver[T]) Format(state fmt.State, _ rune) { restricted(state) }
func (*Receiver[T]) LogValue() slog.Value           { return slog.StringValue("framework[restricted]") }
