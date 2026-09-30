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

package configsource

import (
	"fmt"
	"log/slog"
)

func (Layer) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("configsource.Layer[restricted]"))
}
func (Layer) LogValue() slog.Value { return slog.StringValue("configsource.Layer[restricted]") }
func (Variable) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("configsource.Variable[restricted]"))
}
func (Variable) LogValue() slog.Value            { return slog.StringValue("configsource.Variable[restricted]") }
func (Schema[T]) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("configsource.Schema")) }
func (Schema[T]) LogValue() slog.Value           { return slog.StringValue("configsource.Schema") }
func (Schema[T]) MarshalJSON() ([]byte, error)   { return nil, fail(ErrSerialization, "marshal_schema") }
func (*Schema[T]) UnmarshalJSON([]byte) error    { return fail(ErrSerialization, "unmarshal_schema") }
