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

package slog

import (
	"fmt"
	stdslog "log/slog"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
)

type private struct{}

func (private) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("logging.slog[restricted]"))
}
func (private) LogValue() stdslog.Value        { return stdslog.StringValue("logging.slog[restricted]") }
func (private) MarshalJSON() ([]byte, error)   { return nil, fail(logging.ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error    { return fail(logging.ErrSerialization, "unmarshal") }
func (*Handler) LogValue() stdslog.Value       { return private{}.LogValue() }
func (*Status) LogValue() stdslog.Value        { return private{}.LogValue() }
func (*Record) LogValue() stdslog.Value        { return private{}.LogValue() }
func (Binding) Format(state fmt.State, _ rune) { private{}.Format(state, 'v') }
func (Binding) LogValue() stdslog.Value        { return private{}.LogValue() }
func (Binding) MarshalJSON() ([]byte, error)   { return nil, fail(logging.ErrSerialization, "marshal") }
func (*Binding) UnmarshalJSON([]byte) error    { return fail(logging.ErrSerialization, "unmarshal") }
