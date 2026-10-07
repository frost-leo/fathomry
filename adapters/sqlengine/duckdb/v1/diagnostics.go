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

package duckdb

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string { return "duckdb[restricted]" }

func (private) GoString() string { return "duckdb[restricted]" }

func (private) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("duckdb[restricted]")) }

func (private) LogValue() slog.Value { return slog.StringValue("duckdb[restricted]") }

func (private) MarshalJSON() ([]byte, error) { return nil, fail(ErrSerialization, "marshal") }

func (*private) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal") }

func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }

// LogValue redacts Settings values and non-nil pointers. Normalize optional nil
// pointers to untyped nil before logging rather than calling their value wrapper.
func (Settings) LogValue() slog.Value { return private{}.LogValue() }

func (*Owner) LogValue() slog.Value { return private{}.LogValue() }

func (*Handle) LogValue() slog.Value { return private{}.LogValue() }

func (*Client) LogValue() slog.Value { return private{}.LogValue() }

func (*Result) LogValue() slog.Value { return private{}.LogValue() }

func (*Request) LogValue() slog.Value { return private{}.LogValue() }

func (*Column) LogValue() slog.Value { return private{}.LogValue() }

func (*Step) LogValue() slog.Value { return private{}.LogValue() }

func (*Progress) LogValue() slog.Value { return private{}.LogValue() }

func (*Reader) LogValue() slog.Value { return private{}.LogValue() }

func (*ReadProgress) LogValue() slog.Value { return private{}.LogValue() }

func (*BuildInfo) LogValue() slog.Value { return private{}.LogValue() }

func (*ModuleInfo) LogValue() slog.Value { return private{}.LogValue() }

func (Decimal) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }

func (Decimal) LogValue() slog.Value { return private{}.LogValue() }

func (Decimal) MarshalJSON() ([]byte, error) { return nil, fail(ErrSerialization, "marshal") }

func (*Decimal) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal") }

func (UUID) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }

func (UUID) LogValue() slog.Value { return private{}.LogValue() }

func (UUID) MarshalJSON() ([]byte, error) { return nil, fail(ErrSerialization, "marshal") }

func (*UUID) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal") }

func (Interval) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }

func (Interval) LogValue() slog.Value { return private{}.LogValue() }

func (Interval) MarshalJSON() ([]byte, error) { return nil, fail(ErrSerialization, "marshal") }

func (*Interval) UnmarshalJSON([]byte) error { return fail(ErrSerialization, "unmarshal") }
