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

package zerolog

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string                  { return "zerolog[restricted]" }
func (private) GoString() string                { return "zerolog[restricted]" }
func (private) Format(state fmt.State, _ rune)  { _, _ = state.Write([]byte("zerolog[restricted]")) }
func (private) LogValue() slog.Value            { return slog.StringValue("zerolog[restricted]") }
func (private) MarshalJSON() ([]byte, error)    { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error     { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(s fmt.State, v rune)     { private{}.Format(s, v) }
func (Settings) LogValue() slog.Value           { return private{}.LogValue() }
func (Sink) Format(s fmt.State, v rune)         { private{}.Format(s, v) }
func (Sink) LogValue() slog.Value               { return private{}.LogValue() }
func (File) Format(s fmt.State, v rune)         { private{}.Format(s, v) }
func (File) LogValue() slog.Value               { return private{}.LogValue() }
func (Dependencies) Format(s fmt.State, v rune) { private{}.Format(s, v) }
func (Dependencies) LogValue() slog.Value       { return private{}.LogValue() }
func (*Owner) LogValue() slog.Value             { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value            { return private{}.LogValue() }
func (*Client) LogValue() slog.Value            { return private{}.LogValue() }
func (*View) LogValue() slog.Value              { return private{}.LogValue() }
func (*Prepared) LogValue() slog.Value          { return private{}.LogValue() }
func (*SourceInfo) LogValue() slog.Value        { return private{}.LogValue() }
func (*Profile) LogValue() slog.Value           { return private{}.LogValue() }
func (*Result) LogValue() slog.Value            { return private{}.LogValue() }
func (*SinkResult) LogValue() slog.Value        { return private{}.LogValue() }
func (*Record) LogValue() slog.Value            { return private{}.LogValue() }
func (*Correlation) LogValue() slog.Value       { return private{}.LogValue() }
func (*RecordOutcome) LogValue() slog.Value     { return private{}.LogValue() }
