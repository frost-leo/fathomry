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

package otel

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string                         { return "otel[restricted]" }
func (private) GoString() string                       { return "otel[restricted]" }
func (private) Format(state fmt.State, _ rune)         { _, _ = state.Write([]byte("otel[restricted]")) }
func (private) LogValue() slog.Value                   { return slog.StringValue("otel[restricted]") }
func (private) MarshalJSON() ([]byte, error)           { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error            { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(state fmt.State, verb rune)     { private{}.Format(state, verb) }
func (TLS) Format(state fmt.State, verb rune)          { private{}.Format(state, verb) }
func (Dependencies) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }

func (Info) String() string                            { return private{}.String() }
func (Info) GoString() string                          { return private{}.GoString() }
func (Info) Format(state fmt.State, verb rune)         { private{}.Format(state, verb) }
func (Info) MarshalJSON() ([]byte, error)              { return private{}.MarshalJSON() }
func (*Info) UnmarshalJSON(data []byte) error          { return (&private{}).UnmarshalJSON(data) }
func (Attribution) String() string                     { return private{}.String() }
func (Attribution) GoString() string                   { return private{}.GoString() }
func (Attribution) Format(state fmt.State, verb rune)  { private{}.Format(state, verb) }
func (Attribution) MarshalJSON() ([]byte, error)       { return private{}.MarshalJSON() }
func (*Attribution) UnmarshalJSON(data []byte) error   { return (&private{}).UnmarshalJSON(data) }
func (SignalResult) String() string                    { return private{}.String() }
func (SignalResult) GoString() string                  { return private{}.GoString() }
func (SignalResult) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (SignalResult) MarshalJSON() ([]byte, error)      { return private{}.MarshalJSON() }
func (*SignalResult) UnmarshalJSON(data []byte) error  { return (&private{}).UnmarshalJSON(data) }
func (Fact) String() string                            { return private{}.String() }
func (Fact) GoString() string                          { return private{}.GoString() }
func (Fact) Format(state fmt.State, verb rune)         { private{}.Format(state, verb) }
func (Fact) MarshalJSON() ([]byte, error)              { return private{}.MarshalJSON() }
func (*Fact) UnmarshalJSON(data []byte) error          { return (&private{}).UnmarshalJSON(data) }

// LogValue redacts configured endpoints, headers and credentials. Normalize
// optional nil Settings pointers to untyped nil before logging.
func (Settings) LogValue() slog.Value      { return private{}.LogValue() }
func (TLS) LogValue() slog.Value           { return private{}.LogValue() }
func (Dependencies) LogValue() slog.Value  { return private{}.LogValue() }
func (*Owner) LogValue() slog.Value        { return private{}.LogValue() }
func (*Client) LogValue() slog.Value       { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value       { return private{}.LogValue() }
func (*Prepared) LogValue() slog.Value     { return private{}.LogValue() }
func (*Span) LogValue() slog.Value         { return private{}.LogValue() }
func (*Result) LogValue() slog.Value       { return private{}.LogValue() }
func (*Attribution) LogValue() slog.Value  { return private{}.LogValue() }
func (*Info) LogValue() slog.Value         { return private{}.LogValue() }
func (*Fact) LogValue() slog.Value         { return private{}.LogValue() }
func (*Profile) LogValue() slog.Value      { return private{}.LogValue() }
func (*SignalResult) LogValue() slog.Value { return private{}.LogValue() }
func (*LogRecord) LogValue() slog.Value    { return private{}.LogValue() }
func (*Link) LogValue() slog.Value         { return private{}.LogValue() }
func (*SpanInput) LogValue() slog.Value    { return private{}.LogValue() }
func (*ExportLoop) LogValue() slog.Value   { return private{}.LogValue() }
func (*ExportStatus) LogValue() slog.Value { return private{}.LogValue() }
