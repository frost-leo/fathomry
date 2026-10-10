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

package temporal

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string                 { return "temporal[restricted]" }
func (private) GoString() string               { return "temporal[restricted]" }
func (private) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("temporal[restricted]")) }
func (private) LogValue() slog.Value           { return slog.StringValue("temporal[restricted]") }
func (private) MarshalJSON() ([]byte, error)   { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error    { return fail(ErrSerialization, "unmarshal") }

func (Attribution) String() string                    { return private{}.String() }
func (Attribution) GoString() string                  { return private{}.GoString() }
func (Attribution) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (Attribution) LogValue() slog.Value              { return private{}.LogValue() }
func (Attribution) MarshalJSON() ([]byte, error)      { return private{}.MarshalJSON() }
func (*Attribution) UnmarshalJSON(data []byte) error  { return (&private{}).UnmarshalJSON(data) }

func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }

// LogValue redacts credentials and endpoints. Normalize optional typed-nil
// Settings pointers to untyped nil before passing them to slog.
func (Settings) LogValue() slog.Value { return private{}.LogValue() }
