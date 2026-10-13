/*
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

package chromedp

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) Format(s fmt.State, _ rune)        { _, _ = s.Write([]byte("chromedp[restricted]")) }
func (private) LogValue() slog.Value              { return slog.StringValue("chromedp[restricted]") }
func (private) MarshalJSON() ([]byte, error)      { return nil, fail(ErrSerialization, "serialization") }
func (*private) UnmarshalJSON([]byte) error       { return fail(ErrSerialization, "reconstruction") }
func (Settings) Format(s fmt.State, v rune)       { private{}.Format(s, v) }
func (Settings) LogValue() slog.Value             { return private{}.LogValue() }
func (ContextOptions) Format(s fmt.State, v rune) { private{}.Format(s, v) }
func (ContextOptions) LogValue() slog.Value       { return private{}.LogValue() }
