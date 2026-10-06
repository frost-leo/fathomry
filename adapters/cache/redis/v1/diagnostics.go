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

package redis

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string                     { return "redis[restricted]" }
func (private) GoString() string                   { return "redis[restricted]" }
func (private) Format(state fmt.State, _ rune)     { _, _ = state.Write([]byte("redis[restricted]")) }
func (private) LogValue() slog.Value               { return slog.StringValue("redis[restricted]") }
func (private) MarshalJSON() ([]byte, error)       { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error        { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (Settings) LogValue() slog.Value              { return private{}.LogValue() }
func (*Owner) LogValue() slog.Value                { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value               { return private{}.LogValue() }
func (*Client) LogValue() slog.Value               { return private{}.LogValue() }
func (*View) LogValue() slog.Value                 { return private{}.LogValue() }
func (*Command) LogValue() slog.Value              { return private{}.LogValue() }
func (*Result) LogValue() slog.Value               { return private{}.LogValue() }
func (*Reply) LogValue() slog.Value                { return private{}.LogValue() }
func (*Value) LogValue() slog.Value                { return private{}.LogValue() }
func (*Pair) LogValue() slog.Value                 { return private{}.LogValue() }
func (*Password) LogValue() slog.Value             { return private{}.LogValue() }
func (*Prepared) LogValue() slog.Value             { return private{}.LogValue() }
func (*Session) LogValue() slog.Value              { return private{}.LogValue() }
func (*Subscription) LogValue() slog.Value         { return private{}.LogValue() }
func (*SubscriptionOptions) LogValue() slog.Value  { return private{}.LogValue() }
func (*TransactionResult) LogValue() slog.Value    { return private{}.LogValue() }
func (*Profile) LogValue() slog.Value              { return private{}.LogValue() }
