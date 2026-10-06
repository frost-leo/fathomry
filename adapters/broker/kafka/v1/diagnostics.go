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

package kafka

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string                     { return "kafka[restricted]" }
func (private) GoString() string                   { return "kafka[restricted]" }
func (private) Format(state fmt.State, _ rune)     { _, _ = state.Write([]byte("kafka[restricted]")) }
func (private) LogValue() slog.Value               { return slog.StringValue("kafka[restricted]") }
func (private) MarshalJSON() ([]byte, error)       { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error        { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (Settings) LogValue() slog.Value              { return private{}.LogValue() }
func (*Owner) LogValue() slog.Value                { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value               { return private{}.LogValue() }
func (*Client) LogValue() slog.Value               { return private{}.LogValue() }
func (*Result) LogValue() slog.Value               { return private{}.LogValue() }
func (*TransactionIDs) LogValue() slog.Value       { return private{}.LogValue() }
func (*Header) LogValue() slog.Value               { return private{}.LogValue() }
func (*Message) LogValue() slog.Value              { return private{}.LogValue() }
func (*Position) LogValue() slog.Value             { return private{}.LogValue() }
func (*Range) LogValue() slog.Value                { return private{}.LogValue() }
func (*Record) LogValue() slog.Value               { return private{}.LogValue() }
func (*Page) LogValue() slog.Value                 { return private{}.LogValue() }
func (*Write) LogValue() slog.Value                { return private{}.LogValue() }
func (*Read) LogValue() slog.Value                 { return private{}.LogValue() }
func (*Topic) LogValue() slog.Value                { return private{}.LogValue() }
func (*Checkpoint) LogValue() slog.Value           { return private{}.LogValue() }
func (*CheckpointResult) LogValue() slog.Value     { return private{}.LogValue() }
func (*ConsumerProgress) LogValue() slog.Value     { return private{}.LogValue() }
func (*Assignment) LogValue() slog.Value           { return private{}.LogValue() }
func (*GroupSnapshot) LogValue() slog.Value        { return private{}.LogValue() }
func (*GroupBatch) LogValue() slog.Value           { return private{}.LogValue() }
func (*Consumer) LogValue() slog.Value             { return private{}.LogValue() }
func (*Group) LogValue() slog.Value                { return private{}.LogValue() }
