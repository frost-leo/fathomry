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

package minio

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string                     { return "minio[restricted]" }
func (private) GoString() string                   { return "minio[restricted]" }
func (private) Format(state fmt.State, _ rune)     { _, _ = state.Write([]byte("minio[restricted]")) }
func (private) LogValue() slog.Value               { return slog.StringValue("minio[restricted]") }
func (private) MarshalJSON() ([]byte, error)       { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error        { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (Settings) String() string                    { return private{}.String() }
func (Settings) GoString() string                  { return private{}.String() }
func (Settings) LogValue() slog.Value              { return private{}.LogValue() }

func (*Owner) LogValue() slog.Value        { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value       { return private{}.LogValue() }
func (*Client) LogValue() slog.Value       { return private{}.LogValue() }
func (*Result) LogValue() slog.Value       { return private{}.LogValue() }
func (*Address) LogValue() slog.Value      { return private{}.LogValue() }
func (*Object) LogValue() slog.Value       { return private{}.LogValue() }
func (*Transfer) LogValue() slog.Value     { return private{}.LogValue() }
func (*Removal) LogValue() slog.Value      { return private{}.LogValue() }
func (*Upload) LogValue() slog.Value       { return private{}.LogValue() }
func (*Part) LogValue() slog.Value         { return private{}.LogValue() }
func (*Delegation) LogValue() slog.Value   { return private{}.LogValue() }
func (*ReadRequest) LogValue() slog.Value  { return private{}.LogValue() }
func (*WriteRequest) LogValue() slog.Value { return private{}.LogValue() }
func (*CopyRequest) LogValue() slog.Value  { return private{}.LogValue() }
func (*ListRequest) LogValue() slog.Value  { return private{}.LogValue() }
func (*UploadQuery) LogValue() slog.Value  { return private{}.LogValue() }
func (*SignRequest) LogValue() slog.Value  { return private{}.LogValue() }
func (*Multipart) LogValue() slog.Value    { return private{}.LogValue() }
func (*Cursor) LogValue() slog.Value       { return private{}.LogValue() }
