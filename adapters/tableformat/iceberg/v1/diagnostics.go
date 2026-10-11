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

package iceberg

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (private) String() string               { return "iceberg[restricted]" }
func (private) GoString() string             { return "iceberg[restricted]" }
func (private) Format(s fmt.State, _ rune)   { _, _ = s.Write([]byte("iceberg[restricted]")) }
func (private) LogValue() slog.Value         { return slog.StringValue("iceberg[restricted]") }
func (private) MarshalJSON() ([]byte, error) { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error  { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(s fmt.State, v rune)  { private{}.Format(s, v) }
func (Settings) LogValue() slog.Value        { return private{}.LogValue() }

func (ScanOptions) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (ScanOptions) LogValue() slog.Value              { return private{}.LogValue() }
func (ScanOptions) MarshalJSON() ([]byte, error)      { return nil, fail(ErrSerialization, "marshal") }
func (*ScanOptions) UnmarshalJSON([]byte) error       { return fail(ErrSerialization, "unmarshal") }

func (SchemaEdit) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (SchemaEdit) LogValue() slog.Value              { return private{}.LogValue() }
func (SchemaEdit) MarshalJSON() ([]byte, error)      { return nil, fail(ErrSerialization, "marshal") }
func (*SchemaEdit) UnmarshalJSON([]byte) error       { return fail(ErrSerialization, "unmarshal") }

func (PartitionEdit) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (PartitionEdit) LogValue() slog.Value              { return private{}.LogValue() }
func (PartitionEdit) MarshalJSON() ([]byte, error)      { return nil, fail(ErrSerialization, "marshal") }
func (*PartitionEdit) UnmarshalJSON([]byte) error       { return fail(ErrSerialization, "unmarshal") }

func (RewriteChanges) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (RewriteChanges) LogValue() slog.Value              { return private{}.LogValue() }
func (RewriteChanges) MarshalJSON() ([]byte, error)      { return nil, fail(ErrSerialization, "marshal") }
func (*RewriteChanges) UnmarshalJSON([]byte) error       { return fail(ErrSerialization, "unmarshal") }

func (OrphanOptions) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }
func (OrphanOptions) LogValue() slog.Value              { return private{}.LogValue() }
func (OrphanOptions) MarshalJSON() ([]byte, error)      { return nil, fail(ErrSerialization, "marshal") }
func (*OrphanOptions) UnmarshalJSON([]byte) error       { return fail(ErrSerialization, "unmarshal") }
