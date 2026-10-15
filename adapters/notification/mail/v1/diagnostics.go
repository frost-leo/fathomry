/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package mail

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (*PartInput) LogValue() slog.Value { return private{}.LogValue() }

func (private) String() string                     { return "mail[restricted]" }
func (private) GoString() string                   { return "mail[restricted]" }
func (private) Format(state fmt.State, _ rune)     { _, _ = state.Write([]byte("mail[restricted]")) }
func (private) LogValue() slog.Value               { return slog.StringValue("mail[restricted]") }
func (private) MarshalJSON() ([]byte, error)       { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error        { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(state fmt.State, verb rune) { private{}.Format(state, verb) }

// Normalize an optional nil Settings pointer to untyped nil before slog.
func (Settings) LogValue() slog.Value        { return private{}.LogValue() }
func (*Owner) LogValue() slog.Value          { return private{}.LogValue() }
func (*Client) LogValue() slog.Value         { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value         { return private{}.LogValue() }
func (*Prepared) LogValue() slog.Value       { return private{}.LogValue() }
func (*NativeOptions) LogValue() slog.Value  { return private{}.LogValue() }
func (*Result) LogValue() slog.Value         { return private{}.LogValue() }
func (*Content) LogValue() slog.Value        { return private{}.LogValue() }
func (*Part) LogValue() slog.Value           { return private{}.LogValue() }
func (*Header) LogValue() slog.Value         { return private{}.LogValue() }
func (*Inline) LogValue() slog.Value         { return private{}.LogValue() }
func (*Attachment) LogValue() slog.Value     { return private{}.LogValue() }
func (*Message) LogValue() slog.Value        { return private{}.LogValue() }
func (*Input) LogValue() slog.Value          { return private{}.LogValue() }
func (*FileInput) LogValue() slog.Value      { return private{}.LogValue() }
func (*TemplateInput) LogValue() slog.Value  { return private{}.LogValue() }
func (*Transform) LogValue() slog.Value      { return private{}.LogValue() }
func (*Preparation) LogValue() slog.Value    { return private{}.LogValue() }
func (*DKIMOptions) LogValue() slog.Value    { return private{}.LogValue() }
func (*DKIM) LogValue() slog.Value           { return private{}.LogValue() }
func (*SMIME) LogValue() slog.Value          { return private{}.LogValue() }
func (*ImportOptions) LogValue() slog.Value  { return private{}.LogValue() }
func (*LocalDelivery) LogValue() slog.Value  { return private{}.LogValue() }
func (*SessionRequest) LogValue() slog.Value { return private{}.LogValue() }
func (*SessionInfo) LogValue() slog.Value    { return private{}.LogValue() }
func (*Extension) LogValue() slog.Value      { return private{}.LogValue() }
func (*HeaderChange) LogValue() slog.Value   { return private{}.LogValue() }
func (*ProcessInfo) LogValue() slog.Value    { return private{}.LogValue() }
func (*Delivery) LogValue() slog.Value       { return private{}.LogValue() }
func (*Recipient) LogValue() slog.Value      { return private{}.LogValue() }
func (*ResponseReader) LogValue() slog.Value { return private{}.LogValue() }
