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

package lark

import (
	"fmt"
	"log/slog"
)

type private struct{}

func (*UnforwardedEvent) LogValue() slog.Value { return private{}.LogValue() }

func (*CredentialRequest) LogValue() slog.Value { return private{}.LogValue() }

func (*Callback) LogValue() slog.Value       { return private{}.LogValue() }
func (*HTTPResponse) LogValue() slog.Value   { return private{}.LogValue() }
func (*Incoming) LogValue() slog.Value       { return private{}.LogValue() }
func (*WebSocketAck) LogValue() slog.Value   { return private{}.LogValue() }
func (*WebSocketStats) LogValue() slog.Value { return private{}.LogValue() }
func (*Profile) LogValue() slog.Value        { return private{}.LogValue() }

func (private) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("lark[restricted]")) }
func (private) LogValue() slog.Value           { return slog.StringValue("lark[restricted]") }
func (private) MarshalJSON() ([]byte, error)   { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error    { return fail(ErrSerialization, "unmarshal") }
func (Settings) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("lark.Settings[restricted]"))
}
func (Settings) LogValue() slog.Value { return private{}.LogValue() }
func (WebSocketSettings) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("lark.WebSocketSettings[restricted]"))
}
func (WebSocketSettings) LogValue() slog.Value    { return private{}.LogValue() }
func (*Result) LogValue() slog.Value              { return private{}.LogValue() }
func (*Owner) LogValue() slog.Value               { return private{}.LogValue() }
func (*Handle) LogValue() slog.Value              { return private{}.LogValue() }
func (*Client) LogValue() slog.Value              { return private{}.LogValue() }
func (*Prepared) LogValue() slog.Value            { return private{}.LogValue() }
func (*NativeOptions) LogValue() slog.Value       { return private{}.LogValue() }
func (*Credentials) LogValue() slog.Value         { return private{}.LogValue() }
func (*WebhookCredentials) LogValue() slog.Value  { return private{}.LogValue() }
func (*Request) LogValue() slog.Value             { return private{}.LogValue() }
func (*RequestOptions) LogValue() slog.Value      { return private{}.LogValue() }
func (*AssetInput) LogValue() slog.Value          { return private{}.LogValue() }
func (*JSON) LogValue() slog.Value                { return private{}.LogValue() }
func (*Content) LogValue() slog.Value             { return private{}.LogValue() }
func (*Recipient) LogValue() slog.Value           { return private{}.LogValue() }
func (*Page) LogValue() slog.Value                { return private{}.LogValue() }
func (*Revision) LogValue() slog.Value            { return private{}.LogValue() }
func (*Exchange) LogValue() slog.Value            { return private{}.LogValue() }
func (*AuthenticationStage) LogValue() slog.Value { return private{}.LogValue() }
func (*MessageInfo) LogValue() slog.Value         { return private{}.LogValue() }
func (*Event) LogValue() slog.Value               { return private{}.LogValue() }
