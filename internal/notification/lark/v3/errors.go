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

package lark

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies the independently selected official Feishu integration.
const ProviderID = "notification.lark.v3"

const (
	ErrInput           fault.Kind = "fathomry." + ProviderID + ".input"
	ErrCall            fault.Kind = "fathomry." + ProviderID + ".call"
	ErrLimit           fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrUnsupported     fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrAuth            fault.Kind = "fathomry." + ProviderID + ".auth"
	ErrHTTP            fault.Kind = "fathomry." + ProviderID + ".http"
	ErrAPI             fault.Kind = "fathomry." + ProviderID + ".api"
	ErrResponse        fault.Kind = "fathomry." + ProviderID + ".response"
	ErrReceive         fault.Kind = "fathomry." + ProviderID + ".receive"
	ErrCleanup         fault.Kind = "fathomry." + ProviderID + ".cleanup"
	ErrWebSocket       fault.Kind = "fathomry." + ProviderID + ".websocket"
	ErrProtocol        fault.Kind = "fathomry." + ProviderID + ".protocol"
	ErrAcknowledgement fault.Kind = "fathomry." + ProviderID + ".acknowledgement"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

type private struct{}

func (private) String() string                 { return "lark[restricted]" }
func (private) GoString() string               { return "lark[restricted]" }
func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "lark[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("lark[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("lark: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("lark: runtime reconstruction unsupported")
}
func (*OptionsV1) LogValue() slog.Value        { return slog.StringValue("lark[restricted]") }
func (*Source) LogValue() slog.Value           { return slog.StringValue("lark[restricted]") }
func (*Client) LogValue() slog.Value           { return slog.StringValue("lark[restricted]") }
func (*Result) LogValue() slog.Value           { return slog.StringValue("lark[restricted]") }
func (*Exchange) LogValue() slog.Value         { return slog.StringValue("lark[restricted]") }
func (*JSON) LogValue() slog.Value             { return slog.StringValue("lark[restricted]") }
func (*Content) LogValue() slog.Value          { return slog.StringValue("lark[restricted]") }
func (*Recipient) LogValue() slog.Value        { return slog.StringValue("lark[restricted]") }
func (*Page) LogValue() slog.Value             { return slog.StringValue("lark[restricted]") }
func (*Upload) LogValue() slog.Value           { return slog.StringValue("lark[restricted]") }
func (*Revision) LogValue() slog.Value         { return slog.StringValue("lark[restricted]") }
func (*MessageInfo) LogValue() slog.Value      { return slog.StringValue("lark[restricted]") }
func (*Event) LogValue() slog.Value            { return slog.StringValue("lark[restricted]") }
func (*Callback) LogValue() slog.Value         { return slog.StringValue("lark[restricted]") }
func (*WebSocketOptions) LogValue() slog.Value { return slog.StringValue("lark[restricted]") }
func (*WebSocketStats) LogValue() slog.Value   { return slog.StringValue("lark[restricted]") }
func (*WebSocketAck) LogValue() slog.Value     { return slog.StringValue("lark[restricted]") }
func (*WebSocketResult) LogValue() slog.Value  { return slog.StringValue("lark[restricted]") }
func (*WebSocketEvent) LogValue() slog.Value   { return slog.StringValue("lark[restricted]") }
func (*Receiver) LogValue() slog.Value         { return slog.StringValue("lark[restricted]") }
