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

package nacos

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

// NativeEvidence is explicit supported RPC/HTTP evidence projected without an
// Internal import. Message is sensitive. It does not describe an unrelated outer
// failure or determine mutation state/retry policy. Original cause objects remain
// intact in the supplied error, including third-party transport/status errors.
type NativeEvidence struct {
	private
	resultCode, errorCode, httpStatus int
	message                           string
}

func (value NativeEvidence) ResultCode() int { return value.resultCode }
func (value NativeEvidence) ErrorCode() int  { return value.errorCode }
func (value NativeEvidence) HTTPStatus() int { return value.httpStatus }
func (value NativeEvidence) Message() string { return value.message }

// InspectError projects the first supported native RPC/HTTP cause. False means no
// such evidence was found, not proof of a request's absence or lack of effects.
func InspectError(err error) (NativeEvidence, bool) {
	var remote *native.RemoteError
	var status native.HTTPStatus
	rpc := errors.As(err, &remote) && remote != nil
	http := errors.As(err, &status)
	value := NativeEvidence{}
	if rpc {
		value.resultCode = remote.ResultCode()
		value.errorCode = remote.ErrorCode()
		value.message = remote.Message()
	}
	if http {
		value.httpStatus = int(status)
	}
	return value, rpc || http
}

func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(failure.Occurrence); ok {
		return err
	}
	code := ErrRead
	if operation == "close" {
		code = ErrClose
	}
	if direct, ok := err.(interface{ Is(error) bool }); ok {
		for _, entry := range []struct {
			native error
			public failure.Code
		}{
			{native.ErrInput, ErrInput}, {native.ErrLimit, ErrLimit}, {native.ErrRead, ErrRead}, {native.ErrWrite, ErrWrite},
			{native.ErrDecode, ErrDecode}, {native.ErrDenied, ErrDenied}, {native.ErrMissing, ErrMissing}, {native.ErrEmpty, ErrEmpty},
			{native.ErrClosed, ErrClosed}, {native.ErrState, ErrState}, {native.ErrUnavailable, ErrUnavailable}, {native.ErrUnsupported, ErrUnsupported},
		} {
			if direct.Is(entry.native) {
				code = entry.public
				break
			}
		}
	}
	return fail(code, operation, err)
}

type private struct{}

func (private) String() string                 { return "nacos[restricted]" }
func (private) GoString() string               { return "nacos[restricted]" }
func (private) Format(state fmt.State, _ rune) { restricted(state) }
func (private) MarshalJSON() ([]byte, error)   { return nil, fail(ErrSerialization, "marshal") }
func (*private) UnmarshalJSON([]byte) error    { return fail(ErrSerialization, "unmarshal") }
func restricted(state fmt.State)               { _, _ = state.Write([]byte("nacos[restricted]")) }

func (*Owner) Format(state fmt.State, _ rune)          { restricted(state) }
func (*Owner) LogValue() slog.Value                    { return slog.StringValue("nacos[restricted]") }
func (*Client) Format(state fmt.State, _ rune)         { restricted(state) }
func (*Client) LogValue() slog.Value                   { return slog.StringValue("nacos[restricted]") }
func (*Handle) Format(state fmt.State, _ rune)         { restricted(state) }
func (*Handle) LogValue() slog.Value                   { return slog.StringValue("nacos[restricted]") }
func (*Document) Format(state fmt.State, _ rune)       { restricted(state) }
func (*Document) LogValue() slog.Value                 { return slog.StringValue("nacos[restricted]") }
func (*PublishInput) Format(state fmt.State, _ rune)   { restricted(state) }
func (*PublishInput) LogValue() slog.Value             { return slog.StringValue("nacos[restricted]") }
func (*MutationResult) Format(state fmt.State, _ rune) { restricted(state) }
func (*MutationResult) LogValue() slog.Value           { return slog.StringValue("nacos[restricted]") }
func (*SearchInput) Format(state fmt.State, _ rune)    { restricted(state) }
func (*SearchInput) LogValue() slog.Value              { return slog.StringValue("nacos[restricted]") }
func (*SearchItem) Format(state fmt.State, _ rune)     { restricted(state) }
func (*SearchItem) LogValue() slog.Value               { return slog.StringValue("nacos[restricted]") }
func (*SearchPage) Format(state fmt.State, _ rune)     { restricted(state) }
func (*SearchPage) LogValue() slog.Value               { return slog.StringValue("nacos[restricted]") }
func (*Subscription) Format(state fmt.State, _ rune)   { restricted(state) }
func (*Subscription) LogValue() slog.Value             { return slog.StringValue("nacos[restricted]") }
func (*Observation) Format(state fmt.State, _ rune)    { restricted(state) }
func (*Observation) LogValue() slog.Value              { return slog.StringValue("nacos[restricted]") }
func (*Batch) Format(state fmt.State, _ rune)          { restricted(state) }
func (*Batch) LogValue() slog.Value                    { return slog.StringValue("nacos[restricted]") }
func (*Change) Format(state fmt.State, _ rune)         { restricted(state) }
func (*Change) LogValue() slog.Value                   { return slog.StringValue("nacos[restricted]") }
func (*NativeEvidence) Format(state fmt.State, _ rune) { restricted(state) }
func (*NativeEvidence) LogValue() slog.Value           { return slog.StringValue("nacos[restricted]") }
func (Settings) Format(state fmt.State, _ rune)        { restricted(state) }
func (Settings) LogValue() slog.Value                  { return slog.StringValue("nacos[restricted]") }
func (Server) Format(state fmt.State, _ rune)          { restricted(state) }
func (Server) LogValue() slog.Value                    { return slog.StringValue("nacos[restricted]") }
func (Key) Format(state fmt.State, _ rune)             { restricted(state) }
func (Key) LogValue() slog.Value                       { return slog.StringValue("nacos[restricted]") }
func (ObserveOptions) Format(state fmt.State, _ rune)  { restricted(state) }
func (ObserveOptions) LogValue() slog.Value            { return slog.StringValue("nacos[restricted]") }
