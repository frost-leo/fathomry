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
	"github.com/frost-leo/fathomry/internal/fault"
	"io"
	"log/slog"
)

// ProviderID qualifies this technical integration, not a user source or data key.
const ProviderID = "configsource.nacos.v2"
const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrRead        fault.Kind = "fathomry." + ProviderID + ".read"
	ErrDecode      fault.Kind = "fathomry." + ProviderID + ".decode"
	ErrDenied      fault.Kind = "fathomry." + ProviderID + ".denied"
	ErrMissing     fault.Kind = "fathomry." + ProviderID + ".missing"
	ErrEmpty       fault.Kind = "fathomry." + ProviderID + ".empty"
	ErrClosed      fault.Kind = "fathomry." + ProviderID + ".closed"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrUnavailable fault.Kind = "fathomry." + ProviderID + ".unavailable"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
)

func fail(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

// RemoteError retains observed native response codes and deliberately inspectable
// server text. Error formatting does not disclose that text.
type RemoteError struct {
	private
	resultCode, errorCode int
	message               string
}

func (value *RemoteError) Error() string { return "nacos remote request failed" }

// ResultCode returns the native response result; zero is not positive evidence.
func (value *RemoteError) ResultCode() int {
	if value == nil {
		return 0
	}
	return value.resultCode
}

// ErrorCode returns the native error code without parsing diagnostic text.
func (value *RemoteError) ErrorCode() int {
	if value == nil {
		return 0
	}
	return value.errorCode
}

// Message deliberately exposes potentially sensitive native server text.
func (value *RemoteError) Message() string {
	if value == nil {
		return ""
	}
	return value.message
}

// HTTPStatus retains response status without credentials, URL or response body.
type HTTPStatus int

func (value HTTPStatus) Error() string { return fmt.Sprintf("nacos HTTP status %d", int(value)) }

type private struct{}

func (private) String() string   { return "nacos[restricted]" }
func (private) GoString() string { return "nacos[restricted]" }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("nacos: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("nacos: runtime reconstruction unsupported")
}
func restricted(state fmt.State)                     { _, _ = io.WriteString(state, "nacos[restricted]") }
func (*OptionsV1) Format(state fmt.State, _ rune)    { restricted(state) }
func (*ServerV1) Format(state fmt.State, _ rune)     { restricted(state) }
func (*KeyV1) Format(state fmt.State, _ rune)        { restricted(state) }
func (*Client) Format(state fmt.State, _ rune)       { restricted(state) }
func (*Document) Format(state fmt.State, _ rune)     { restricted(state) }
func (*Subscription) Format(state fmt.State, _ rune) { restricted(state) }
func (*Change) Format(state fmt.State, _ rune)       { restricted(state) }
func (*RemoteError) Format(state fmt.State, _ rune)  { restricted(state) }
func (*OptionsV1) LogValue() slog.Value              { return slog.StringValue("nacos[restricted]") }
func (*ServerV1) LogValue() slog.Value               { return slog.StringValue("nacos[restricted]") }
func (*KeyV1) LogValue() slog.Value                  { return slog.StringValue("nacos[restricted]") }
func (*Client) LogValue() slog.Value                 { return slog.StringValue("nacos[restricted]") }
func (*Document) LogValue() slog.Value               { return slog.StringValue("nacos[restricted]") }
func (*Subscription) LogValue() slog.Value           { return slog.StringValue("nacos[restricted]") }
func (*Change) LogValue() slog.Value                 { return slog.StringValue("nacos[restricted]") }
func (*RemoteError) LogValue() slog.Value            { return slog.StringValue("nacos[restricted]") }
