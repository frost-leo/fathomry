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

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
	"github.com/frost-leo/fathomry/internal/fault"
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

const errorTraversalLimit = 128

var nativeErrorMappings = [...]struct {
	native fault.Kind
	public failure.Code
}{
	{native.ErrInput, ErrInput}, {native.ErrLimit, ErrLimit}, {native.ErrRead, ErrRead}, {native.ErrWrite, ErrWrite},
	{native.ErrDecode, ErrDecode}, {native.ErrDenied, ErrDenied}, {native.ErrMissing, ErrMissing}, {native.ErrEmpty, ErrEmpty},
	{native.ErrClosed, ErrClosed}, {native.ErrState, ErrState}, {native.ErrUnavailable, ErrUnavailable}, {native.ErrUnsupported, ErrUnsupported},
}

func codeForKind(kind fault.Kind) failure.Code {
	for _, entry := range nativeErrorMappings {
		if entry.native == kind {
			return entry.public
		}
	}
	return 0
}

func translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if errorbridge.Classified(err) {
		return err
	}
	code := ErrRead
	if operation == "close" {
		code = ErrClose
	}
	direct, hasNativeIdentity := err.(interface{ Is(error) bool })
	if hasNativeIdentity {
		for _, entry := range nativeErrorMappings {
			if direct.Is(entry.native) {
				return fail(entry.public, operation, err)
			}
		}
	}
	_, forwarded := errorbridge.Inspect(err, errorTraversalLimit, codeForKind)
	if forwarded != nil {
		return forwarded
	}
	return fail(code, operation, err)
}

func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
