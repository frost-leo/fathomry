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

import "github.com/frost-leo/fathomry/failure/v1"

// Stable notification identities do not prescribe retry or business disposition.
const (
	ErrInput           failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0001
	ErrUnsupported     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0002
	ErrOperation       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0003
	ErrLimit           failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0004
	ErrState           failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0005
	ErrCleanup         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0006
	ErrSerialization   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0007
	ErrAuth            failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0008
	ErrResponse        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x0009
	ErrProtocol        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x000A
	ErrAcknowledgement failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLark)<<16 | 0x000B
)

func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrOperation), definition(ErrLimit), definition(ErrState), definition(ErrCleanup), definition(ErrSerialization), definition(ErrAuth), definition(ErrResponse), definition(ErrProtocol), definition(ErrAcknowledgement)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The Lark input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The selected Lark capability cannot satisfy this profile."
	case ErrOperation:
		identifier, message = "operation_failed", "The Lark operation failed; external effects may be unknown."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A Lark capability bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The Lark handle cannot perform this operation."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Lark cleanup reported a failure; release is separate."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Lark runtime value serialization is unsupported."
	case ErrAuth:
		identifier, message = "authentication_failed", "Lark authentication failed; no broader authority is inferred."
	case ErrResponse:
		identifier, message = "invalid_response", "The Lark response is incomplete or invalid; effects may be unknown."
	case ErrProtocol:
		identifier, message = "invalid_protocol", "The Lark transport protocol was rejected."
	case ErrAcknowledgement:
		identifier, message = "acknowledgement_failed", "The Lark acknowledgement did not complete as requested."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.notification_lark." + identifier), Module: "fathomry", Component: "notification_lark", Revision: 1, Message: message}
}
