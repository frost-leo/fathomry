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

import "github.com/frost-leo/fathomry/failure/v1"

// Stable notification identities do not prescribe retry or business disposition.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMail)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMail)<<16 | 0x0002
	ErrSend          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMail)<<16 | 0x0003
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMail)<<16 | 0x0004
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMail)<<16 | 0x0005
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMail)<<16 | 0x0006
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMail)<<16 | 0x0007
)

func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrSend), definition(ErrLimit), definition(ErrState), definition(ErrCleanup), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The mail input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The selected mail capability cannot satisfy this profile."
	case ErrSend:
		identifier, message = "operation_failed", "The mail operation failed; external effects may be unknown."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A mail capability bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The mail handle cannot perform this operation."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Mail cleanup reported a failure; release is separate."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Mail runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.notification_mail." + identifier), Module: "fathomry", Component: "notification_mail", Revision: 1, Message: message}
}
