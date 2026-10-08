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

package surf

import "github.com/frost-leo/fathomry/failure/v1"

// Stable Network identities classify technical failures, not retry or effect policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0002
	ErrTransport     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0003
	ErrRead          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0004
	ErrIntegrity     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0005
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0006
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0007
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0008
	ErrCallback      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x000A
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySurf)<<16 | 0x0009
)

// Definitions returns detached offline metadata without constructing HTTP sources.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrTransport), definition(ErrRead), definition(ErrIntegrity), definition(ErrLimit), definition(ErrState), definition(ErrCleanup), definition(ErrCallback), definition(ErrSerialization)}
}

func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The HTTP input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested HTTP capability is unsupported."
	case ErrTransport:
		identifier, message = "transport_failed", "The HTTP transport failed; remote effects may be unknown."
	case ErrRead:
		identifier, message = "read_failed", "Reading the HTTP input or response failed."
	case ErrIntegrity:
		identifier, message = "integrity_failed", "The HTTP response is incomplete or malformed."
	case ErrLimit:
		identifier, message = "limit_exceeded", "An HTTP capability bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The HTTP handle cannot perform this operation in its current state."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "HTTP cleanup reported a failure; completion is separate."
	case ErrCallback:
		identifier, message = "callback_failed", "An HTTP callback failed."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "HTTP runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.http_surf." + identifier), Module: "fathomry", Component: "http_surf", Revision: 1, Message: message}
}
