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

package trino

import "github.com/frost-leo/fathomry/failure/v1"

// Stable identities describe technical failures, never retryability or effect state.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0002
	ErrAuthority     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0003
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0004
	ErrConnect       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0005
	ErrProtocol      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0006
	ErrOperation     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0007
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0008
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x0009
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTrino)<<16 | 0x000a
)

// Definitions returns detached, offline error declarations.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrAuthority), definition(ErrLimit), definition(ErrConnect), definition(ErrProtocol), definition(ErrOperation), definition(ErrState), definition(ErrCleanup), definition(ErrSerialization)}
}

func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The Trino input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested Trino capability is unsupported."
	case ErrAuthority:
		identifier, message = "authority_refused", "The Trino endpoint or continuation is outside the permitted authority."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A Trino capability bound was exceeded."
	case ErrConnect:
		identifier, message = "connection_failed", "The Trino connection or readiness check failed."
	case ErrProtocol:
		identifier, message = "protocol_failed", "The Trino protocol response violated the supported profile."
	case ErrOperation:
		identifier, message = "operation_failed", "The Trino operation failed; inspect its effect evidence."
	case ErrState:
		identifier, message = "invalid_state", "The Trino handle cannot perform this operation in its current state."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Trino cleanup reported a failure; completion is separate."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Trino runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.database_trino." + identifier), Module: "fathomry", Component: "database_trino", Revision: 1, Message: message}
}
