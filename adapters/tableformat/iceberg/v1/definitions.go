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

package iceberg

import "github.com/frost-leo/fathomry/failure/v1"

const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0002
	ErrAuthority     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0003
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0004
	ErrProtocol      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0005
	ErrOperation     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0006
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0007
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0008
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityIceberg)<<16 | 0x0009
)

func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrAuthority), definition(ErrLimit), definition(ErrProtocol), definition(ErrOperation), definition(ErrState), definition(ErrCleanup), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var id, message string
	switch code {
	case ErrInput:
		id, message = "invalid_input", "The Iceberg input or settings are invalid."
	case ErrUnsupported:
		id, message = "unsupported_profile", "The selected native capability is unavailable."
	case ErrAuthority:
		id, message = "authority_refused", "The requested action is outside the source authority."
	case ErrLimit:
		id, message = "limit_exceeded", "An Iceberg capacity or data bound was exceeded."
	case ErrProtocol:
		id, message = "protocol_failed", "The Iceberg response failed validation."
	case ErrOperation:
		id, message = "operation_failed", "The Iceberg operation failed; inspect its effect evidence."
	case ErrState:
		id, message = "invalid_state", "The Iceberg handle cannot perform this operation."
	case ErrCleanup:
		id, message = "cleanup_failed", "Iceberg cleanup failed; actual release is separate."
	case ErrSerialization:
		id, message = "runtime_serialization", "Iceberg runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.tableformat_iceberg." + id), Module: "fathomry", Component: "tableformat_iceberg", Revision: 1, Message: message}
}
