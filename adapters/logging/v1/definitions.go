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

package logging

import "github.com/frost-leo/fathomry/failure/v1"

const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLogging)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLogging)<<16 | 0x0002
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLogging)<<16 | 0x0003
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLogging)<<16 | 0x0004
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityLogging)<<16 | 0x0005
)

// Definitions returns detached offline shared logging declarations.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrLimit), definition(ErrState), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var id, message string
	switch code {
	case ErrInput:
		id, message = "invalid_input", "The structured logging input or ingress options are invalid."
	case ErrUnsupported:
		id, message = "unsupported_value", "The logging value or requested ingress capability is unsupported."
	case ErrLimit:
		id, message = "limit_exceeded", "A structured logging or retained-ingress bound was exceeded."
	case ErrState:
		id, message = "invalid_state", "The logging ingress cannot perform this operation in its current state."
	case ErrSerialization:
		id, message = "runtime_serialization", "Logging runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.logging." + id), Module: "fathomry", Component: "logging", Revision: 1, Message: message}
}
