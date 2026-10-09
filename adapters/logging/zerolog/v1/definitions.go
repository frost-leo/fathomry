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

package zerolog

import "github.com/frost-leo/fathomry/failure/v1"

// Stable logging identities do not imply delivery, retry or durability policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityZerolog)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityZerolog)<<16 | 0x0002
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityZerolog)<<16 | 0x0003
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityZerolog)<<16 | 0x0004
	ErrWrite         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityZerolog)<<16 | 0x0005
	ErrMaintenance   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityZerolog)<<16 | 0x0006
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityZerolog)<<16 | 0x0007
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityZerolog)<<16 | 0x0008
)

// Definitions returns detached offline metadata without constructing any output.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrLimit), definition(ErrState), definition(ErrWrite), definition(ErrMaintenance), definition(ErrCleanup), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The logging input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested logging capability is unsupported."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A logging data, work or ownership bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The logging handle cannot perform this operation in its current state."
	case ErrWrite:
		identifier, message = "write_failed", "A logging destination failed; partial effects remain possible."
	case ErrMaintenance:
		identifier, message = "maintenance_failed", "Owned logging file maintenance failed; effects remain separate."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Logging cleanup reported a failure; completion is separate."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Logging runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.logging_zerolog." + identifier), Module: "fathomry", Component: "logging_zerolog", Revision: 1, Message: message}
}
