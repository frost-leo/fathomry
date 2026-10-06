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

package viper

import "github.com/frost-leo/fathomry/failure/v1"

// Stable configuration-capability errors do not imply retry or effect policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityViper)<<16 | 0x0001
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityViper)<<16 | 0x0002
	ErrRead          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityViper)<<16 | 0x0003
	ErrDecode        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityViper)<<16 | 0x0004
	ErrClose         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityViper)<<16 | 0x0005
	ErrClosed        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityViper)<<16 | 0x0006
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityViper)<<16 | 0x0007
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityViper)<<16 | 0x0008
)

// Definitions supplies detached declarations for explicit offline composition.
func Definitions() []failure.Definition {
	return []failure.Definition{
		definition(ErrInput),
		definition(ErrLimit),
		definition(ErrRead),
		definition(ErrDecode),
		definition(ErrClose),
		definition(ErrClosed),
		definition(ErrState),
		definition(ErrSerialization),
	}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The configuration input or declaration is invalid."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A configuration capability bound was exceeded."
	case ErrRead:
		identifier, message = "read_failed", "Configuration acquisition or inspection failed."
	case ErrDecode:
		identifier, message = "decode_failed", "Configuration decoding or preparation failed."
	case ErrClose:
		identifier, message = "cleanup_failed", "Configuration resource cleanup reported a failure."
	case ErrClosed:
		identifier, message = "closed", "The configuration owner is closing or closed."
	case ErrState:
		identifier, message = "completion_unconfirmed", "Configuration cleanup completion has not been confirmed."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Configuration runtime handle serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.configsource_viper." + identifier), Module: "fathomry", Component: "configsource_viper", Revision: 1, Message: message}
}
