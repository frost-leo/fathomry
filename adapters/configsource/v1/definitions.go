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

package configsource

import "github.com/frost-leo/fathomry/failure/v1"

// Stable configuration-capability errors do not imply retry or effect policy.
const (
	ErrInput  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfigurationData)<<16 | 0x0001
	ErrLimit  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfigurationData)<<16 | 0x0002
	ErrRead   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfigurationData)<<16 | 0x0003
	ErrDecode failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfigurationData)<<16 | 0x0004
	// 0x0005..0x0007 remain reserved; preparation owns no lifecycle.
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfigurationData)<<16 | 0x0008
)

// Definitions supplies detached declarations for explicit offline composition.
func Definitions() []failure.Definition {
	return []failure.Definition{
		definition(ErrInput),
		definition(ErrLimit),
		definition(ErrRead),
		definition(ErrDecode),
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
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Configuration runtime handle serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.configuration_data." + identifier), Module: "fathomry", Component: "configuration_data", Revision: 1, Message: message}
}
