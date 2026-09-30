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

import "github.com/frost-leo/fathomry/failure/v1"

// Stable configuration-capability errors do not imply retry or effect policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0001
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0002
	ErrRead          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0003
	ErrDecode        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0004
	ErrClose         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0005
	ErrClosed        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0006
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0007
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0008
	ErrDenied        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x0009
	ErrMissing       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x000a
	ErrEmpty         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x000b
	ErrUnavailable   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x000c
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x000d
	ErrWrite         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityNacos)<<16 | 0x000e
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
		definition(ErrDenied),
		definition(ErrMissing),
		definition(ErrEmpty),
		definition(ErrUnavailable),
		definition(ErrUnsupported),
		definition(ErrWrite),
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
	case ErrDenied:
		identifier, message = "access_denied", "The configuration service refused access."
	case ErrMissing:
		identifier, message = "document_missing", "A required configuration document is missing."
	case ErrEmpty:
		identifier, message = "document_empty", "A required configuration document is empty."
	case ErrUnavailable:
		identifier, message = "service_unavailable", "The configuration service could not complete the request."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The selected configuration protocol profile is unsupported."
	case ErrWrite:
		identifier, message = "write_failed", "The configuration mutation reported a failure; inspect its effect evidence."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.configsource_nacos." + identifier), Module: "fathomry", Component: "configsource_nacos", Revision: 1, Message: message}
}
func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
