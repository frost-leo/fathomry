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

package otel

import "github.com/frost-leo/fathomry/failure/v1"

// Stable Observability identities describe technical failures, not retry,
// delivery, durability or business-completion policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0002
	ErrEnvironment   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0003
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0004
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0005
	ErrExport        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0006
	ErrPartial       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0007
	ErrProtocol      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0008
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x0009
	ErrUndelivered   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x000A
	ErrRecursion     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x000B
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOTel)<<16 | 0x000C
)

// Definitions returns detached offline metadata without constructing providers,
// exporting telemetry or changing process-global configuration.
func Definitions() []failure.Definition {
	return []failure.Definition{
		definition(ErrInput), definition(ErrUnsupported), definition(ErrEnvironment),
		definition(ErrLimit), definition(ErrState), definition(ErrExport),
		definition(ErrPartial), definition(ErrProtocol), definition(ErrCleanup),
		definition(ErrUndelivered), definition(ErrRecursion), definition(ErrSerialization),
	}
}

func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The telemetry input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested telemetry capability is unsupported."
	case ErrEnvironment:
		identifier, message = "ambient_configuration", "Ambient telemetry configuration conflicts with the explicit profile."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A telemetry data, work or ownership bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The telemetry handle cannot perform this operation in its current state."
	case ErrExport:
		identifier, message = "export_failed", "Telemetry export failed; receiver effects may be unknown."
	case ErrPartial:
		identifier, message = "partial_export", "The telemetry receiver reported partial acceptance."
	case ErrProtocol:
		identifier, message = "invalid_response", "The telemetry exchange violated the admitted protocol."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Telemetry cleanup reported a failure; completion is separate."
	case ErrUndelivered:
		identifier, message = "undelivered_records", "Telemetry cleanup found records that were not submitted."
	case ErrRecursion:
		identifier, message = "recursive_operation", "A telemetry operation cannot recursively enter its own pipeline."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Telemetry runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.telemetry_otel." + identifier),
		Module: "fathomry", Component: "telemetry_otel", Revision: 1, Message: message}
}
