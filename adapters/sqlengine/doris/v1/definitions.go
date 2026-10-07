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

package doris

import "github.com/frost-leo/fathomry/failure/v1"

// Stable identities are technical classifications, never retry/effect policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0002
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0003
	ErrProtocol      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0004
	ErrTransport     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0005
	ErrSQL           failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0006
	ErrLoad          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0007
	ErrUncertain     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0008
	ErrRowQuality    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x0009
	ErrDuplicate     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x000a
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x000b
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x000c
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDoris)<<16 | 0x000d
)

// Definitions returns detached offline declarations.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrLimit), definition(ErrProtocol), definition(ErrTransport), definition(ErrSQL), definition(ErrLoad), definition(ErrUncertain), definition(ErrRowQuality), definition(ErrDuplicate), definition(ErrCleanup), definition(ErrState), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The Doris input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested Doris capability is unsupported by this client profile."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A Doris capability bound was exceeded."
	case ErrProtocol:
		identifier, message = "protocol_failed", "The Doris response violated the supported protocol profile."
	case ErrTransport:
		identifier, message = "transport_failed", "The Doris transport failed; remote effects may be unknown."
	case ErrSQL:
		identifier, message = "sql_failed", "The Doris SQL operation failed; inspect its effect evidence."
	case ErrLoad:
		identifier, message = "load_failed", "The Doris load or label observation failed."
	case ErrUncertain:
		identifier, message = "effect_uncertain", "Doris visibility or effects remain unconfirmed; this does not authorize replay."
	case ErrRowQuality:
		identifier, message = "row_quality_failed", "Doris row quality did not meet the strict load contract; effects are separate."
	case ErrDuplicate:
		identifier, message = "duplicate_label", "The Doris label already exists; matching this payload is unproven."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Doris cleanup reported a failure; local release is separate."
	case ErrState:
		identifier, message = "invalid_state", "The Doris owner or cursor cannot perform this operation in its current state."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Doris runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.database_doris." + identifier),
		Module: "fathomry", Component: "database_doris", Revision: 1, Message: message}
}
