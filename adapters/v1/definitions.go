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

package adapters

import "github.com/frost-leo/fathomry/failure/v1"

// Stable identities describe the shared operation capability, not an Adapter
// layer band, SDK retry policy, mutation effect or business terminal state.
const (
	ErrOptions       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0001
	ErrHandle        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0002
	ErrClosed        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0003
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0004
	ErrWait          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0005
	ErrRequest       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0006
	ErrOutcome       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0007
	ErrSettled       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0008
	ErrReleased      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x0009
	ErrPending       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x000A
	ErrEvidence      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x000B
	ErrOperation     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x000C
	ErrSource        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x000D
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityOperation)<<16 | 0x000E
)

// Definitions returns detached declarations for explicit failure/i18n composition.
func Definitions() []failure.Definition {
	result := make([]failure.Definition, 0, 14)
	for _, code := range []failure.Code{ErrOptions, ErrHandle, ErrClosed, ErrLimit, ErrWait, ErrRequest, ErrOutcome, ErrSettled, ErrReleased, ErrPending, ErrEvidence, ErrOperation, ErrSource, ErrSerialization} {
		result = append(result, definition(code))
	}
	return result
}
func definition(code failure.Code) failure.Definition {
	var reason, message string
	switch code {
	case ErrOptions:
		reason, message = "invalid_options", "The operation runtime options or declaration are invalid."
	case ErrHandle:
		reason, message = "invalid_handle", "The operation runtime or handle is not initialized."
	case ErrClosed:
		reason, message = "closed", "The operation runtime or receiver is closing or closed."
	case ErrLimit:
		reason, message = "limit_exceeded", "An operation admission or ownership bound was exceeded."
	case ErrWait:
		reason, message = "wait_interrupted", "Waiting ended without confirming the requested completion."
	case ErrRequest:
		reason, message = "invalid_request", "The operation metadata or reservation is invalid."
	case ErrOutcome:
		reason, message = "missing_outcome", "Actual operation work ended without a reported outcome."
	case ErrSettled:
		reason, message = "outcome_settled", "The operation outcome is already being published or settled."
	case ErrReleased:
		reason, message = "authority_released", "This operation or delivery authority has already been released."
	case ErrPending:
		reason, message = "work_pending", "Actual operation work has not been confirmed released."
	case ErrEvidence:
		reason, message = "evidence_failed", "Required operation evidence could not be admitted or received."
	case ErrOperation:
		reason, message = "operation_failed", "The accepted operation reported a primary or cleanup failure."
	case ErrSource:
		reason, message = "source_unavailable", "The explicitly selected resource could not be borrowed."
	case ErrSerialization:
		reason, message = "runtime_serialization", "Operation runtime handle serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.operation." + reason), Module: "fathomry", Component: "operation",
		Revision: 1, Message: message, Details: failure.Contract{ID: "fathomry.operation.details", Version: 1}}
}
