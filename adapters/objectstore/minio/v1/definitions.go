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

package minio

import "github.com/frost-leo/fathomry/failure/v1"

// Stable codes classify failures, never retryability, rollback or effect state.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0002
	ErrAuthority     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0003
	ErrConnect       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0004
	ErrRead          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0005
	ErrWrite         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0006
	ErrList          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0007
	ErrRemove        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0008
	ErrMissing       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0009
	ErrDenied        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000a
	ErrExpired       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000b
	ErrCondition     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000c
	ErrIntegrity     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000d
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000e
	ErrProtocol      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x000f
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0010
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0011
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityMinIO)<<16 | 0x0012
)

// Definitions returns detached offline declarations for explicit composition.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrAuthority), definition(ErrConnect), definition(ErrRead), definition(ErrWrite), definition(ErrList), definition(ErrRemove), definition(ErrMissing), definition(ErrDenied), definition(ErrExpired), definition(ErrCondition), definition(ErrIntegrity), definition(ErrLimit), definition(ErrProtocol), definition(ErrCleanup), definition(ErrState), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The object-storage input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested object-storage capability is unsupported."
	case ErrAuthority:
		identifier, message = "authority_refused", "The selected source does not grant this object-storage operation."
	case ErrConnect:
		identifier, message = "connection_failed", "Object-storage construction or readiness failed."
	case ErrRead:
		identifier, message = "read_failed", "The object read failed; inspect partial data and cleanup evidence."
	case ErrWrite:
		identifier, message = "write_failed", "The object write failed; its remote effects may be unknown."
	case ErrList:
		identifier, message = "enumeration_failed", "Object enumeration failed or is incomplete."
	case ErrRemove:
		identifier, message = "removal_failed", "Object removal failed; inspect each target's evidence."
	case ErrMissing:
		identifier, message = "object_missing", "The service reported a missing object, version, bucket or upload."
	case ErrDenied:
		identifier, message = "access_denied", "The service denied object-storage access."
	case ErrExpired:
		identifier, message = "credentials_expired", "The service reported expired credentials."
	case ErrCondition:
		identifier, message = "condition_failed", "An object-storage condition was not satisfied."
	case ErrIntegrity:
		identifier, message = "integrity_failed", "Object integrity verification failed."
	case ErrLimit:
		identifier, message = "limit_exceeded", "An object-storage capability bound was exceeded."
	case ErrProtocol:
		identifier, message = "protocol_invalid", "The object-storage response did not satisfy the selected protocol."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Object-storage cleanup failed; local release and remote effects are separate."
	case ErrState:
		identifier, message = "invalid_state", "The object-storage handle cannot perform this operation in its current state."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Object-storage runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.objectstore_minio." + identifier), Module: "fathomry", Component: "objectstore_minio", Revision: 1, Message: message}
}
