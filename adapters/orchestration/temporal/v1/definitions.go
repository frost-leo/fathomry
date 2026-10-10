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

package temporal

import "github.com/frost-leo/fathomry/failure/v1"

// Stable Temporal identities describe technical facts, not retry policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0001
	ErrAuthority     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0002
	ErrConnect       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0003
	ErrRPC           failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0004
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0005
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0006
	ErrExecution     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0007
	ErrWorker        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0008
	ErrTask          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x0009
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x000a
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityTemporal)<<16 | 0x000b
)

// Definitions returns detached offline metadata without constructing a source.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrAuthority), definition(ErrConnect), definition(ErrRPC), definition(ErrLimit), definition(ErrCleanup), definition(ErrExecution), definition(ErrWorker), definition(ErrTask), definition(ErrState), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The Temporal input or settings are invalid."
	case ErrAuthority:
		identifier, message = "authority_refused", "The Temporal operation exceeds the selected authority."
	case ErrConnect:
		identifier, message = "connection_failed", "Temporal client construction failed; retained ownership requires cleanup."
	case ErrRPC:
		identifier, message = "rpc_failed", "The Temporal RPC failed; remote effects may be unknown."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A Temporal data, work or ownership bound was exceeded."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Temporal cleanup failed or remains incomplete."
	case ErrExecution:
		identifier, message = "execution_failed", "The Temporal operation failed; remote completion is a separate fact."
	case ErrWorker:
		identifier, message = "worker_failed", "The Temporal Worker failed; actual local join remains separately observable."
	case ErrTask:
		identifier, message = "task_failed", "The Temporal user callback failed."
	case ErrState:
		identifier, message = "invalid_state", "The Temporal handle cannot perform this operation in its current state."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Temporal runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.temporal." + identifier), Module: "fathomry", Component: "temporal", Revision: 1, Message: message}
}
