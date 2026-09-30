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

package framework

import "github.com/frost-leo/fathomry/failure/v1"

// Stable identities describe composition, not service retry or effect policy.
const (
	ErrOptions       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityAssembly)<<16 | 0x0001
	ErrHandle        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityAssembly)<<16 | 0x0002
	ErrClosed        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityAssembly)<<16 | 0x0003
	ErrWait          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityAssembly)<<16 | 0x0004
	ErrClose         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityAssembly)<<16 | 0x0005
	ErrDelivery      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityAssembly)<<16 | 0x0006
	ErrPending       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityAssembly)<<16 | 0x0007
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityAssembly)<<16 | 0x0008
)

// Definitions supplies explicit assembly declarations for offline composition.
func Definitions() []failure.Definition {
	return []failure.Definition{
		definition(ErrOptions),
		definition(ErrHandle),
		definition(ErrClosed),
		definition(ErrWait),
		definition(ErrClose),
		definition(ErrDelivery),
		definition(ErrPending),
		definition(ErrSerialization),
	}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrOptions:
		identifier, message = "invalid_options", "The runtime composition or reception options are invalid."
	case ErrHandle:
		identifier, message = "invalid_handle", "The runtime composition handle is not initialized."
	case ErrClosed:
		identifier, message = "closed", "The runtime composition or receiver is closing or closed."
	case ErrWait:
		identifier, message = "wait_interrupted", "Waiting ended without confirming actual completion."
	case ErrClose:
		identifier, message = "cleanup_failed", "Runtime composition cleanup reported a failure."
	case ErrDelivery:
		identifier, message = "delivery_failed", "Required evidence reception failed; custody is retained."
	case ErrPending:
		identifier, message = "evidence_pending", "Required evidence remains unacknowledged."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Runtime composition handle serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.assembly." + identifier), Module: "fathomry", Component: "assembly", Revision: 1, Message: message}
}
func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
