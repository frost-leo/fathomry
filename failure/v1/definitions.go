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

package failure

// Package-owned admission failures use explicit numeric allocations. These values
// describe rejected API inputs, not an SDK's status or a requested error occurrence.
const (
	ErrCode          Code = ErrorPrefix | Code(FacilityFailure)<<16 | 0x0001
	ErrDefinition    Code = ErrorPrefix | Code(FacilityFailure)<<16 | 0x0002
	ErrLocation      Code = ErrorPrefix | Code(FacilityFailure)<<16 | 0x0003
	ErrDetails       Code = ErrorPrefix | Code(FacilityFailure)<<16 | 0x0004
	ErrLimit         Code = ErrorPrefix | Code(FacilityFailure)<<16 | 0x0005
	ErrCatalog       Code = ErrorPrefix | Code(FacilityFailure)<<16 | 0x0006
	ErrSerialization Code = ErrorPrefix | Code(FacilityFailure)<<16 | 0x0007
)

// Definitions returns this package's detached code declarations for explicit atlas
// composition. Nothing is registered globally or automatically added to a catalog.
func Definitions() []Definition {
	result := make([]Definition, 0, 7)
	for _, code := range []Code{ErrCode, ErrDefinition, ErrLocation, ErrDetails, ErrLimit, ErrCatalog, ErrSerialization} {
		result = append(result, builtin(code))
	}
	return result
}

func builtin(code Code) Definition {
	var name, message, description string
	switch code {
	case ErrCode:
		name, message = "invalid_code", "The public error code is invalid."
		description = "Use the 32-bit customer-failure layout with a nonzero facility and local number, or its unsigned decimal/hexadecimal spelling."
	case ErrDefinition:
		name, message = "invalid_definition", "The error definition is invalid or conflicts with another definition."
		description = "Codes and symbols must be unique, first-party allocations must match their owners, and each catalog facility must identify exactly one module/component."
	case ErrLocation:
		name, message = "invalid_location", "The error location is invalid."
		description = "Operation and instance labels must be bounded non-secret identifiers; empty labels explicitly mean unspecified."
	case ErrDetails:
		name, message = "invalid_details", "The error detail contract or copy function is invalid."
		description = "Declare the component-owned detail contract and provide its explicit copy function; failure does not prescribe the detail field types."
	case ErrLimit:
		name, message = "limit_exceeded", "The error contract exceeds a supported bound."
		description = "The rejected definition set, cause slots or shared metadata exceeded an explicit count or byte allowance."
	case ErrCatalog:
		name, message = "invalid_catalog", "The error definition catalog is not prepared."
		description = "Prepare an explicit catalog before querying it; no global registry or fallback catalog is selected."
	case ErrSerialization:
		name, message = "runtime_serialization", "Runtime error serialization is unsupported."
		description = "Native Go causes and runtime details do not define a portable or durable error protocol."
	}
	return Definition{Code: code, Identifier: Identifier("fathomry.failure." + name),
		Module: "fathomry", Component: "failure", Revision: 1, Message: message, Description: description}
}

func reject(code Code) *Error {
	return &Error{state: &errorState{diagnostic: Diagnostic{Definition: builtin(code)}}}
}
