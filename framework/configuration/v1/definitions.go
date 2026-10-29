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

package configuration

import "github.com/frost-leo/fathomry/failure/v1"

// Stable identities describe configuration acceptance, not a Framework code band.
const (
	ErrDeclaration   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0001
	ErrSource        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0002
	ErrMissing       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0003
	ErrObservation   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0004
	ErrHandle        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0005
	ErrClosed        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0006
	ErrWait          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0007
	ErrPublish       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0008
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x0009
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x000a
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | 0x000b
)

// Details identifies a selected source slot; -1 means unknown/not applicable.
type Details struct{ SourceIndex int }

// Definitions supplies detached declarations for explicit component composition.
func Definitions() []failure.Definition {
	return []failure.Definition{
		definition(ErrDeclaration),
		definition(ErrSource),
		definition(ErrMissing),
		definition(ErrObservation),
		definition(ErrHandle),
		definition(ErrClosed),
		definition(ErrWait),
		definition(ErrPublish),
		definition(ErrLimit),
		definition(ErrSerialization),
		definition(ErrCleanup),
	}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrDeclaration:
		identifier, message = "invalid_declaration", "The configuration declaration or binding is invalid."
	case ErrSource:
		identifier, message = "source_failed", "The selected source could not provide a complete configuration observation."
	case ErrMissing:
		identifier, message = "required_document_missing", "A required configuration document is missing."
	case ErrObservation:
		identifier, message = "invalid_observation", "The source observation does not match the declared document selection."
	case ErrHandle:
		identifier, message = "invalid_handle", "The configuration state or producer is not initialized."
	case ErrClosed:
		identifier, message = "closed", "The configuration producer is closing or closed."
	case ErrWait:
		identifier, message = "wait_interrupted", "Waiting ended without confirming configuration producer completion."
	case ErrPublish:
		identifier, message = "publication_failed", "The accepted configuration could not be published."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A configuration orchestration bound was exceeded."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Configuration producer handle serialization is unsupported."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Configuration scenario cleanup failed; accepted data remains separately observable."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.configuration." + identifier), Module: "fathomry", Component: "configuration", Revision: 1, Message: message, Details: failure.Contract{ID: "fathomry.configuration.details", Version: 1}}
}
func fail(code failure.Code, operation string, causes ...error) error {
	return at(code, operation, -1, causes...)
}
func at(code failure.Code, operation string, index int, causes ...error) error {
	value, err := failure.NewDetailed(definition(code), failure.Location{Operation: operation}, Details{SourceIndex: index}, func(value Details) Details { return value }, causes...)
	if err != nil {
		return err
	}
	return value
}
