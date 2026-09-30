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

package resource

import (
	"github.com/frost-leo/fathomry/failure/v1"
)

// Stable errors classify this public lifecycle boundary, not SDK retry policy.
const (
	ErrOptions       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0001
	ErrScope         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0002
	ErrClosed        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0003
	ErrSealed        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0004
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0005
	ErrSelection     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0006
	ErrBuild         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0007
	ErrUnavailable   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0008
	ErrLease         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x0009
	ErrSuperseded    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x000A
	ErrWait          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x000B
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x000C
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityResource)<<16 | 0x000D
)

// Details belongs to one direct resource occurrence. Target and Generation are
// binding-local ordering numbers, not durable versions. Zero means unspecified.
// Pending denotes unconfirmed owned work, never evidence of external rollback.
type Details struct {
	Scope      string
	Target     uint64
	Generation uint64
	Pending    bool
}

// Definitions returns detached declarations for explicit failure/i18n composition.
func Definitions() []failure.Definition {
	result := make([]failure.Definition, 0, 13)
	for _, code := range []failure.Code{
		ErrOptions, ErrScope, ErrClosed, ErrSealed, ErrLimit, ErrSelection, ErrBuild,
		ErrUnavailable, ErrLease, ErrSuperseded, ErrWait, ErrCleanup, ErrSerialization,
	} {
		result = append(result, definition(code))
	}
	return result
}

func definition(code failure.Code) failure.Definition {
	var name, message string
	switch code {
	case ErrOptions:
		name, message = "invalid_options", "The resource declaration or options are invalid."
	case ErrScope:
		name, message = "invalid_scope", "The resource scope or handle is not initialized."
	case ErrClosed:
		name, message = "closed", "The resource scope or observation is closing or closed."
	case ErrSealed:
		name, message = "declarations_sealed", "Resource declarations are already sealed."
	case ErrLimit:
		name, message = "limit_exceeded", "A resource ownership or admission bound was exceeded."
	case ErrSelection:
		name, message = "selection_failed", "The component configuration could not be selected."
	case ErrBuild:
		name, message = "construction_failed", "The resource candidate could not be constructed or adopted."
	case ErrUnavailable:
		name, message = "not_available", "No active resource generation is available."
	case ErrLease:
		name, message = "invalid_lease", "The resource lease is invalid or has already been released."
	case ErrSuperseded:
		name, message = "superseded", "The resource configuration target was superseded before adoption."
	case ErrWait:
		name, message = "wait_interrupted", "Waiting ended without confirming completion."
	case ErrCleanup:
		name, message = "cleanup_failed", "Resource cleanup reported a failure or remains incomplete."
	case ErrSerialization:
		name, message = "runtime_serialization", "Resource runtime handle serialization is unsupported."
	}
	return failure.Definition{
		Code: code, Identifier: failure.Identifier("fathomry.resource." + name),
		Module: "fathomry", Component: "resource", Revision: 1, Message: message,
		Details: failure.Contract{ID: "fathomry.resource.details", Version: 1},
	}
}

func fail(code failure.Code, operation, name string, details Details, causes ...error) error {
	value, err := failure.NewDetailed(definition(code),
		failure.Location{Operation: operation, Instance: name},
		details, func(value Details) Details { return value }, causes...)
	if err != nil {
		return err
	}
	return value
}
