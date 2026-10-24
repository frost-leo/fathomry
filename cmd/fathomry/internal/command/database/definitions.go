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

package database

import (
	"context"
	"errors"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

// Stable database identities do not imply retryability or absence of effects.
const (
	ErrInput          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0001
	ErrConfiguration  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0002
	ErrClosed         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0003
	ErrBusy           failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0004
	ErrLimit          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0005
	ErrAssets         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0006
	ErrProfile        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0007
	ErrAccess         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0008
	ErrSchema         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x0009
	ErrDatabase       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x000a
	ErrCleanup        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x000b
	ErrCanceled       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x000c
	ErrSerialization  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x000d
	ErrDatabaseExists failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDatabaseControl)<<16 | 0x000e
)

// Definitions returns detached database-owned definitions for explicit composition.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrConfiguration), definition(ErrClosed), definition(ErrBusy), definition(ErrLimit), definition(ErrAssets), definition(ErrProfile), definition(ErrAccess), definition(ErrSchema), definition(ErrDatabase), definition(ErrCleanup), definition(ErrCanceled), definition(ErrSerialization), definition(ErrDatabaseExists)}
}
func definition(code failure.Code) failure.Definition {
	var name, message string
	switch code {
	case ErrInput:
		name, message = "invalid_input", "The database input or settings are invalid."
	case ErrConfiguration:
		name, message = "configuration_unavailable", "Explicit control database configuration is unavailable or failed."
	case ErrClosed:
		name, message = "closed", "The control database is closing or closed."
	case ErrBusy:
		name, message = "busy", "The control database or installer is busy."
	case ErrLimit:
		name, message = "limit_exceeded", "A control database admission limit was exceeded."
	case ErrAssets:
		name, message = "schema_assets_invalid", "The embedded schema assets failed validation."
	case ErrProfile:
		name, message = "schema_profile_unsupported", "The selected database or session profile is unsupported."
	case ErrAccess:
		name, message = "schema_access_unproven", "Required schema privileges could not be established."
	case ErrSchema:
		name, message = "schema_state_refused", "The observed schema state refuses automatic application."
	case ErrDatabase:
		name, message = "database_failed", "The control database operation failed; effect evidence remains separate."
	case ErrCleanup:
		name, message = "cleanup_failed", "Database cleanup failed or remains incomplete; known effects are preserved."
	case ErrCanceled:
		name, message = "canceled", "The control database operation was canceled."
	case ErrSerialization:
		name, message = "runtime_serialization", "Control database runtime serialization is unsupported."
	case ErrDatabaseExists:
		name, message = "database_exists", "The control database already exists; creation did not adopt or modify it."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.database_control." + name), Module: "fathomry", Component: "database_control", Revision: 1, Message: message}
}

// Fail constructs a control-database occurrence for the supplied operation while
// retaining native causes and operation-level cancellation/admission classification.
func Fail(code failure.Code, operation string, causes ...error) error {
	cause := errors.Join(causes...)
	if errors.Is(cause, adapters.ErrEvidence) || errors.Is(cause, adapters.ErrLimit) {
		code = ErrLimit
	} else if code == ErrDatabase && (errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded)) {
		code = ErrCanceled
	}
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}
