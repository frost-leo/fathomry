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

package duckdb

import "github.com/frost-leo/fathomry/failure/v1"

// Stable identities describe technical failures, never retryability or effect state.
// Native causes remain available through deliberate errors.Is/errors.As inspection;
// their text may contain SQL, values and local paths and must not be logged blindly.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDuckDB)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDuckDB)<<16 | 0x0002
	ErrNative        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDuckDB)<<16 | 0x0003
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDuckDB)<<16 | 0x0004
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDuckDB)<<16 | 0x0005
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDuckDB)<<16 | 0x0006
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityDuckDB)<<16 | 0x0007
)

// Definitions returns detached, offline error declarations without opening DuckDB.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrNative), definition(ErrLimit), definition(ErrState), definition(ErrCleanup), definition(ErrSerialization)}
}

func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The database input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested database capability is unsupported."
	case ErrNative:
		identifier, message = "native_failed", "The DuckDB native operation failed; inspect its effect evidence."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A database capability bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The database handle cannot perform this operation in its current state."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Database cleanup reported a failure; completion is separate."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Database runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.database_duckdb." + identifier), Module: "fathomry", Component: "database_duckdb", Revision: 1, Message: message}
}
