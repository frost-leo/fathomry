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

package settings

import "github.com/frost-leo/fathomry/failure/v1"

// Numeric package-owned errors are independent of source/SDK statuses. Inspect
// their occurrences with failure.Inspect and match them with errors.Is.
const (
	ErrSnapshot      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySettings)<<16 | 0x0001
	ErrCopy          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySettings)<<16 | 0x0002
	ErrType          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySettings)<<16 | 0x0003
	ErrPath          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySettings)<<16 | 0x0004
	ErrStore         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySettings)<<16 | 0x0005
	ErrUnconfigured  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySettings)<<16 | 0x0006
	ErrConfigured    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySettings)<<16 | 0x0007
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilitySettings)<<16 | 0x0008
)

// Definitions returns detached declarations for explicit atlas composition.
// No registration, settings lookup or localization occurs.
func Definitions() []failure.Definition {
	result := make([]failure.Definition, 0, 8)
	for _, code := range []failure.Code{
		ErrSnapshot, ErrCopy, ErrType, ErrPath, ErrStore, ErrUnconfigured, ErrConfigured, ErrSerialization,
	} {
		result = append(result, definition(code))
	}
	return result
}

func definition(code failure.Code) failure.Definition {
	var name, message, description string
	switch code {
	case ErrSnapshot:
		name, message = "invalid_snapshot", "The settings snapshot is not prepared."
		description = "Prepare data with its owner-defined copy function before reading or publishing it."
	case ErrCopy:
		name, message = "invalid_copy", "The settings copy function is missing."
		description = "Supply an explicit synchronous, bounded and concurrency-safe copy function that isolates mutable data."
	case ErrType:
		name, message = "type_mismatch", "The selected settings type does not match the requested operation."
		description = "Use the exact declared Go type and traverse only explicitly named fields, string-keyed maps, arrays or slices."
	case ErrPath:
		name, message = "invalid_path", "The settings selection path is invalid or ambiguous."
		description = "Use bounded slash tokens with tilde escaping and unique explicit JSON field names; traversal must terminate within its bound."
	case ErrStore:
		name, message = "invalid_store", "The settings store is not initialized."
		description = "Create a typed store before publishing snapshots to its data domain."
	case ErrUnconfigured:
		name, message = "not_configured", "No settings snapshot is available."
		description = "Publish a prepared snapshot before capturing its reader, and explicitly configure the application before accessing its default."
	case ErrConfigured:
		name, message = "already_configured", "The application settings reader is already configured."
		description = "Publish through the original store, or use an independent reader for another data domain; the process default cannot be replaced."
	case ErrSerialization:
		name, message = "runtime_serialization", "Settings runtime handle serialization is unsupported."
		description = "Explicitly copy data for serialization; readers, stores and snapshot handles are not portable data contracts."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.settings." + name),
		Module: "fathomry", Component: "settings", Revision: 1, Message: message, Description: description}
}

func reject(code failure.Code, operation string) error {
	occurrence, err := failure.New(definition(code), failure.Location{Operation: operation})
	if err != nil {
		return err
	}
	return occurrence
}
