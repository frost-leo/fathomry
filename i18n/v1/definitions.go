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

package i18n

import "github.com/frost-leo/fathomry/failure/v1"

const (
	ErrCatalog       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0001
	ErrResource      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0002
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0003
	ErrLocale        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0004
	ErrMessage       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0005
	ErrArguments     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0006
	ErrBinding       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0007
	ErrPreferences   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0008
	ErrProjection    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x0009
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityI18n)<<16 | 0x000A
)

// Definitions supplies detached declarations for explicit atlas composition.
func Definitions() []failure.Definition {
	codes := []failure.Code{ErrCatalog, ErrResource, ErrLimit, ErrLocale, ErrMessage, ErrArguments, ErrBinding, ErrPreferences, ErrProjection, ErrSerialization}
	result := make([]failure.Definition, 0, len(codes))
	for _, code := range codes {
		result = append(result, definition(code))
	}
	return result
}
func definition(code failure.Code) failure.Definition {
	var name, message string
	switch code {
	case ErrCatalog:
		name, message = "invalid_catalog", "The localization catalog is not prepared."
	case ErrResource:
		name, message = "invalid_resource", "The localization resource is invalid."
	case ErrLimit:
		name, message = "limit_exceeded", "The localization input or output exceeds a supported bound."
	case ErrLocale:
		name, message = "invalid_locale", "The requested locale is invalid."
	case ErrMessage:
		name, message = "missing_message", "The localization message is not declared."
	case ErrArguments:
		name, message = "invalid_arguments", "The localization arguments do not match the message contract."
	case ErrBinding:
		name, message = "invalid_binding", "The error localization binding does not match its definition."
	case ErrPreferences:
		name, message = "invalid_preferences", "Application localization preferences are unavailable or invalid."
	case ErrProjection:
		name, message = "invalid_projection", "The component could not project safe localization arguments."
	case ErrSerialization:
		name, message = "runtime_serialization", "Localization runtime handle serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.i18n." + name), Module: "fathomry", Component: "i18n", Revision: 1, Message: message}
}
func reject(code failure.Code, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{}, causes...)
	if err != nil {
		return err
	}
	return value
}
