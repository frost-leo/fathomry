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

package project

import "github.com/frost-leo/fathomry/failure/v1"

const (
	ErrDependency  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0001
	ErrDestination failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0002
	ErrPartial     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0003
)

// Definitions returns the project-creation capability's independent error data.
func Definitions() []failure.Definition {
	return []failure.Definition{
		{Code: ErrDependency, Identifier: "fathomry.project_creation.invalid_dependency", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "A supported Framework version or explicit development checkout is required."},
		{Code: ErrDestination, Identifier: "fathomry.project_creation.destination_unavailable", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "The destination must be absent and its parent must be accessible."},
		{Code: ErrPartial, Identifier: "fathomry.project_creation.incomplete_creation", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "Project creation did not complete; created files have been retained."},
	}
}
func fail(code failure.Code, causes ...error) error {
	for _, definition := range Definitions() {
		if definition.Code == code {
			value, err := failure.New(definition, failure.Location{Operation: "project_create"}, causes...)
			if err != nil {
				return err
			}
			return value
		}
	}
	return failure.ErrDefinition
}
