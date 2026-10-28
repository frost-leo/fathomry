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

package create

import "github.com/frost-leo/fathomry/failure/v1"

const (
	ErrInput    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0101
	ErrExists   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0102
	ErrCreate   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0103
	ErrRender   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0104
	ErrCanceled failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0105
	ErrPlatform failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectCreation)<<16 | 0x0106
)

// Definitions describes only the supported offline scaffold profile.
func Definitions() []failure.Definition {
	return []failure.Definition{
		{Code: ErrInput, Identifier: "fathomry.project_creation.invalid_input", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "Project creation inputs do not match the supported profile."},
		{Code: ErrExists, Identifier: "fathomry.project_creation.target_exists", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "The target already exists; no existing entry was adopted or overwritten."},
		{Code: ErrCreate, Identifier: "fathomry.project_creation.creation_failed", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "Project creation or handle closing failed. Inspect the retained creation report."},
		{Code: ErrRender, Identifier: "fathomry.project_creation.render_failed", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "The project template could not be prepared before creation."},
		{Code: ErrCanceled, Identifier: "fathomry.project_creation.canceled", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "Project creation was canceled; acknowledged filesystem effects remain."},
		{Code: ErrPlatform, Identifier: "fathomry.project_creation.unsupported_platform", Module: "fathomry", Component: "project_creation", Revision: 1, Message: "Project creation is supported on Linux local filesystems only."},
	}
}

func fail(code failure.Code, operation string, causes ...error) error {
	for _, definition := range Definitions() {
		if definition.Code == code {
			value, err := failure.New(definition, failure.Location{Operation: operation}, causes...)
			if err != nil {
				return err
			}
			return value
		}
	}
	return failure.ErrDefinition
}
