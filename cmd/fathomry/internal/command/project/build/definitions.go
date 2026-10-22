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

package build

import (
	"context"

	"github.com/frost-leo/fathomry/failure/v1"
)

// Stable identities belong to project building, not a CLI layer or deployment.
const (
	ErrOptions    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0001
	ErrCompile    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0002
	ErrInspection failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0003
	ErrRejected   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0004
	ErrPublish    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0005
	ErrCanceled   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0006
	ErrLimit      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0007
	ErrOutput     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0008
	ErrCleanup    failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x0009
	ErrPlatform   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityProjectBuild)<<16 | 0x000A
)

// Definitions returns detached capability metadata. Native causes are not human text.
func Definitions() []failure.Definition {
	return []failure.Definition{
		{Code: ErrOptions, Identifier: "fathomry.project_build.invalid_options", Module: "fathomry", Component: "project_build", Revision: 1, Message: "Project build options or the root module are invalid."},
		{Code: ErrCompile, Identifier: "fathomry.project_build.compilation_failed", Module: "fathomry", Component: "project_build", Revision: 1, Message: "Go compilation failed; no new artifact was published."},
		{Code: ErrInspection, Identifier: "fathomry.project_build.inspection_failed", Module: "fathomry", Component: "project_build", Revision: 1, Message: "The executable did not provide a supported offline project declaration."},
		{Code: ErrRejected, Identifier: "fathomry.project_build.rejected", Module: "fathomry", Component: "project_build", Revision: 1, Message: "Project declarations failed validation or contain conflicting identities."},
		{Code: ErrPublish, Identifier: "fathomry.project_build.publication_failed", Module: "fathomry", Component: "project_build", Revision: 1, Message: "Artifact publication failed."},
		{Code: ErrCanceled, Identifier: "fathomry.project_build.canceled", Module: "fathomry", Component: "project_build", Revision: 1, Message: "Project building was canceled or its deadline expired."},
		{Code: ErrLimit, Identifier: "fathomry.project_build.limit_exceeded", Module: "fathomry", Component: "project_build", Revision: 1, Message: "A project build resource limit was exceeded."},
		{Code: ErrOutput, Identifier: "fathomry.project_build.output_failed", Module: "fathomry", Component: "project_build", Revision: 1, Message: "Build output could not be delivered; an artifact may already exist."},
		{Code: ErrCleanup, Identifier: "fathomry.project_build.cleanup_failed", Module: "fathomry", Component: "project_build", Revision: 1, Message: "Build cleanup failed; an artifact may already exist."},
		{Code: ErrPlatform, Identifier: "fathomry.project_build.unsupported_platform", Module: "fathomry", Component: "project_build", Revision: 1, Message: "Project building requires a native Linux or macOS target."},
	}
}
func fail(code failure.Code, causes ...error) error {
	for _, definition := range Definitions() {
		if definition.Code == code {
			value, err := failure.New(definition, failure.Location{Operation: "project_build"}, causes...)
			if err != nil {
				return err
			}
			return value
		}
	}
	return failure.ErrDefinition
}
func canceled(ctx context.Context) error {
	if ctx == nil {
		return fail(ErrOptions)
	}
	if ctx.Err() != nil {
		return fail(ErrCanceled, ctx.Err(), context.Cause(ctx))
	}
	return nil
}
