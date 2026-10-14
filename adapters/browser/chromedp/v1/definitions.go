/*
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

package chromedp

import "github.com/frost-leo/fathomry/failure/v1"

const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityChromedp)<<16 | 1
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityChromedp)<<16 | 2
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityChromedp)<<16 | 3
	ErrOperation     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityChromedp)<<16 | 4
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityChromedp)<<16 | 5
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityChromedp)<<16 | 6
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityChromedp)<<16 | 7
)

func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrState), definition(ErrLimit), definition(ErrOperation), definition(ErrCleanup), definition(ErrUnsupported), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var id, message string
	switch code {
	case ErrInput:
		id, message = "invalid_input", "The Browser input or settings are invalid."
	case ErrState:
		id, message = "invalid_state", "The Browser handle cannot perform this operation."
	case ErrLimit:
		id, message = "limit_exceeded", "A Browser capacity or data bound was exceeded."
	case ErrOperation:
		id, message = "operation_failed", "The Browser operation failed; inspect its effect evidence."
	case ErrCleanup:
		id, message = "cleanup_failed", "Browser cleanup failed; actual release is separate."
	case ErrUnsupported:
		id, message = "unsupported_capability", "The selected Browser capability is unavailable."
	case ErrSerialization:
		id, message = "runtime_serialization", "Browser runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.browser_chromedp." + id), Module: "fathomry", Component: "browser_chromedp", Revision: 1, Message: message}
}
