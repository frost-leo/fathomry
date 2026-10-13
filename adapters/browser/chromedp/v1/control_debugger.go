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

import (
	"github.com/chromedp/cdproto/debugger"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// ResumeDebugger resumes only through an observed pause's reserved reply lane.
// The token expires on an observed resume/new pause or after submission. Native
// CDP cannot atomically exclude another session replacing the current pause.
func ResumeDebugger(params debugger.ResumeParams) Reply {
	return Reply{native: native.ResumeDebugger(params)}
}

// StepOutDebugger resumes until the current function returns. A later paused
// event must be consumed separately, even if native call-frame IDs are reused.
func StepOutDebugger() Reply { return Reply{native: native.StepOutDebugger()} }
