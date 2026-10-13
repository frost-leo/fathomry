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
	sdk "github.com/chromedp/chromedp"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// SetUploadFiles delegates native selector readiness and file-input selection.
// Paths explicitly name files on the browser host. Remote browser paths are not
// Go-local paths; this operation implements no remote file transfer service.
// The input remains borrowed and immutable until execution returns. Nil is
// invalid. An explicit empty slice is forwarded unchanged and is not a portable
// clear operation; use Clear to clear the input's selected files.
// Selection acknowledgement does not prove a later form upload has completed.
func SetUploadFiles[S TextSelector](selector S, files []browser.HostPath, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.SetUploadFiles(selector, files, nativeQuery(options)...), name: "upload_files"}
}
