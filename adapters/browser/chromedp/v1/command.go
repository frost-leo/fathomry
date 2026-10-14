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
	"github.com/chromedp/cdproto/cdp"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Command delegates a finite page-local CDP command using its exact native
// parameter and result types. It is not a replacement for native readiness,
// navigation or Poll helpers. Parameters remain borrowed and immutable until
// execution returns, and are copied after admission before dispatch.
//
// The documented page-command set excludes browser-owner commands and commands
// that require managed resource identities or separate producer cleanup.
// Native result IDs are data, never authority to act on another resource.
// Native RPC acknowledgement does not establish HTTP success or undo effects.
// Header and user-agent overrides affect the page's requests, including work by
// other callers of a borrowed page. They are not per-request or origin filters.
func Command[P, R any](command cdp.Command[P, R], params P) Action[R] {
	return Action[R]{native: native.PageCommand(command, params), name: "command"}
}
