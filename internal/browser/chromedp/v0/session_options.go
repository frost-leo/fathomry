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

package chromedp

import "strings"

// SessionOptionsV1 supplies runtime BrowserContext construction inputs. Version
// zero selects 1. Empty proxy fields inherit Chrome's configured behavior; they
// do not assert direct routing. Native Chrome proxy syntax/bypass semantics apply.
// A per-context proxy is not evidence of complete connection/process isolation.
// These immutable strings are borrowed only during Run; no business policy runs.
type SessionOptionsV1 struct {
	private
	Version         uint32
	ProxyServer     string
	ProxyBypassList string
}

func sessionOptions(options []SessionOptionsV1, maximum int64) (SessionOptionsV1, error) {
	if len(options) > 1 {
		return SessionOptionsV1{}, failure(ErrInput, "session-options")
	}
	var input SessionOptionsV1
	if len(options) == 1 {
		input = options[0]
	}
	if input.Version != 0 && input.Version != 1 || int64(len(input.ProxyServer)+len(input.ProxyBypassList)) > maximum || strings.ContainsAny(input.ProxyServer+input.ProxyBypassList, "\x00\r\n") || input.ProxyServer == "" && input.ProxyBypassList != "" {
		return SessionOptionsV1{}, failure(ErrInput, "session-options")
	}
	return input, nil
}
