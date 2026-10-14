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

import (
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/target"
	"strings"
)

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
	Borrow          bool
	ContextID       cdp.BrowserContextID
	TargetID        target.ID
	DefaultContext  bool
	// SharedStorage authorizes context-wide storage in an explicitly borrowed context.
	SharedStorage bool
	// SharedPermissions authorizes permission overrides in a borrowed context.
	SharedPermissions bool
	// SharedDownloads accepts context-wide download policy changes and reset.
	SharedDownloads bool
	// ManagedEvents selects explicit SubscribeEvent queues instead of the legacy
	// eager NextEvent observation queue. It applies to all pages of this context.
	ManagedEvents bool
}

func sessionOptions(options []SessionOptionsV1, maximum int64) (SessionOptionsV1, error) {
	if len(options) > 1 {
		return SessionOptionsV1{}, failure(ErrInput, "session-options")
	}
	var input SessionOptionsV1
	if len(options) == 1 {
		input = options[0]
	}
	if len(input.ContextID) > 128 || len(input.TargetID) > 128 || strings.ContainsAny(string(input.ContextID)+string(input.TargetID), "\x00\r\n") {
		return SessionOptionsV1{}, failure(ErrInput, "borrow-identity")
	}
	if input.Borrow {
		if (input.ContextID == "") != input.DefaultContext || input.ProxyServer != "" || input.ProxyBypassList != "" {
			return SessionOptionsV1{}, failure(ErrInput, "borrow-context")
		}
	} else if input.ContextID != "" || input.TargetID != "" || input.DefaultContext || input.SharedStorage || input.SharedPermissions || input.SharedDownloads {
		return SessionOptionsV1{}, failure(ErrInput, "borrow-grant")
	}
	if input.Version != 0 && input.Version != 1 || int64(len(input.ProxyServer)+len(input.ProxyBypassList)) > maximum || strings.ContainsAny(input.ProxyServer+input.ProxyBypassList, "\x00\r\n") || input.ProxyServer == "" && input.ProxyBypassList != "" {
		return SessionOptionsV1{}, failure(ErrInput, "session-options")
	}
	return input, nil
}
