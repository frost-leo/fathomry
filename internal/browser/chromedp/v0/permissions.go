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
	"context"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
)

func (session *Session) permissionContext() (cdp.BrowserContextID, error) {
	if session == nil || session.browserContext == nil {
		return "", failure(ErrInput, "permission-context")
	}
	if session.borrowed && !session.sharedPermissions {
		return "", failure(ErrState, "shared-permissions-grant-required")
	}
	if session.browserID == "" && !(session.borrowed && session.defaultContext && session.defaultIdentityObserved) {
		return "", failure(ErrState, "permission-context-identity")
	}
	return session.browserID, nil
}

// SetPermission overrides only this retained context. Empty native origins keep
// their native all-origins meaning. Borrowed contexts need SharedPermissions.
// The caller's parameters are copied, never filled in place.
func (retained *Context) SetPermission(ctx context.Context, params browser.SetPermissionParams) error {
	_, err := contextCommand(ctx, retained, (*Session).permissionContext, browser.SetPermission, func(id cdp.BrowserContextID) (browser.SetPermissionParams, error) {
		if params.Permission == nil || params.BrowserContextID != "" && params.BrowserContextID != id {
			return browser.SetPermissionParams{}, failure(ErrInput, "permission-params")
		}
		copied := params
		copied.BrowserContextID = id
		return copied, nil
	})
	return err
}

// ResetPermissions resets all origin overrides in this context, not the browser.
// Native session shutdown also resets its modified contexts' permission overrides.
func (retained *Context) ResetPermissions(ctx context.Context) error {
	_, err := contextCommand(ctx, retained, (*Session).permissionContext, browser.ResetPermissions, func(id cdp.BrowserContextID) (browser.ResetPermissionsParams, error) {
		return browser.ResetPermissionsParams{BrowserContextID: id}, nil
	})
	return err
}
