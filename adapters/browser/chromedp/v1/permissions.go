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
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

// SetPermission overrides this context's permission settings. Borrowed contexts
// require ContextOptions.SharedPermissions. Empty native origins mean all origins.
// Parameters remain borrowed and immutable until return, and are copied after
// admission before the retained context identity is filled. A conflicting native
// BrowserContextID is rejected, not used to select another context.
//
// Overrides affect every user of that context. Native control-session shutdown
// resets all permission overrides in each context that session modified, including
// overrides another controller set. SharedPermissions explicitly accepts this
// native shared-context behavior; it does not promise per-controller isolation.
func (c *Context) SetPermission(ctx context.Context, params browser.SetPermissionParams) (*adapters.Receipt[Result], error) {
	_, receipt, err := runContextOperation(ctx, c, "set_permission", func(ctx context.Context) (sdk.Void, error) {
		return sdk.Void{}, c.native.SetPermission(ctx, params)
	})
	return receipt, err
}

// ResetPermissions resets all permission overrides in this retained context.
// It needs the same explicit SharedPermissions grant for a borrowed context.
func (c *Context) ResetPermissions(ctx context.Context) (*adapters.Receipt[Result], error) {
	_, receipt, err := runContextOperation(ctx, c, "reset_permissions", func(ctx context.Context) (sdk.Void, error) {
		return sdk.Void{}, c.native.ResetPermissions(ctx)
	})
	return receipt, err
}

func (s *Scope) SetPermission(ctx context.Context, params browser.SetPermissionParams) (*adapters.Receipt[Result], error) {
	if s == nil {
		return nil, fail(ErrInput, "scope")
	}
	return s.retained.SetPermission(ctx, params)
}
func (s *Scope) ResetPermissions(ctx context.Context) (*adapters.Receipt[Result], error) {
	if s == nil {
		return nil, fail(ErrInput, "scope")
	}
	return s.retained.ResetPermissions(ctx)
}
