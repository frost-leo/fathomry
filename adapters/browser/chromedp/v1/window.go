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

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	resource "github.com/frost-leo/fathomry/internal/resource"
)

// Window is a non-owning native window observation/grant. It cannot survive
// closure of its original page or source, adopt a raw window ID, or close peers.
type Window struct {
	private
	native *native.Window
	page   *Page
}

func (window *Window) DataCopy() []byte {
	if window == nil || window.native == nil {
		return nil
	}
	return window.native.DataCopy()
}

// Window obtains a grant for a Page from this positively owned local browser.
// A connection Owner for a borrowed browser is not a process/window owner.
// The returned native bounds are a snapshot, not a claim about visible rendering.
func (owner *Owner) Window(ctx context.Context, page *Page) (window *Window, receipt *adapters.Receipt[Result], err error) {
	if owner == nil || owner.state == nil {
		return nil, nil, fail(ErrInput, "window-owner")
	}
	_, receipt, err = runPageOperation(ctx, page, "window", func(work context.Context) (jsonv2.Value, error) {
		finish, err := owner.state.use()
		if err != nil {
			return nil, err
		}
		defer finish()
		capability, _, err := resource.Bind(owner.state.assembly, owner.state.selected)
		if err != nil {
			return nil, err
		}
		value, err := native.AcquireWindow(work, capability, page.native)
		if err != nil {
			return nil, err
		}
		window = &Window{native: value, page: page}
		return jsonv2.Value(value.DataCopy()), nil
	})
	if err != nil {
		window = nil
	}
	return window, receipt, err
}

// RunWindowCommand delegates native GetWindowBounds, SetWindowBounds and
// SetContentsSize. WindowID must be zero or match the frozen native identity.
// Native optional bounds and zero values are preserved, with deep copying before
// dispatch. Membership revalidation costs one extra source-owner CDP command.
// It is not atomic against another controller moving the target to another window.
// Mutations need no new cleanup handle and are not automatically reverted.
func RunWindowCommand[P, R any](ctx context.Context, window *Window, command cdp.Command[P, R], params P) (result R, receipt *adapters.Receipt[Result], err error) {
	if window == nil || window.native == nil || window.page == nil {
		return result, nil, fail(ErrInput, "window")
	}
	return runPageOperation(ctx, window.page, "window_command", func(work context.Context) (R, error) {
		return native.RunWindowCommand(work, window.native, command, params)
	})
}
