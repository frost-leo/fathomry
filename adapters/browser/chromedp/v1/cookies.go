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

	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

// Cookies returns deliberate cookie data from this retained context only.
// Borrowed contexts require ContextOptions.SharedStorage even for observation.
func (c *Context) Cookies(ctx context.Context) ([]*network.Cookie, *adapters.Receipt[Result], error) {
	return runContextOperation(ctx, c, "cookies", func(ctx context.Context) ([]*network.Cookie, error) { return c.native.Cookies(ctx) })
}

// SetCookies copies native cookie data after admission and before browser dispatch.
// Data remains borrowed and immutable until return. Native partition fields remain available.
func (c *Context) SetCookies(ctx context.Context, cookies []*network.CookieParam) (*adapters.Receipt[Result], error) {
	_, receipt, err := runContextOperation(ctx, c, "set_cookies", func(ctx context.Context) (sdk.Void, error) {
		return sdk.Void{}, c.native.SetCookies(ctx, cookies)
	})
	return receipt, err
}

// ClearCookies clears this context, never other isolated contexts or the browser.
// An explicit shared/default context grant can affect its other page users.
func (c *Context) ClearCookies(ctx context.Context) (*adapters.Receipt[Result], error) {
	_, receipt, err := runContextOperation(ctx, c, "clear_cookies", func(ctx context.Context) (sdk.Void, error) {
		return sdk.Void{}, c.native.ClearCookies(ctx)
	})
	return receipt, err
}

func (s *Scope) Cookies(ctx context.Context) ([]*network.Cookie, *adapters.Receipt[Result], error) {
	if s == nil {
		return nil, nil, fail(ErrInput, "scope")
	}
	return s.retained.Cookies(ctx)
}
func (s *Scope) SetCookies(ctx context.Context, cookies []*network.CookieParam) (*adapters.Receipt[Result], error) {
	if s == nil {
		return nil, fail(ErrInput, "scope")
	}
	return s.retained.SetCookies(ctx, cookies)
}
func (s *Scope) ClearCookies(ctx context.Context) (*adapters.Receipt[Result], error) {
	if s == nil {
		return nil, fail(ErrInput, "scope")
	}
	return s.retained.ClearCookies(ctx)
}
