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
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/storage"
	sdk "github.com/chromedp/chromedp"
)

func (session *Session) storageContext() (cdp.BrowserContextID, error) {
	if session == nil || session.browserContext == nil {
		return "", failure(ErrInput, "storage-context")
	}
	if session.borrowed && !session.sharedStorage {
		return "", failure(ErrState, "shared-storage-grant-required")
	}
	if session.browserID == "" && !(session.borrowed && session.defaultContext && session.defaultIdentityObserved) {
		return "", failure(ErrState, "storage-context-identity")
	}
	return session.browserID, nil
}

func contextCommand[P, R any](ctx context.Context, retained *Context, authorize func(*Session) (cdp.BrowserContextID, error), command cdp.Command[P, R], build func(cdp.BrowserContextID) (P, error)) (result R, err error) {
	if retained == nil || retained.contextState == nil {
		return result, failure(ErrInput, "context-command")
	}
	return runContextAction(ctx, retained, func(work context.Context, session *Session) (result R, err error) {
		identity, err := authorize(session)
		if err != nil {
			return result, err
		}
		if err := session.reserveCommand(); err != nil {
			return result, err
		}
		params, err := build(identity)
		if err != nil {
			return result, err
		}
		buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return result, err
		}
		var copied P
		if err := jsonv2.Unmarshal(buffer.data, &copied); err != nil {
			return result, err
		}
		return cdp.Call(session.commandWork(work, false), sdk.FromContext(session.owner.root).Browser, command, copied)
	})
}

// Cookies reads this retained context's cookie store, not the browser-wide store.
// A borrowed context needs its explicit SharedStorage grant, including for reads.
func (retained *Context) Cookies(ctx context.Context) ([]*network.Cookie, error) {
	result, err := contextCommand(ctx, retained, (*Session).storageContext, storage.GetCookies, func(id cdp.BrowserContextID) (storage.GetCookiesParams, error) {
		return storage.GetCookiesParams{BrowserContextID: id}, nil
	})
	return result.Cookies, err
}

// SetCookies borrows data until return and copies it before filling scoped parameters.
// A nil cookie slice or element is invalid. An explicit empty slice is a native no-op.
func (retained *Context) SetCookies(ctx context.Context, cookies []*network.CookieParam) error {
	_, err := contextCommand(ctx, retained, (*Session).storageContext, storage.SetCookies, func(id cdp.BrowserContextID) (storage.SetCookiesParams, error) {
		if cookies == nil || int64(len(cookies)) > retained.session.owner.settings.MaxCommandBytes/2 {
			return storage.SetCookiesParams{}, failure(ErrInput, "cookies")
		}
		for _, cookie := range cookies {
			if cookie == nil {
				return storage.SetCookiesParams{}, failure(ErrInput, "cookie")
			}
		}
		return storage.SetCookiesParams{BrowserContextID: id, Cookies: cookies}, nil
	})
	return err
}

// ClearCookies clears only the retained context selected by the same grant.
func (retained *Context) ClearCookies(ctx context.Context) error {
	_, err := contextCommand(ctx, retained, (*Session).storageContext, storage.ClearCookies, func(id cdp.BrowserContextID) (storage.ClearCookiesParams, error) {
		return storage.ClearCookiesParams{BrowserContextID: id}, nil
	})
	return err
}
