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

package nuki

import (
	"net/url"
	"slices"

	http "github.com/nukilabs/http"
)

type guardedJar struct {
	owner *owner
	jar   http.CookieJar
	op    *operation
}

func (jar *guardedJar) failed(err error) {
	if jar.op != nil {
		jar.op.primaryError(err)
	} else {
		jar.owner.recordCleanup(err)
	}
}
func (jar *guardedJar) Cookies(address *url.URL) (result []*http.Cookie) {
	if !jar.owner.callbacks.enter() {
		return nil
	}
	defer jar.owner.callbacks.leave()
	defer func() {
		if recovered := recover(); recovered != nil {
			jar.failed(callbackFailure(recovered))
			result = nil
		}
	}()
	copied := *address
	cookies, err := copyCookies(jar.jar.Cookies(&copied), jar.owner.settings.MaxHeaderBytes)
	if err != nil {
		jar.failed(err)
		return nil
	}
	return cookies
}
func (jar *guardedJar) SetCookies(address *url.URL, cookies []*http.Cookie) {
	if !jar.owner.callbacks.enter() {
		return
	}
	defer jar.owner.callbacks.leave()
	defer func() {
		if recovered := recover(); recovered != nil {
			jar.failed(callbackFailure(recovered))
		}
	}()
	copied, err := copyCookies(cookies, jar.owner.settings.MaxHeaderBytes)
	if err != nil {
		jar.failed(err)
		return
	}
	location := *address
	jar.jar.SetCookies(&location, copied)
}
func copyCookies(cookies []*http.Cookie, limit int64) ([]*http.Cookie, error) {
	if int64(len(cookies))*256 > limit {
		return nil, failure(ErrLimit, "cookies")
	}
	total := int64(len(cookies)) * 256
	for _, cookie := range cookies {
		if cookie == nil {
			return nil, failure(ErrInput, "cookie")
		}
		total += int64(len(cookie.Name) + len(cookie.Value) + len(cookie.Path) + len(cookie.Domain) + len(cookie.Raw) + len(cookie.RawExpires))
		for _, value := range cookie.Unparsed {
			total += int64(len(value)) + 32
		}
		if total > limit {
			return nil, failure(ErrLimit, "cookies")
		}
	}
	result := make([]*http.Cookie, len(cookies))
	for index, cookie := range cookies {
		copied := *cookie
		copied.Unparsed = slices.Clone(cookie.Unparsed)
		result[index] = &copied
	}
	return result, nil
}
