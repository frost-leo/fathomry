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

package httpcloak

import (
	"net/url"

	http "github.com/sardanioss/http"
)

type operationJar struct{ op *operation }

func (jar operationJar) Cookies(address *url.URL) []*http.Cookie {
	if jar.op.ctx.Err() != nil {
		return nil
	}
	location := *address
	values := jar.op.client.owner.native.Jar.Cookies(&location)
	cookies, err := copyCookies(values, jar.op.client.owner.settings.MaxHeaderBytes)
	if err != nil {
		jar.op.fail(err)
		return nil
	}
	return cookies
}

func (jar operationJar) SetCookies(address *url.URL, values []*http.Cookie) {
	if jar.op.ctx.Err() != nil {
		return
	}
	cookies, err := copyCookies(values, jar.op.client.owner.settings.MaxHeaderBytes)
	if err != nil {
		jar.op.fail(err)
		return
	}
	location := *address
	jar.op.client.owner.native.Jar.SetCookies(&location, cookies)
}

func copyCookies(values []*http.Cookie, maximum int64) ([]*http.Cookie, error) {
	if int64(len(values)) > maximum/256 {
		return nil, failure(ErrLimit, "cookies")
	}
	remaining := maximum - int64(len(values))*256
	for _, cookie := range values {
		if cookie == nil {
			return nil, failure(ErrInput, "cookie")
		}
		for _, field := range []string{cookie.Name, cookie.Value, cookie.Path, cookie.Domain, cookie.Raw, cookie.RawExpires} {
			if int64(len(field)) > remaining {
				return nil, failure(ErrLimit, "cookies")
			}
			remaining -= int64(len(field))
		}
		if int64(len(cookie.Unparsed)) > remaining/16 {
			return nil, failure(ErrLimit, "cookies")
		}
		remaining -= int64(len(cookie.Unparsed)) * 16
		for _, field := range cookie.Unparsed {
			if int64(len(field)) > remaining {
				return nil, failure(ErrLimit, "cookies")
			}
			remaining -= int64(len(field))
		}
	}
	result := make([]*http.Cookie, len(values))
	for index, cookie := range values {
		copied := *cookie
		copied.Unparsed = append([]string(nil), cookie.Unparsed...)
		result[index] = &copied
	}
	return result, nil
}
