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

package surf

import (
	"errors"
	"net/url"

	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
)

func (op *operation) prepare(request *sdk.Request) error {
	if err := op.ctx.Err(); err != nil {
		return err
	}
	headers := request.GetRequest().Header
	if headers == nil {
		headers = make(http.Header)
		request.GetRequest().Header = headers
	}
	for key, values := range op.client.owner.native.Headers {
		headers[key] = append([]string(nil), values...)
	}
	for key, values := range op.original.Header {
		headers[key] = append([]string(nil), values...)
	}
	for _, hook := range op.client.owner.native.RequestMiddleware {
		if err := op.ctx.Err(); err != nil {
			return err
		}
		view := sdk.FathomryRequestView(request.GetRequest())
		err := invoke("request-middleware", func() error { return hook(view) })
		prepared := view.GetRequest()
		if prepared.Body != nil {
			err = errors.Join(err, failure(ErrUnsupported, "middleware-body"), op.closeRejected(prepared.Body))
		}
		if err != nil {
			return err
		}
		if !headerFits(prepared.Header, op.client.owner.settings.MaxHeaderBytes, true) {
			return failure(ErrLimit, "middleware-headers")
		}
		request.FathomryWithRequestHeaders(view)
	}
	if !headerFits(request.GetRequest().Header, op.client.owner.settings.MaxHeaderBytes, true) {
		return failure(ErrLimit, "request-headers")
	}
	return nil
}
func (op *operation) observe(response *sdk.Response) error {
	raw := response.GetResponse()
	op.mu.Lock()
	op.data.metadata = Metadata{present: true, status: int(response.StatusCode), protocol: string(response.Proto), url: response.URL.String(),
		headers: http.Header(response.Headers).Clone(), length: response.ContentLength}
	op.mu.Unlock()
	if !headerFits(raw.Header, op.client.owner.settings.MaxHeaderBytes, false) {
		return failure(ErrLimit, "response-headers")
	}
	for _, hook := range op.client.owner.native.ResponseMiddleware {
		if err := op.ctx.Err(); err != nil {
			return err
		}
		view := sdk.FathomryResponseView(response)
		if err := invoke("response-middleware", func() error { return hook(view) }); err != nil {
			return err
		}
	}
	return nil
}
func (op *operation) redirect(request *http.Request, via []*http.Request) error {
	if len(via) >= op.client.owner.settings.MaxRoundTrips {
		return failure(ErrLimit, "redirects")
	}
	if err := validateRequest(op.ctx, copyRequest(request, op.ctx), op.client.owner.settings); err != nil {
		return err
	}
	hook := op.client.owner.native.CheckRedirect
	if hook == nil {
		return nil
	}
	view := sdk.FathomryRequestView(request).GetRequest()
	history := make([]*http.Request, len(via))
	for index, item := range via {
		history[index] = sdk.FathomryRequestView(item).GetRequest()
	}
	err := invoke("redirect", func() error { return hook(view, history) })
	if view.Body != nil {
		err = errors.Join(err, failure(ErrUnsupported, "redirect-body"), op.closeRejected(view.Body))
	}
	if err == nil {
		if !headerFits(view.Header, op.client.owner.settings.MaxHeaderBytes, true) {
			return failure(ErrLimit, "redirect-headers")
		}
		request.Header = view.Header.Clone()
	}
	return err
}

type operationJar struct {
	op  *operation
	jar http.CookieJar
}

func cloneCookies(cookies []*http.Cookie, maximum int64) ([]*http.Cookie, error) {
	if len(cookies) > 4096 {
		return nil, failure(ErrLimit, "cookies")
	}
	result := make([]*http.Cookie, len(cookies))
	var bytes int64
	for index, cookie := range cookies {
		if cookie == nil {
			return nil, failure(ErrInput, "cookie")
		}
		if err := cookie.Valid(); err != nil {
			return nil, failure(ErrInput, "cookie", err)
		}
		bytes += int64(len(cookie.Name) + len(cookie.Value) + len(cookie.Path) + len(cookie.Domain) + len(cookie.Raw) + len(cookie.RawExpires) + 128)
		for _, field := range cookie.Unparsed {
			bytes += int64(len(field))
		}
		if bytes > maximum {
			return nil, failure(ErrLimit, "cookies")
		}
		copy := *cookie
		copy.Unparsed = append([]string(nil), cookie.Unparsed...)
		result[index] = &copy
	}
	return result, nil
}
func (jar operationJar) Cookies(uri *url.URL) []*http.Cookie {
	if !jar.op.work.enter() {
		return nil
	}
	defer jar.op.work.leave()
	copy := *uri
	var cookies []*http.Cookie
	err := invoke("cookie-read", func() error {
		var err error
		cookies, err = cloneCookies(jar.jar.Cookies(&copy), jar.op.client.owner.settings.MaxHeaderBytes)
		return err
	})
	if err != nil {
		jar.op.fail(err)
		return nil
	}
	return cookies
}
func (jar operationJar) SetCookies(uri *url.URL, cookies []*http.Cookie) {
	if !jar.op.work.enter() {
		return
	}
	defer jar.op.work.leave()
	copy := *uri
	values, err := cloneCookies(cookies, jar.op.client.owner.settings.MaxHeaderBytes)
	if err != nil {
		jar.op.fail(err)
		return
	}
	jar.op.fail(invoke("cookie-write", func() error { jar.jar.SetCookies(&copy, values); return nil }))
}
