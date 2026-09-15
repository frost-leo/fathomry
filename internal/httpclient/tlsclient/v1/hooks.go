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

package tlsclient

import (
	"errors"
	"net/url"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
)

func (op *operation) notice(err error) {
	if err == nil {
		return
	}
	op.mu.Lock()
	if len(op.data.warnings) >= op.client.owner.settings.MaxExchanges*64 {
		if op.primary == nil {
			op.primary = failure(ErrLimit, "hook-evidence")
		}
	} else {
		op.data.warnings = append(op.data.warnings, err)
	}
	op.mu.Unlock()
}
func (op *operation) applyPreview(request, preview *http.Request) error {
	if preview.Body != nil || preview.GetBody != nil || preview.Response != nil || preview.URL == nil {
		return failure(ErrUnsupported, "native-owning-hook")
	}
	checked := *preview
	checked.Body, checked.GetBody = request.Body, request.GetBody
	if err := validateRequest(op.ctx, &checked, op.client.owner.settings); err != nil {
		return err
	}
	address := *preview.URL
	request.URL = &address
	request.Header = preview.Header.Clone()
	request.Trailer = preview.Trailer.Clone()
	request.Method, request.Host = preview.Method, preview.Host
	return nil
}
func (op *operation) before(request *http.Request) error {
	for _, hook := range op.client.owner.native.PreHooks {
		preview := previewRequest(request)
		hookErr, panicked := op.invokeHook("pre-hook", func() error { return hook(preview) })
		if hookErr != nil && (panicked || !errors.Is(hookErr, sdk.ErrContinueHooks)) {
			return hookErr
		}
		if hookErr != nil {
			op.notice(hookErr)
		}
		if err := op.applyPreview(request, preview); err != nil {
			return err
		}
	}
	return nil
}
func (op *operation) after(request *http.Request, response *http.Response, requestErr error) {
	for _, hook := range op.client.owner.native.PostHooks {
		var preview *http.Response
		if response != nil {
			value := *response
			value.Header = response.Header.Clone()
			value.Trailer = response.Trailer.Clone()
			value.TransferEncoding = append([]string(nil), response.TransferEncoding...)
			value.Body, value.TLS = nil, nil
			value.Request = previewRequest(request)
			preview = &value
		}
		hookErr, panicked := op.invokeHook("post-hook", func() error {
			return hook(&sdk.PostResponseContext{Request: previewRequest(request), Response: preview, Error: requestErr})
		})
		if hookErr != nil {
			op.notice(hookErr)
			if panicked || !errors.Is(hookErr, sdk.ErrContinueHooks) {
				return
			}
		}
	}
}

func (op *operation) invokeHook(phase string, hook func() error) (err error, panicked bool) {
	done, err := op.client.owner.enterCallback(op.ctx)
	if err != nil {
		return err, false
	}
	defer done()
	returned := false
	defer func() {
		if !returned {
			// Preserve native hook containment without formatting caller panic values.
			cause, _ := recover().(error)
			err, panicked = failure(ErrState, phase+"-panic", cause), true
		}
	}()
	err = hook()
	returned = true
	return err, false
}

func (op *operation) redirect(request *http.Request, previous []*http.Request) error {
	if len(previous) >= op.client.owner.settings.MaxExchanges {
		return failure(ErrLimit, "redirect")
	}
	callback := op.client.owner.native.CheckRedirect
	if callback == nil {
		return nil
	}
	done, err := op.client.owner.enterCallback(op.ctx)
	if err != nil {
		return err
	}
	defer done()
	preview := previewRequest(request)
	history := make([]*http.Request, len(previous))
	for index, prior := range previous {
		history[index] = previewRequest(prior)
	}
	if err := callback(preview, history); err != nil {
		return err
	}
	return op.applyPreview(request, preview)
}

type operationJar struct{ op *operation }

func (jar operationJar) Cookies(address *url.URL) []*http.Cookie {
	op := jar.op
	done, err := op.client.owner.enterCallback(op.ctx)
	if err != nil {
		op.fail(err)
		return nil
	}
	defer done()
	copied := *address
	cookies := op.client.owner.native.Jar.Cookies(&copied)
	result, err := copyCookies(cookies, op.client.owner.settings.MaxHeaderBytes)
	if err != nil {
		op.fail(err)
		return nil
	}
	return result
}
func (jar operationJar) SetCookies(address *url.URL, cookies []*http.Cookie) {
	op := jar.op
	done, err := op.client.owner.enterCallback(op.ctx)
	if err != nil {
		op.fail(err)
		return
	}
	defer done()
	values, err := copyCookies(cookies, op.client.owner.settings.MaxHeaderBytes)
	if err != nil {
		op.fail(err)
		return
	}
	copied := *address
	op.client.owner.native.Jar.SetCookies(&copied, values)
}
func copyCookies(input []*http.Cookie, maximum int64) ([]*http.Cookie, error) {
	if int64(len(input)) > maximum/64 {
		return nil, failure(ErrLimit, "cookies")
	}
	result := make([]*http.Cookie, 0, len(input))
	var total int64
	for _, cookie := range input {
		if cookie == nil {
			return nil, failure(ErrInput, "cookie")
		}
		total += 128 + int64(len(cookie.Name)+len(cookie.Value)+len(cookie.Path)+len(cookie.Domain)+len(cookie.RawExpires)+len(cookie.Raw))
		if total > maximum {
			return nil, failure(ErrLimit, "cookies")
		}
		for _, part := range cookie.Unparsed {
			total += 16 + int64(len(part))
			if total > maximum {
				return nil, failure(ErrLimit, "cookies")
			}
		}
		if !token(cookie.Name) || strings.ContainsAny(cookie.Value, "\";\\") {
			return nil, failure(ErrInput, "cookie")
		}
		for index := range len(cookie.Value) {
			if cookie.Value[index] < 32 || cookie.Value[index] >= 127 {
				return nil, failure(ErrInput, "cookie")
			}
		}
		clone := *cookie
		clone.Unparsed = append([]string(nil), cookie.Unparsed...)
		result = append(result, &clone)
	}
	return result, nil
}
