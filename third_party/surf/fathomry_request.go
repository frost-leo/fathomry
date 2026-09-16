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
	"context"
	"errors"
	"io"
	"sync"

	"github.com/enetx/http"
	"github.com/enetx/surf/internal/specclone"
	"github.com/enetx/surf/profiles"
	utls "github.com/refraction-networking/utls"
)

// FathomryApplyVariant applies any caller-selected native profile, without a
// Fathomry profile catalog. The caller transfers each returned container; callbacks
// are borrowed and must return fresh per-use mutable values.
func (builder *Builder) FathomryApplyVariant(variant profiles.Variant, os profiles.OSKey) *Builder {
	return (&Impersonate{builder: builder, os: os}).applyVariant(variant)
}

type fathomryFraming struct {
	io.ReadCloser
	expected, maximum, count int64
	once                     sync.Once
	closeErr                 error
	readErr                  error
}

func (body *fathomryFraming) Read(buffer []byte) (int, error) {
	if body.readErr != nil {
		return 0, body.readErr
	}
	if body.maximum > 0 {
		left := body.maximum - body.count
		if int64(len(buffer)) > left+1 {
			buffer = buffer[:left+1]
		}
	}
	count, err := body.ReadCloser.Read(buffer)
	body.count += int64(count)
	if body.maximum > 0 && body.count > body.maximum {
		err = errors.Join(ErrFathomryBodyLimit, err)
	}
	if body.expected >= 0 && body.count > body.expected {
		err = errors.Join(io.ErrUnexpectedEOF, err)
	}
	if errors.Is(err, io.EOF) && body.expected >= 0 && body.count != body.expected {
		err = errors.Join(io.ErrUnexpectedEOF, err)
	}
	body.readErr = err
	return count, err
}
func (body *fathomryFraming) Close() error {
	body.once.Do(func() { body.closeErr = body.ReadCloser.Close() })
	return body.closeErr
}

func fathomrySourceEOF(source io.Reader) error {
	var buffer [1]byte
	for range 100 {
		count, err := source.Read(buffer[:])
		if count != 0 {
			return errors.Join(errors.New("surf: trailing encoded data"), err)
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return io.ErrNoProgress
}

type fathomryContextBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	stop   func() bool
	once   sync.Once
	err    error
}

func (body *fathomryContextBody) Close() error {
	body.once.Do(func() {
		body.cancel()
		body.stop()
		body.err = body.ReadCloser.Close()
	})
	return body.err
}

// FathomryRequestView supplies native metadata manipulation without a usable
// client or body handle. Do returns an error. Views must not be retained.
func FathomryRequestView(request *http.Request) *Request {
	view := request.Clone(request.Context())
	view.Body = nil
	view.GetBody = nil
	view.Response = nil
	view.TLS = nil
	return &Request{request: view, cli: &Client{}, err: errors.New("surf: metadata view cannot execute")}
}

// FathomryCopyHelloSpec uses the native container copier. Opaque extensions and
// populated session state require the ownership-transferring factory instead.
func FathomryCopyHelloSpec(spec utls.ClientHelloSpec) utls.ClientHelloSpec {
	return *specclone.Clone(&spec)
}

// FathomryResponseView preserves native metadata helpers without exporting the
// owning client, transport, request body or response body.
func FathomryResponseView(response *Response) *Response {
	result := *response
	result.Client = &Client{}
	result.Body = nil
	result.request = nil
	if response.response != nil {
		raw := *response.response
		raw.Body = nil
		raw.TLS = nil
		if raw.Request != nil {
			raw.Request = FathomryRequestView(raw.Request).GetRequest()
		}
		raw.Header = raw.Header.Clone()
		raw.Trailer = raw.Trailer.Clone()
		result.response = &raw
	}
	result.Headers = Headers(http.Header(response.Headers).Clone())
	result.Cookies = make(Cookies, len(response.Cookies))
	for index, cookie := range response.Cookies {
		if cookie != nil {
			copy := *cookie
			copy.Unparsed = append([]string(nil), cookie.Unparsed...)
			result.Cookies[index] = &copy
		}
	}
	if response.URL != nil {
		uri := *response.URL
		result.URL = &uri
	}
	return &result
}

// FathomryWithRequestHeaders retains the SDK's profile-specific ordered header
// handling while accepting metadata prepared through a restricted native view.
func (request *Request) FathomryWithRequestHeaders(view *Request) {
	request.request.Header = view.request.Header.Clone()
}
