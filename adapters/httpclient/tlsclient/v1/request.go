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
	"context"
	"errors"
	http "github.com/bogdanfinn/fhttp"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1"
)

// ProxySelection chooses runtime routing, not a provider or rotation policy.
type ProxySelection uint8

const (
	ProxyFromProvider ProxySelection = iota
	ProxyDirect
	ProxyAddress
)

// RequestOptions freezes a route before native dispatch. At most one is allowed.
// Explicit proxy URL credentials are sensitive; a locked provider refuses any
// non-provider selection. A failed selection never falls back to direct access.
type RequestOptions struct {
	private
	Proxy          ProxySelection
	ProxyURL       string
	ConnectHeaders http.Header
}

func requestOptions(values []RequestOptions) []native.RequestOptionsV1 {
	result := make([]native.RequestOptionsV1, len(values))
	for index, value := range values {
		result[index] = native.RequestOptionsV1{Proxy: native.ProxySelection(value.Proxy), ProxyURL: value.ProxyURL, ConnectHeaders: value.ConnectHeaders}
	}
	return result
}

// Do retains bounded finite response data. ctx replaces Request.Context and owns
// the native operation; cleanupCtx bounds only cleanup waiting. A non-nil receipt
// identifies accepted work and independent evidence even alongside an error.
// Admitted Body/GetBody are borrowed through receipt release and closed natively.
func (client *Client) Do(ctx, cleanupCtx context.Context, input *http.Request, options ...RequestOptions) (*adapters.Receipt[Result], error) {
	if cleanupCtx == nil || len(options) > 1 {
		return nil, fail(ErrInput, "do")
	}
	var nativeErr error
	receipt, err := client.dispatch(ctx, "do", func(group *family) {
		receipt, err := group.native.Do(group.lifetime, cleanupCtx, group.rootCorrelation(), input, requestOptions(options)...)
		nativeErr = translate(err, "do")
		group.attach(receipt, err, nil)
	})
	return receipt, errors.Join(err, nativeErr)
}

// Open retains a bounded native response stream. Call Close even after EOF;
// headers do not complete the operation. Owner/context cancellation also initiates
// cleanup, while actual late work keeps its original source generation.
func (client *Client) Open(ctx context.Context, input *http.Request, options ...RequestOptions) (*Stream, *adapters.Receipt[Result], error) {
	if len(options) > 1 {
		return nil, nil, fail(ErrInput, "open-response")
	}
	var stream *Stream
	var nativeErr error
	receipt, err := client.dispatch(ctx, "open-response", func(group *family) {
		value, receipt, err := group.native.Open(group.lifetime, group.rootCorrelation(), input, requestOptions(options)...)
		nativeErr = translate(err, "open-response")
		var cleanup func(context.Context) error
		if value != nil {
			stream = &Stream{native: value, receipt: group.call.Receipt()}
			cleanup = value.Close
		}
		group.attach(receipt, err, cleanup)
	})
	return stream, receipt, errors.Join(err, nativeErr)
}
