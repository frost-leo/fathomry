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
	"context"
	"errors"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1"
	http "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
)

// ProxySelection controls this request's route, not provider rotation.
type ProxySelection uint8

const (
	ProxyFromProvider ProxySelection = iota
	ProxyDirect
	ProxyAddress
)

// RequestOptions preserves native exact headers, ordering and TLS-only mode.
// At most one value is allowed. ExactHeaders replaces normal/preset headers and
// disables the jar for this request. CONNECT headers belong only to the proxy.
// Keep inputs immutable until Do/Open returns; admitted readers and factories
// remain borrowed until receipt release. A failed proxy never falls back direct.
type RequestOptions struct {
	private
	Proxy              ProxySelection
	ProxyURL           string
	ConnectHeaders     http.Header
	HeaderOrder        []string
	ExactHeaders       []fingerprint.HeaderPair
	TLSOnly            *bool
	DisableClientHints bool
	FollowRedirects    bool
}

func requestOptions(values []RequestOptions) []native.RequestOptionsV1 {
	result := make([]native.RequestOptionsV1, len(values))
	for index, value := range values {
		result[index] = native.RequestOptionsV1{Proxy: native.ProxySelection(value.Proxy),
			ProxyURL: value.ProxyURL, ConnectHeaders: value.ConnectHeaders,
			HeaderOrder: value.HeaderOrder, ExactHeaders: value.ExactHeaders,
			TLSOnly: value.TLSOnly, DisableClientHints: value.DisableClientHints,
			FollowRedirects: value.FollowRedirects}
	}
	return result
}

// Do retains bounded decoded response bytes. Both ctx and Request.Context may
// cancel native work; cleanupCtx bounds only cleanup waiting. Admitted bodies are
// closed natively. A non-nil receipt identifies independently observable work,
// including partial results and cleanup continuing after the caller stops waiting.
func (client *Client) Do(ctx, cleanupCtx context.Context, input *http.Request, options ...RequestOptions) (*adapters.Receipt[Result], error) {
	if cleanupCtx == nil || len(options) > 1 {
		return nil, fail(ErrInput, "do")
	}
	selected := requestOptions(options)
	if err := native.ValidateRequestV1(ctx, input, selected...); err != nil {
		return nil, translate(err, "do")
	}
	var nativeErr error
	receipt, err := client.dispatch(ctx, input.Context(), "do", func(group *family) {
		receipt, err := group.native.Do(group.lifetime, cleanupCtx, group.rootCorrelation(), input, selected...)
		nativeErr = translate(err, "do")
		group.attach(receipt, err, nil)
	})
	return receipt, errors.Join(err, nativeErr)
}

// Open returns a genuine retained response stream. Close remains mandatory even
// after EOF. Both method and native request contexts retain cancellation authority.
func (client *Client) Open(ctx context.Context, input *http.Request, options ...RequestOptions) (*Stream, *adapters.Receipt[Result], error) {
	if len(options) > 1 {
		return nil, nil, fail(ErrInput, "open-response")
	}
	selected := requestOptions(options)
	if err := native.ValidateRequestV1(ctx, input, selected...); err != nil {
		return nil, nil, translate(err, "open-response")
	}
	var stream *Stream
	var nativeErr error
	receipt, err := client.dispatch(ctx, input.Context(), "open-response", func(group *family) {
		value, receipt, err := group.native.Open(group.lifetime, group.rootCorrelation(), input, selected...)
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
