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

	http "github.com/enetx/http"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/surf/v1"
)

// RequestOptions is runtime input, not loadable data. Nil ProxyURL inherits the
// source; a pointer to empty explicitly selects direct. Multipart is optional;
// at most one options value is accepted. No failed route falls back to direct.
// Keep the request, ProxyURL value and header/options containers immutable until
// Do/Open returns: native metadata is frozen after public admission, not before
// queued waiting. Admitted readers and factories remain borrowed until cleanup.
type RequestOptions struct {
	private
	Version        uint32
	ProxyURL       *string
	ConnectHeaders http.Header
	Multipart      *Multipart
}

func requestOptions(values []RequestOptions) ([]native.RequestOptionsV1, error) {
	result := make([]native.RequestOptionsV1, len(values))
	for index, value := range values {
		multipart, err := nativeMultipart(value.Multipart)
		if err != nil {
			return nil, err
		}
		result[index] = native.RequestOptionsV1{Version: value.Version, ProxyURL: value.ProxyURL, ConnectHeaders: value.ConnectHeaders, Multipart: multipart}
	}
	return result, nil
}

// Do uses ctx instead of Request.Context, retaining bounded finite response data.
// cleanupCtx bounds only cleanup waiting. Admitted bodies/parts transfer Close
// responsibility; a non-nil receipt identifies independently observable work.
func (client *Client) Do(ctx, cleanupCtx context.Context, input *http.Request, options ...RequestOptions) (*adapters.Receipt[Result], error) {
	if cleanupCtx == nil || len(options) > 1 {
		return nil, fail(ErrInput, "do")
	}
	selected, err := requestOptions(options)
	if err != nil {
		return nil, err
	}
	var nativeErr error
	receipt, err := client.dispatch(ctx, "do", func(group *family) {
		receipt, err := group.native.Do(group.lifetime, cleanupCtx, group.rootCorrelation(), input, selected...)
		nativeErr = translate(err, "do")
		group.attach(receipt, err, nil)
	})
	return receipt, errors.Join(err, nativeErr)
}

// Open retains a response stream. Close remains mandatory after EOF; multipart
// upload streaming is independently declared by RequestOptions.Multipart.
func (client *Client) Open(ctx context.Context, input *http.Request, options ...RequestOptions) (*Stream, *adapters.Receipt[Result], error) {
	if len(options) > 1 {
		return nil, nil, fail(ErrInput, "open-response")
	}
	selected, err := requestOptions(options)
	if err != nil {
		return nil, nil, err
	}
	var stream *Stream
	var nativeErr error
	receipt, err := client.dispatch(ctx, "open-response", func(group *family) {
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
