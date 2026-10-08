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
	"context"
	"errors"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/nuki/v1"
	http "github.com/nukilabs/http"
)

// ProxyMode preserves explicit provider, direct and per-call proxy selection.
type ProxyMode string

const (
	ProxyFromProvider ProxyMode = "provider"
	ProxyDirect       ProxyMode = "direct"
	ProxyAddress      ProxyMode = "proxy"
)

// RequestOptions is runtime contract 1; Version zero selects 1. ForceHTTP3 does
// not enable a source-disabled protocol. CONNECT headers belong to the proxy.
// Containers, request metadata and native extensions must remain immutable until
// Do/Consume returns, including queued admission. Bodies and GetBody are borrowed
// only after native admission and remain borrowed until actual receipt release.
type RequestOptions struct {
	private
	Version        uint32
	ProxyMode      ProxyMode
	ProxyURL       string
	ConnectHeaders http.Header
	ForceHTTP3     bool
}

func requestOptions(ctx context.Context, input *http.Request, options []RequestOptions) ([]native.RequestOptionsV1, error) {
	if len(options) > 1 {
		return nil, fail(ErrInput, "request-options")
	}
	var selected []native.RequestOptionsV1
	if len(options) == 1 {
		value := options[0]
		selected = []native.RequestOptionsV1{{Version: value.Version, ProxyMode: native.ProxyMode(value.ProxyMode), ProxyURL: value.ProxyURL, ConnectHeaders: value.ConnectHeaders, ForceHTTP3: value.ForceHTTP3}}
	}
	if err := native.ValidateRequestV1(ctx, input, selected...); err != nil {
		return nil, translate(err, "request")
	}
	return selected, nil
}

// Do admits finite asynchronous work and returns before response completion.
// It retains the native request type, including Priority, ExcludedCookies and
// DiscardResponseCookies. ctx replaces Request.Context and owns actual work;
// canceling Receipt.Wait alone does not cancel the operation.
func (client *Client) Do(ctx context.Context, input *http.Request, options ...RequestOptions) (*adapters.Receipt[Result], error) {
	selected, err := requestOptions(ctx, input, options)
	if err != nil {
		return nil, err
	}
	var nativeErr error
	receipt, err := client.dispatch(ctx, "do", func(group *family) {
		receipt, err := group.native.Do(group.lifetime, group.rootCorrelation(), input, selected...)
		nativeErr = translate(err, "do")
		group.attach(receipt, err)
	})
	return receipt, errors.Join(err, nativeErr)
}

// Consume admits an asynchronous callback-scoped response, never an owning Open
// stream. The callback may inspect metadata and Read until it returns. Returning
// before EOF preserves Complete=false without inventing a read failure. The
// original source remains held through callback revocation and entered-read joins.
func (client *Client) Consume(ctx context.Context, input *http.Request, consume func(context.Context, *Response) error, options ...RequestOptions) (*adapters.Receipt[Result], error) {
	if consume == nil {
		return nil, fail(ErrInput, "consumer")
	}
	selected, err := requestOptions(ctx, input, options)
	if err != nil {
		return nil, err
	}
	var nativeErr error
	receipt, err := client.dispatch(ctx, "consume", func(group *family) {
		receipt, err := group.native.Consume(group.lifetime, group.rootCorrelation(), input, func(ctx context.Context, value *native.Response) error {
			response := &Response{native: value}
			defer response.revoked.Store(true)
			return consume(ctx, response)
		}, selected...)
		nativeErr = translate(err, "consume")
		group.attach(receipt, err)
	})
	return receipt, errors.Join(err, nativeErr)
}
