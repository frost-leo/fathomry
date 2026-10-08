// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package nuki

import (
	"context"

	http "github.com/nukilabs/http"
)

// RequestPolicyV1 is frozen source-specific metadata, not a second admission controller.
type RequestPolicyV1 struct{ MaxRequestBytes, MaxHeaderBytes int64 }

func (value settings) requestPolicy() RequestPolicyV1 {
	return RequestPolicyV1{MaxRequestBytes: value.MaxRequestBytes, MaxHeaderBytes: value.MaxHeaderBytes}
}

// ValidateRequestV1 bounds caller metadata before any public copy or admission.
// Exact source limits are enforced again after the public generation is adopted.
func ValidateRequestV1(ctx context.Context, request *http.Request, options ...RequestOptionsV1) error {
	if ctx == nil || request == nil || len(options) > 1 {
		return failure(ErrInput, "request")
	}
	if err := ctx.Err(); err != nil {
		return failure(ErrState, "request", err, context.Cause(ctx))
	}
	value := settings{MaxRequestBytes: 1 << 30, MaxHeaderBytes: 1 << 20}
	if err := validateRequest(request, value); err != nil {
		return err
	}
	if len(options) == 1 {
		option := options[0]
		if option.Version != 0 && option.Version != 1 {
			return failure(ErrInput, "request-version")
		}
		if option.ProxyMode != "" && option.ProxyMode != ProxyFromProvider && option.ProxyMode != ProxyDirect && option.ProxyMode != ProxyAddress {
			return failure(ErrInput, "proxy-choice")
		}
		if _, err := parseProxy(option.ProxyURL); err != nil {
			return err
		}
		if err := validateHeaders(option.ConnectHeaders, value.MaxHeaderBytes); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotRequestV1 copies only admitted native client metadata. Body and GetBody
// remain borrowed; this function never reads, invokes or closes them.
func SnapshotRequestV1(ctx context.Context, request *http.Request, policy RequestPolicyV1) (*http.Request, error) {
	if ctx == nil || policy.MaxRequestBytes < 1 || policy.MaxRequestBytes > 1<<30 || policy.MaxHeaderBytes < 1024 || policy.MaxHeaderBytes > 1<<20 {
		return nil, failure(ErrInput, "request-policy")
	}
	if err := validateRequest(request, settings{MaxRequestBytes: policy.MaxRequestBytes, MaxHeaderBytes: policy.MaxHeaderBytes}); err != nil {
		return nil, err
	}
	return snapshotRequest(request, ctx), nil
}

func snapshotRequest(request *http.Request, ctx context.Context) *http.Request {
	location := *request.URL
	exclusions := make(map[string]struct{}, len(request.ExcludedCookies))
	for name := range request.ExcludedCookies {
		exclusions[name] = struct{}{}
	}
	if request.ExcludedCookies == nil {
		exclusions = nil
	}
	copied := &http.Request{Method: request.Method, URL: &location, Proto: request.Proto, ProtoMajor: request.ProtoMajor, ProtoMinor: request.ProtoMinor,
		Header: request.Header.Clone(), Body: request.Body, GetBody: request.GetBody, ContentLength: request.ContentLength,
		TransferEncoding: append([]string(nil), request.TransferEncoding...), Close: request.Close, Host: request.Host, Trailer: request.Trailer.Clone(),
		Priority: request.Priority, ExcludedCookies: exclusions, DiscardResponseCookies: request.DiscardResponseCookies}
	return copied.WithContext(ctx)
}
