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
	"net"
	"net/url"
	"strings"

	http "github.com/sardanioss/http"
	"github.com/sardanioss/http/httptrace"
)

// RequestPolicyV1 freezes the admission-relevant settings of one prepared source.
// Its contents are inert. Readers and factories are never invoked by Snapshot.
type RequestPolicyV1 struct {
	private
	value           settings
	enforceResolver bool
	connectTo       map[string]string
	echExplicit     bool
}

// ValidateRequestV1 checks absolute input bounds without copying containers or
// invoking body readers, replay factories or borrowed callbacks.
func ValidateRequestV1(ctx context.Context, request *http.Request, options ...RequestOptionsV1) error {
	value := defaults(OptionsV1{Protocol: HTTP1, MaxRequestBytes: 1 << 30, MaxHeaderBytes: 1 << 20})
	return (RequestPolicyV1{value: value}).Validate(ctx, request, options...)
}

// Validate checks the exact prepared policy before native admission or copying.
func (policy RequestPolicyV1) Validate(ctx context.Context, request *http.Request, options ...RequestOptionsV1) error {
	if policy.value.MaxHeaderBytes == 0 || ctx == nil || request == nil {
		return failure(ErrInput, "request")
	}
	for _, parent := range []context.Context{ctx, request.Context()} {
		if trace := httptrace.ContextClientTrace(parent); trace != nil && trace.GotConn != nil {
			return failure(ErrUnsupported, "owning-trace")
		}
	}
	if request.Form != nil || request.PostForm != nil || request.MultipartForm != nil || request.RemoteAddr != "" || request.RequestURI != "" || request.TLS != nil || request.Cancel != nil || request.Response != nil || len(request.Proto) > 32 || len(request.Host) > 8192 {
		return failure(ErrUnsupported, "request-state")
	}
	if err := validateRequest(ctx, request, policy.value); err != nil {
		return err
	}
	if len(options) > 1 {
		return failure(ErrInput, "request-options")
	}
	var option RequestOptionsV1
	if len(options) > 0 {
		option = options[0]
	}
	if option.Proxy > ProxyAddress || policy.value.RoutingLocked && option.Proxy != ProxyFromProvider || option.Proxy != ProxyAddress && option.ProxyURL != "" || option.Proxy == ProxyAddress && option.ProxyURL == "" {
		return failure(ErrInput, "proxy-selection")
	}
	if _, err := parseProxy(option.ProxyURL); err != nil {
		return err
	}
	if !headerFits(option.ConnectHeaders, policy.value.MaxHeaderBytes) || len(option.HeaderOrder) > 1024 || len(option.ExactHeaders) > 1024 {
		return failure(ErrLimit, "request-options")
	}
	var size int64
	for _, key := range option.HeaderOrder {
		if !token(key) {
			return failure(ErrInput, "header-order")
		}
		size += int64(len(key)) + 16
		if size > policy.value.MaxHeaderBytes {
			return failure(ErrLimit, "header-order")
		}
	}
	for _, field := range option.ExactHeaders {
		if !token(field.Key) || !fieldValue(field.Value) || strings.EqualFold(field.Key, "Proxy-Authorization") || strings.EqualFold(field.Key, "Upgrade") {
			return failure(ErrInput, "exact-headers")
		}
		size += int64(len(field.Key)+len(field.Value)) + 32
		if size > policy.value.MaxHeaderBytes {
			return failure(ErrLimit, "header-order")
		}
	}
	if policy.enforceResolver {
		proxyURL := option.ProxyURL
		if option.Proxy == ProxyFromProvider {
			proxyURL = policy.value.ProxyURL
		}
		return policy.authorize(request.URL, proxyURL)
	}
	return nil
}

func (policy RequestPolicyV1) authorize(address *url.URL, proxyURL string) error {
	proxy, err := parseProxy(proxyURL)
	if err != nil {
		return err
	}
	if proxy != nil && (policy.value.Protocol == HTTP3 && (proxy.Scheme == "http" || proxy.Scheme == "https") || policy.value.Protocol != HTTP3 && proxy.Scheme == "masque") {
		return failure(ErrUnsupported, "proxy-protocol")
	}
	if policy.value.ResolverAddress == "" {
		host := address.Hostname()
		if target, ok := policy.connectTo[host]; ok {
			host = target
		}
		if proxy == nil || proxy.Scheme == "masque" {
			if net.ParseIP(host) == nil {
				return failure(ErrInput, "resolver-authority")
			}
		}
		if proxy != nil && net.ParseIP(proxy.Hostname()) == nil {
			return failure(ErrInput, "resolver-authority")
		}
		if proxy == nil && policy.value.Protocol == HTTP3 && !policy.value.DisableECH && !policy.echExplicit && net.ParseIP(address.Hostname()) == nil {
			return failure(ErrInput, "ech-resolver-authority")
		}
	}
	return nil
}

// SnapshotRequestV1 applies the supported hard request envelope before a public
// operation is admitted. Exact source limits are checked again by its policy.
// Body/GetBody remain borrowed and the native request context remains unchanged.
func SnapshotRequestV1(ctx context.Context, request *http.Request, options ...RequestOptionsV1) (*http.Request, RequestOptionsV1, error) {
	value := defaults(OptionsV1{Protocol: HTTP1, MaxRequestBytes: 1 << 30, MaxHeaderBytes: 1 << 20})
	return (RequestPolicyV1{value: value}).Snapshot(ctx, request, options...)
}

// Snapshot validates before copying the admitted client-native containers. It
// rejects server-only state and legacy cancellation rather than silently losing
// their authority. The caller must not mutate inputs concurrently with this call.
func (policy RequestPolicyV1) Snapshot(ctx context.Context, request *http.Request, options ...RequestOptionsV1) (*http.Request, RequestOptionsV1, error) {
	if err := policy.Validate(ctx, request, options...); err != nil {
		return nil, RequestOptionsV1{}, err
	}
	var option RequestOptionsV1
	if len(options) > 0 {
		option = options[0]
	}
	option.ConnectHeaders = option.ConnectHeaders.Clone()
	option.HeaderOrder = append([]string(nil), option.HeaderOrder...)
	option.ExactHeaders = append(option.ExactHeaders[:0:0], option.ExactHeaders...)
	if option.TLSOnly != nil {
		value := *option.TLSOnly
		option.TLSOnly = &value
	}
	address := *request.URL
	result := &http.Request{
		Method: request.Method, URL: &address, Proto: request.Proto,
		ProtoMajor: request.ProtoMajor, ProtoMinor: request.ProtoMinor,
		Header: request.Header.Clone(), Body: request.Body, GetBody: request.GetBody,
		ContentLength: request.ContentLength, TransferEncoding: append([]string(nil), request.TransferEncoding...),
		Close: request.Close, Host: request.Host, Trailer: request.Trailer.Clone(),
	}
	return result.WithContext(request.Context()), option, nil
}

func requestPolicy(value settings, native NativeOptionsV1) RequestPolicyV1 {
	return RequestPolicyV1{value: value, enforceResolver: true, connectTo: native.Transport.ConnectTo, echExplicit: len(native.Transport.ECHConfig) > 0}
}
