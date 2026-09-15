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
	"net/url"
	"sort"
	"strconv"
	"strings"

	http "github.com/sardanioss/http"
)

func token(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char <= 32 || char >= 127 || strings.ContainsRune("()<>@,;:\\\"/[]?={}", char) {
			return false
		}
	}
	return true
}
func fieldValue(value string) bool {
	for _, char := range value {
		if char == '\r' || char == '\n' || char == 0 || char < 32 && char != '\t' || char == 127 {
			return false
		}
	}
	return true
}
func headerFits(header http.Header, limit int64) bool {
	var count int64
	for key, values := range header {
		if !token(key) {
			return false
		}
		for _, value := range values {
			count += int64(len(key) + len(value) + 4)
			if count > limit || !fieldValue(value) {
				return false
			}
		}
	}
	return true
}
func validateRequest(ctx context.Context, request *http.Request, value settings) error {
	if ctx == nil || request == nil || request.URL == nil {
		return failure(ErrInput, "request")
	}
	if err := request.Context().Err(); err != nil {
		return failure(ErrState, "request-context", err, context.Cause(request.Context()))
	}
	address := request.URL
	if request.Method == "" || !token(request.Method) || request.Method == "CONNECT" || request.Method == "GET_0RTT" || request.Method == "HEAD_0RTT" ||
		address.Hostname() == "" || address.Scheme != "http" && address.Scheme != "https" || address.User != nil || address.Opaque != "" || len(address.String()) > 8192 ||
		!fieldValue(address.Host) || !fieldValue(request.Host) || !headerFits(request.Header, value.MaxHeaderBytes) || !headerFits(request.Trailer, value.MaxHeaderBytes) {
		return failure(ErrInput, "request")
	}
	if address.Scheme == "http" && value.Protocol != HTTP1 {
		return failure(ErrUnsupported, "cleartext-protocol")
	}
	if request.ContentLength > value.MaxRequestBytes {
		return failure(ErrLimit, "request-body")
	}
	if request.ContentLength < -1 {
		return failure(ErrInput, "content-length")
	}
	if request.ContentLength > 0 && (request.Body == nil || request.Body == http.NoBody) {
		return failure(ErrInput, "missing-request-body")
	}
	if len(request.TransferEncoding) > 0 && (value.Protocol != HTTP1 || len(request.TransferEncoding) != 1 || request.TransferEncoding[0] != "chunked" || request.ContentLength > 0) {
		return failure(ErrUnsupported, "transfer-encoding")
	}
	if request.Body != nil && nilLike(request.Body) {
		return failure(ErrInput, "request-body")
	}
	for key, values := range request.Header {
		if strings.EqualFold(key, "Transfer-Encoding") {
			return failure(ErrUnsupported, "transfer-encoding-header")
		}
		if strings.EqualFold(key, "Upgrade") || strings.EqualFold(key, "Proxy-Authorization") {
			return failure(ErrUnsupported, "request-headers")
		}
		if strings.EqualFold(key, "Content-Length") {
			if request.ContentLength == 0 && request.Body != nil && request.Body != http.NoBody {
				return failure(ErrInput, "ambiguous-zero-length")
			}
			if len(values) != 1 {
				return failure(ErrInput, "content-length")
			}
			length, err := strconv.ParseInt(values[0], 10, 64)
			if err != nil || length < 0 || length != request.ContentLength {
				return failure(ErrInput, "content-length", err)
			}
		}
	}
	return nil
}
func parseProxy(value string) (*url.URL, error) {
	if value == "" {
		return nil, nil
	}
	if len(value) > 8192 {
		return nil, failure(ErrLimit, "proxy")
	}
	address, err := url.Parse(value)
	if err != nil {
		return nil, failure(ErrInput, "proxy", err)
	}
	if address.Hostname() == "" || address.Path != "" && address.Path != "/" || address.RawQuery != "" || address.Fragment != "" || address.Opaque != "" ||
		(address.Scheme != "http" && address.Scheme != "https" && address.Scheme != "socks5" && address.Scheme != "socks5h") {
		return nil, failure(ErrUnsupported, "proxy")
	}
	if !fieldValue(address.Host) {
		return nil, failure(ErrInput, "proxy")
	}
	return address, nil
}
func freezeOptions(client *Client, options []RequestOptionsV1) (RequestOptionsV1, error) {
	if len(options) > 1 {
		return RequestOptionsV1{}, failure(ErrInput, "request-options")
	}
	var option RequestOptionsV1
	if len(options) == 1 {
		option = options[0]
	}
	if option.Proxy > ProxyAddress || client.owner.settings.RoutingLocked && option.Proxy != ProxyFromProvider ||
		option.Proxy != ProxyAddress && option.ProxyURL != "" || option.Proxy == ProxyAddress && option.ProxyURL == "" {
		return option, failure(ErrInput, "proxy-selection")
	}
	if option.Proxy == ProxyFromProvider {
		option.ProxyURL = client.owner.settings.ProxyURL
	}
	if _, err := parseProxy(option.ProxyURL); err != nil {
		return option, err
	}
	if client.owner.settings.Protocol == HTTP3 && option.ProxyURL != "" {
		return option, failure(ErrUnsupported, "http3-proxy")
	}
	if !headerFits(option.ConnectHeaders, client.owner.settings.MaxHeaderBytes) {
		return option, failure(ErrInput, "proxy-headers")
	}
	option.ConnectHeaders = option.ConnectHeaders.Clone()
	if len(option.ConnectHeaders) > 0 {
		if option.ProxyURL == "" {
			return option, failure(ErrInput, "proxy-headers-without-proxy")
		}
		proxy, _ := parseProxy(option.ProxyURL)
		if proxy.Scheme == "socks5" || proxy.Scheme == "socks5h" {
			return option, failure(ErrUnsupported, "socks-headers")
		}
		keys := make([]string, 0, len(option.ConnectHeaders))
		for key := range option.ConnectHeaders {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		normalized := make(http.Header)
		for _, key := range keys {
			normalized[http.CanonicalHeaderKey(key)] = append(normalized[http.CanonicalHeaderKey(key)], option.ConnectHeaders[key]...)
		}
		option.ConnectHeaders = normalized
	}
	option.HeaderOrder = append([]string(nil), option.HeaderOrder...)
	option.ExactHeaders = append(option.ExactHeaders[:0:0], option.ExactHeaders...)
	if len(option.HeaderOrder) > 1024 || len(option.ExactHeaders) > 1024 {
		return option, failure(ErrLimit, "header-order")
	}
	var bytes int64
	for _, key := range option.HeaderOrder {
		if !token(key) {
			return option, failure(ErrInput, "header-order")
		}
		bytes += int64(len(key))
	}
	for _, entry := range option.ExactHeaders {
		if strings.EqualFold(entry.Key, "Proxy-Authorization") || strings.EqualFold(entry.Key, "Upgrade") {
			return option, failure(ErrUnsupported, "exact-headers")
		}
		if !token(entry.Key) || !fieldValue(entry.Value) {
			return option, failure(ErrInput, "exact-headers")
		}
		bytes += int64(len(entry.Key) + len(entry.Value) + 4)
	}
	if bytes > client.owner.settings.MaxHeaderBytes {
		return option, failure(ErrLimit, "header-order")
	}
	if option.TLSOnly != nil {
		value := *option.TLSOnly
		option.TLSOnly = &value
	}
	return option, nil
}

func responseLength(response *http.Response) error {
	values := response.Header.Values("Content-Length")
	if len(values) == 0 {
		return nil
	}
	if len(values) != 1 {
		return failure(ErrIntegrity, "response-length")
	}
	length, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || length < 0 || response.ContentLength != length {
		return failure(ErrIntegrity, "response-length", err)
	}
	return nil
}
