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
	"net"
	"net/url"
	"reflect"
	"strings"
	"unicode/utf8"

	http "github.com/enetx/http"
	"github.com/enetx/http/httptrace"
)

// RequestOptionsV1 freezes runtime routing before queueing. Nil ProxyURL inherits
// instance routing; an explicit empty value chooses direct routing. It does not
// change the instance identity or create independent admission/capacity.
type RequestOptionsV1 struct {
	private
	Version        uint32
	ProxyURL       *string
	ConnectHeaders http.Header
}
type routeChoice struct {
	proxy   string
	headers http.Header
}

func (route routeChoice) mode() string {
	if route.proxy == "" {
		return "direct"
	}
	value, _ := url.Parse(route.proxy)
	return value.Scheme
}

func parseProxy(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 8192 || !fieldValue(raw) {
		return nil, failure(ErrInput, "proxy")
	}
	value, err := url.Parse(raw)
	if err != nil || value.Opaque != "" || value.Hostname() == "" || value.RawQuery != "" || value.Fragment != "" || value.Path != "" && value.Path != "/" {
		return nil, failure(ErrInput, "proxy")
	}
	switch value.Scheme {
	case "http", "https", "socks5", "socks5h", "socks4", "socks4a":
	default:
		return nil, failure(ErrUnsupported, "proxy-scheme")
	}
	if value.User != nil {
		password, _ := value.User.Password()
		if !fieldValue(value.User.Username()) || !fieldValue(password) || len(value.User.Username()) > 255 || len(password) > 255 {
			return nil, failure(ErrInput, "proxy-user")
		}
		if (value.Scheme == "socks4" || value.Scheme == "socks4a") && password != "" {
			return nil, failure(ErrUnsupported, "socks4-password")
		}
	}
	if value.Port() != "" {
		if _, err := net.LookupPort("tcp", value.Port()); err != nil {
			return nil, failure(ErrInput, "proxy-port")
		}
	}
	return value, nil
}
func (client *Client) route(options []RequestOptionsV1) (routeChoice, error) {
	result := routeChoice{proxy: client.owner.settings.ProxyURL}
	if len(options) > 1 {
		return result, failure(ErrInput, "runtime-options")
	}
	if len(options) == 1 {
		input := options[0]
		if input.Version != 0 && input.Version != 1 {
			return result, failure(ErrInput, "runtime-version")
		}
		if input.ProxyURL != nil {
			if client.owner.settings.RoutingLocked && *input.ProxyURL != result.proxy {
				return result, failure(ErrInput, "routing-locked")
			}
			result.proxy = *input.ProxyURL
		}
		if !headerFits(input.ConnectHeaders, client.owner.settings.MaxHeaderBytes, false) {
			return result, failure(ErrInput, "connect-headers")
		}
		result.headers = make(http.Header, len(input.ConnectHeaders))
		for key, values := range input.ConnectHeaders {
			canonical := http.CanonicalHeaderKey(key)
			if _, duplicate := result.headers[canonical]; duplicate {
				return result, failure(ErrInput, "ambiguous-connect-header")
			}
			result.headers[canonical] = append([]string(nil), values...)
		}
	}
	if _, err := parseProxy(result.proxy); err != nil {
		return result, err
	}
	if len(result.headers) > 0 && result.proxy == "" {
		return result, failure(ErrInput, "direct-connect-headers")
	}
	return result, nil
}
func fieldValue(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if char == '\r' || char == '\n' || char == 0 || char < 32 && char != '\t' || char == 127 {
			return false
		}
	}
	return true
}
func token(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char > 127 || !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("!#$%&'*+-.^_\x60|~", char)) {
			return false
		}
	}
	return true
}
func headerFits(headers http.Header, maximum int64, ordered bool) bool {
	var size int64
	for key, values := range headers {
		if !(ordered && (key == http.HeaderOrderKey || key == http.PHeaderOrderKey)) && !token(key) {
			return false
		}
		size += int64(len(key) + 32)
		for _, value := range values {
			if !fieldValue(value) {
				return false
			}
			size += int64(len(value) + 16)
			if size > maximum {
				return false
			}
		}
		if size > maximum {
			return false
		}
	}
	return true
}
func validateRequest(ctx context.Context, request *http.Request, value settings) error {
	if request == nil || request.URL == nil || request.RequestURI != "" || request.URL.User != nil ||
		request.URL.Opaque != "" || request.URL.Hostname() == "" || request.URL.Fragment != "" ||
		request.URL.Scheme != "http" && request.URL.Scheme != "https" || !token(request.Method) ||
		len(request.URL.String()) > 8192 || !fieldValue(request.URL.String()) ||
		len(request.Host) > 8192 || !fieldValue(request.Host) || request.ContentLength < -1 ||
		request.ContentLength > value.MaxRequestBytes || !headerFits(request.Header, value.MaxHeaderBytes, true) ||
		!headerFits(request.Trailer, value.MaxHeaderBytes, false) || nilLike(request.Body) {
		return failure(ErrInput, "request")
	}
	if httptrace.ContextClientTrace(ctx) != nil || httptrace.ContextClientTrace(request.Context()) != nil {
		return failure(ErrUnsupported, "unmanaged-trace")
	}
	if request.Cancel != nil || request.Response != nil || len(request.TransferEncoding) > 1 {
		return failure(ErrUnsupported, "request-handles")
	}
	return nil
}
func nilLike(value any) bool {
	if value == nil {
		return false
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		return ref.IsNil()
	}
	return false
}
func copyRequest(request *http.Request, ctx context.Context) *http.Request {
	copy := request.Clone(ctx)
	copy.GetBody = request.GetBody
	copy.Body = request.Body
	copy.Response = nil
	return copy
}
