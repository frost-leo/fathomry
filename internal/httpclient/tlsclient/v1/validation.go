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
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/httptrace"
)

func nilLike(value any) bool {
	if value == nil {
		return false
	}
	item := reflect.ValueOf(value)
	switch item.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Func, reflect.Slice, reflect.Chan:
		return item.IsNil()
	}
	return false
}
func fieldValue(value string) bool {
	for index := range len(value) {
		if value[index] < 32 && value[index] != '\t' || value[index] == 127 {
			return false
		}
	}
	return true
}
func token(value string) bool {
	if value == "" {
		return false
	}
	for index := range len(value) {
		ch := value[index]
		if ch <= 32 || ch >= 127 || strings.ContainsRune("()<>@,;:\\\"/[]?={}", rune(ch)) {
			return false
		}
	}
	return true
}
func headerFits(header http.Header, maximum int64, nativeOrder bool) bool {
	var total int64
	for key, values := range header {
		total += int64(len(key)) + 64
		special := nativeOrder && (key == http.HeaderOrderKey || key == http.PHeaderOrderKey)
		if total > maximum || !special && !token(key) {
			return false
		}
		for _, value := range values {
			total += int64(len(value)) + 16
			if total > maximum || !fieldValue(value) {
				return false
			}
		}
	}
	return true
}
func urlFits(address *url.URL, maximum int64) bool {
	var total int64
	for _, part := range []string{address.Scheme, address.Opaque, address.Host, address.Path, address.RawPath, address.RawQuery, address.Fragment, address.RawFragment} {
		total += int64(len(part))
		if total > maximum {
			return false
		}
	}
	if address.User != nil {
		password, _ := address.User.Password()
		total += int64(len(address.User.Username()) + len(password))
		if total > maximum {
			return false
		}
	}
	return int64(len(address.String())) <= maximum
}
func authority(address *url.URL) string {
	port := address.Port()
	if port == "" {
		if address.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(address.Hostname(), port)
}
func validateRequest(ctx context.Context, request *http.Request, value settings) error {
	if ctx == nil || request == nil || request.URL == nil || request.RequestURI != "" || nilLike(request.Body) ||
		request.ContentLength < -1 || request.ContentLength > value.MaxRequestBytes || (request.Body == nil || request.Body == http.NoBody) && request.ContentLength > 0 ||
		len(request.TransferEncoding) > 2 || len(request.Host) > 8192 || !fieldValue(request.Host) {
		return failure(ErrInput, "request")
	}
	if httptrace.ContextClientTrace(ctx) != nil || httptrace.ContextClientTrace(request.Context()) != nil {
		return failure(ErrUnsupported, "native-trace")
	}
	if request.URL.Scheme != "http" && request.URL.Scheme != "https" || request.URL.Hostname() == "" || strings.ContainsAny(request.URL.Hostname(), " \t\r\n/\\") || request.URL.Opaque != "" {
		return failure(ErrUnsupported, "scheme")
	}
	if !urlFits(request.URL, value.MaxHeaderBytes) {
		return failure(ErrLimit, "url")
	}
	host, port, err := net.SplitHostPort(authority(request.URL))
	number, parseErr := strconv.Atoi(port)
	if err != nil || host == "" || parseErr != nil || number < 1 || number > 65535 {
		return failure(ErrInput, "authority", err, parseErr)
	}
	total := int64(len(request.Method) + len(request.Host))
	for _, encoding := range request.TransferEncoding {
		total += int64(len(encoding))
	}
	if total > value.MaxHeaderBytes {
		return failure(ErrLimit, "metadata")
	}
	if request.Method != "" && !token(request.Method) {
		return failure(ErrInput, "method")
	}
	if !headerFits(request.Header, value.MaxHeaderBytes, true) || !headerFits(request.Trailer, value.MaxHeaderBytes, false) {
		return failure(ErrLimit, "headers")
	}
	if request.Cancel != nil {
		select {
		case <-request.Cancel:
			return failure(ErrState, "canceled")
		default:
		}
	}
	return nil
}
func copyRequest(original *http.Request, ctx context.Context) *http.Request {
	request := original.WithContext(ctx)
	address := *original.URL
	request.URL = &address
	request.Header = original.Header.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	request.Trailer = original.Trailer.Clone()
	request.TransferEncoding = append([]string(nil), original.TransferEncoding...)
	request.Form, request.PostForm, request.MultipartForm = nil, nil, nil
	request.Response, request.TLS = nil, nil
	return request
}
func previewRequest(original *http.Request) *http.Request {
	request := copyRequest(original, original.Context())
	request.Body, request.GetBody, request.Cancel = nil, nil, nil
	return request
}
