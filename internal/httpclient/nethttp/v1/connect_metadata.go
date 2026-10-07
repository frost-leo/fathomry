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

package nethttp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// ConnectResponse is an immutable, bounded CONNECT response observation. URL and
// header inspection may reveal proxy credentials. No native body, request or
// owning connection escapes. Native CONNECT parsing uses MaxHeaderBytes as a
// wire limit; this snapshot separately validates the retained metadata envelope.
type ConnectResponse struct {
	private
	proxy    *url.URL
	target   string
	status   int
	protocol string
	headers  http.Header
}

func (value ConnectResponse) ProxyURLCopy() *url.URL {
	if value.proxy == nil {
		return nil
	}
	copy := *value.proxy
	return &copy
}
func (value ConnectResponse) Target() string           { return value.target }
func (value ConnectResponse) StatusCode() int          { return value.status }
func (value ConnectResponse) Protocol() string         { return value.protocol }
func (value ConnectResponse) HeadersCopy() http.Header { return value.headers.Clone() }

func connectHeadersFit(headers http.Header, maximum int64) bool {
	if !headerFits(headers, maximum) {
		return false
	}
	for name := range headers {
		switch strings.ToLower(name) {
		case "host", "content-length", "transfer-encoding", "trailer", "connection", "upgrade":
			return false
		}
	}
	return true
}

func (own *owner) connectHeaders(request *http.Request, proxy *url.URL) (http.Header, error) {
	if proxy == nil || request.URL.Scheme != "https" || (proxy.Scheme != "http" && proxy.Scheme != "https") {
		return nil, nil
	}
	headers := own.settings.ProxyConnectHeader
	if callback := own.native.GetProxyConnectHeader; callback != nil {
		op, ok := request.Context().Value(operationKey{}).(*operation)
		if !ok || !op.callbacks.enter() {
			return nil, failure(ErrState, "connect-headers")
		}
		defer op.callbacks.leave()
		if !own.callbacks.enter() {
			return nil, failure(ErrState, "connect-headers")
		}
		defer own.callbacks.leave()
		if err := op.ctx.Err(); err != nil {
			return nil, err
		}
		address := *proxy
		var err error
		headers, err = callback(op.ctx, &address, authority(request.URL))
		if err != nil {
			return nil, err
		}
		if err := op.ctx.Err(); err != nil {
			return nil, err
		}
	}
	if !connectHeadersFit(headers, own.settings.MaxHeaderBytes) {
		return nil, failure(ErrLimit, "connect-headers")
	}
	headers = headers.Clone()
	if proxy.User != nil {
		if headers == nil {
			headers = make(http.Header)
		}
		password, _ := proxy.User.Password()
		headers.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(proxy.User.Username()+":"+password)))
	}
	if !connectHeadersFit(headers, own.settings.MaxHeaderBytes) {
		return nil, failure(ErrLimit, "connect-headers")
	}
	return headers, nil
}

func routeKey(proxy *url.URL, headers http.Header) string {
	if proxy == nil {
		return ""
	}
	keys := make([]string, 0, len(headers))
	for name := range headers {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	hash := sha256.New()
	write := func(value string) {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}
	for _, name := range keys {
		write(name)
		var count [8]byte
		binary.BigEndian.PutUint64(count[:], uint64(len(headers[name])))
		_, _ = hash.Write(count[:])
		for _, value := range headers[name] {
			write(value)
		}
	}
	return proxy.String() + "\x00" + hex.EncodeToString(hash.Sum(nil))
}

func (own *owner) connectResponse(ctx context.Context, proxy *url.URL, request *http.Request, response *http.Response) error {
	op, ok := ctx.Value(operationKey{}).(*operation)
	if !ok || !op.callbacks.enter() {
		return failure(ErrState, "connect-response")
	}
	defer op.callbacks.leave()
	if !own.callbacks.enter() {
		return failure(ErrState, "connect-response")
	}
	defer own.callbacks.leave()
	if err := op.ctx.Err(); err != nil {
		return err
	}
	if response == nil || !headerFits(response.Header, own.settings.MaxHeaderBytes) ||
		len(response.Proto) > 64 || len(request.Host) > 8192 {
		return failure(ErrLimit, "connect-response")
	}
	if callback := own.native.OnProxyConnectResponse; callback != nil {
		address := *proxy
		value := ConnectResponse{proxy: &address, target: request.Host, status: response.StatusCode,
			protocol: response.Proto, headers: response.Header.Clone()}
		if err := callback(op.ctx, value); err != nil {
			return err
		}
	}
	if err := op.ctx.Err(); err != nil {
		return err
	}
	if binding, _ := ctx.Value(bindingKey{}).(*transportBinding); binding != nil && response.StatusCode == http.StatusOK {
		if !op.callbacks.enter() {
			return failure(ErrState, "tls-entry")
		}
	}
	return nil
}
