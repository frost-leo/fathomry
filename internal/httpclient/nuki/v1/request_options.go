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
	"net"
	"net/url"
	"strconv"
	"strings"

	http "github.com/nukilabs/http"
	trace "github.com/nukilabs/http/httptrace"
	sdk "github.com/nukilabs/tlsclient"
)

// ProxyMode separates per-call routing from the frozen Provider identity.
type ProxyMode string

const (
	ProxyFromProvider ProxyMode = "provider"
	ProxyDirect       ProxyMode = "direct"
	ProxyAddress      ProxyMode = "proxy"
)

// RequestOptionsV1 is runtime contract 1; Version zero selects 1. Headers apply
// only to native CONNECT. An explicit direct/proxy choice never mutates a shared
// SDK client. ForceHTTP3 cannot override a configuration-disabled protocol.
type RequestOptionsV1 struct {
	private
	Version        uint32
	ProxyMode      ProxyMode
	ProxyURL       string
	ConnectHeaders http.Header
	ForceHTTP3     bool
}
type routeChoice struct {
	address    string
	headers    http.Header
	mode       ProxyMode
	forceHTTP3 bool
}

func (client *Client) prepare(ctx context.Context, request *http.Request, options RequestOptionsV1) (*http.Request, routeChoice, error) {
	if client == nil || client.owner == nil || ctx == nil || request == nil {
		return nil, routeChoice{}, failure(ErrInput, "request")
	}
	if err := ctx.Err(); err != nil {
		return nil, routeChoice{}, failure(ErrState, "request", err, context.Cause(ctx))
	}
	if err := client.owner.available(); err != nil {
		return nil, routeChoice{}, err
	}
	if trace.ContextClientTrace(ctx) != nil || request.Cancel != nil {
		return nil, routeChoice{}, failure(ErrUnsupported, "owning-request-extension")
	}
	if options.Version != 0 && options.Version != 1 {
		return nil, routeChoice{}, failure(ErrInput, "request-version")
	}
	value := client.owner.settings
	choice := routeChoice{address: value.ProxyURL, mode: options.ProxyMode, forceHTTP3: options.ForceHTTP3 || sdk.HTTP3Forced(ctx)}
	if choice.mode == "" {
		choice.mode = ProxyFromProvider
	}
	switch choice.mode {
	case ProxyFromProvider:
		if options.ProxyURL != "" {
			return nil, choice, failure(ErrInput, "proxy-choice")
		}
	case ProxyDirect:
		if options.ProxyURL != "" {
			return nil, choice, failure(ErrInput, "proxy-choice")
		}
		choice.address = ""
	case ProxyAddress:
		if options.ProxyURL == "" {
			return nil, choice, failure(ErrInput, "proxy-choice")
		}
		choice.address = options.ProxyURL
	default:
		return nil, choice, failure(ErrInput, "proxy-choice")
	}
	if value.RoutingLocked && (choice.mode != ProxyFromProvider || len(options.ConnectHeaders) != 0) {
		return nil, choice, failure(ErrUnsupported, "routing-locked")
	}
	if _, err := parseProxy(choice.address); err != nil {
		return nil, choice, err
	}
	if err := validateHeaders(options.ConnectHeaders, value.MaxHeaderBytes); err != nil {
		return nil, choice, err
	}
	choice.headers = make(http.Header, len(options.ConnectHeaders))
	for key, values := range options.ConnectHeaders {
		canonical := http.CanonicalHeaderKey(key)
		if _, exists := choice.headers[canonical]; exists {
			return nil, choice, failure(ErrInput, "connect-header-case")
		}
		choice.headers[canonical] = append([]string(nil), values...)
	}
	copied, err := SnapshotRequestV1(ctx, request, value.requestPolicy())
	if err != nil {
		return nil, choice, err
	}
	if err := canonicalizeURL(copied.URL); err != nil {
		return nil, choice, err
	}
	if err := client.owner.validateRouteRequest(copied, choice); err != nil {
		return nil, choice, err
	}
	return copied, choice, nil
}

func (owner *owner) validateRouteRequest(request *http.Request, choice routeChoice) error {
	force := choice.forceHTTP3 || owner.settings.Mode == HTTP3Only || owner.native.Transport.ForceHTTP3
	disabled := owner.settings.Mode == HTTP1Only || owner.settings.Mode == HTTP2Negotiated || owner.native.Transport.DisableHTTP3
	if force && (disabled || owner.native.Profile.H3 == nil || request.URL.Scheme != "https") {
		return failure(ErrUnsupported, "http3-request")
	}
	proxy, err := parseProxy(choice.address)
	if err != nil {
		return err
	}
	if force && proxy != nil && (proxy.Scheme == "http" || proxy.Scheme == "https") && !strings.Contains(proxy.Path, "{") && !strings.Contains(proxy.RawQuery, "{") && !strings.Contains(strings.ToLower(proxy.RawQuery), "%7b") {
		return failure(ErrUnsupported, "http3-route")
	}
	if !disabled && owner.native.Profile.H3 != nil && proxy != nil && proxy.Scheme == "socks5h" && net.ParseIP(request.URL.Hostname()) == nil {
		return failure(ErrUnsupported, "http3-remote-dns")
	}
	return nil
}

func validateRequest(request *http.Request, value settings) error {
	if request != nil && (request.MultipartForm != nil || request.Form != nil || request.PostForm != nil || request.Response != nil || request.TLS != nil || request.Cancel != nil || request.RequestURI != "" || request.RemoteAddr != "" || request.Pattern != "") {
		return failure(ErrUnsupported, "server-request-state")
	}
	if request == nil || request.URL == nil || !boundedURL(request.URL) || request.URL.Hostname() == "" ||
		request.URL.Scheme != "http" && request.URL.Scheme != "https" ||
		int64(len(request.Method)) > value.MaxHeaderBytes || request.Method == "CONNECT" || request.Method != "" && !fieldToken(request.Method) ||
		request.ContentLength < -1 || request.ContentLength > value.MaxRequestBytes ||
		request.Body == nil && request.ContentLength > 0 || len(request.TransferEncoding) > 1 ||
		len(request.TransferEncoding) == 1 && request.TransferEncoding[0] != "chunked" ||
		len(request.Host) > 8192 || !fieldValue(request.Host) {
		return failure(ErrInput, "request")
	}
	if port := request.URL.Port(); port != "" {
		parsed, err := strconv.ParseUint(port, 10, 16)
		if err != nil || parsed == 0 {
			return failure(ErrInput, "port", err)
		}
	}
	if int64(len(request.Method))+int64(len(request.Proto))+int64(len(request.Host)) > value.MaxHeaderBytes || len(request.ExcludedCookies) > 4096 {
		return failure(ErrLimit, "request-metadata")
	}
	remaining := value.MaxHeaderBytes
	for name := range request.ExcludedCookies {
		if int64(len(name))+64 > remaining {
			return failure(ErrLimit, "excluded-cookies")
		}
		remaining -= int64(len(name)) + 64
	}
	if err := validateHeaders(request.Header, value.MaxHeaderBytes); err != nil {
		return err
	}
	if err := validateHeaders(request.Trailer, value.MaxHeaderBytes); err != nil {
		return err
	}
	for key, values := range request.Header {
		if strings.EqualFold(key, "Content-Length") {
			if len(values) != 1 {
				return failure(ErrInput, "content-length")
			}
			length, err := strconv.ParseInt(values[0], 10, 64)
			if err != nil || length != request.ContentLength {
				return failure(ErrInput, "content-length", err)
			}
		}
		if strings.EqualFold(key, "Transfer-Encoding") || strings.EqualFold(key, "Upgrade") {
			return failure(ErrUnsupported, "request-upgrade")
		}
	}
	return nil
}

func validateHeaders(header http.Header, limit int64) error {
	remaining := limit
	for key, values := range header {
		if int64(len(key))+64 > remaining {
			return failure(ErrLimit, "headers")
		}
		remaining -= int64(len(key)) + 64
		if int64(len(values)) > remaining/32 {
			return failure(ErrLimit, "headers")
		}
		if key != http.HeaderOrderKey && !fieldToken(key) {
			return failure(ErrInput, "header-name")
		}
		for _, value := range values {
			if int64(len(value))+32 > remaining {
				return failure(ErrLimit, "headers")
			}
			if !fieldValue(value) {
				return failure(ErrInput, "header-value")
			}
			remaining -= int64(len(value)) + 32
		}
	}
	return nil
}
func fieldToken(value string) bool {
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
		if char < 32 && char != '\t' || char == 127 {
			return false
		}
	}
	return true
}

func boundedURL(value *url.URL) bool {
	remaining := 8192
	for _, field := range []string{value.Scheme, value.Opaque, value.Host, value.Path, value.RawPath, value.RawQuery, value.Fragment, value.RawFragment} {
		if len(field) > remaining {
			return false
		}
		remaining -= len(field)
	}
	if value.User != nil {
		password, _ := value.User.Password()
		if len(value.User.Username()) > remaining {
			return false
		}
		remaining -= len(value.User.Username())
		if len(password) > remaining {
			return false
		}
	}
	encoded := value.String()
	return len(encoded) <= 8192 && fieldValue(encoded)
}
func canonicalizeURL(value *url.URL) error {
	host, err := canonicalHost(value.Hostname())
	if err != nil {
		return err
	}
	if port := value.Port(); port != "" {
		if number, err := strconv.ParseUint(port, 10, 16); err == nil {
			port = strconv.FormatUint(number, 10)
		}
		value.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		value.Host = "[" + host + "]"
	} else {
		value.Host = host
	}
	return nil
}
func metadataRequest(request *http.Request) *http.Request {
	if request == nil {
		return nil
	}
	result := snapshotRequest(request, request.Context())
	result.Body, result.GetBody, result.Response, result.TLS, result.Cancel = nil, nil, nil, nil, nil
	return result
}
