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
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
)

// ProxySelection separates configured routing from explicit direct/proxy input.
type ProxySelection uint8

const (
	ProxyFromProvider ProxySelection = iota
	ProxyDirect
	ProxyAddress
)

// RequestOptionsV1 is process-local version 1 access input. At most one value is
// accepted. ProxyAddress requires ProxyURL; the other modes require it empty.
// ConnectHeaders overlays configured native CONNECT headers. The effective values
// are copied and partition native pools, including authentication. An existing
// sdk.ContextKeyHeader input is also accepted, but cannot be combined with this map.
// RoutingLocked refuses any explicit proxy or CONNECT-header route modification.
type RequestOptionsV1 struct {
	private
	Proxy          ProxySelection
	ProxyURL       string
	ConnectHeaders http.Header
}
type routeChoice struct {
	selection ProxySelection
	proxy     string
	headers   http.Header
	digest    [32]byte
}

func (choice routeChoice) mode() string {
	switch choice.selection {
	case ProxyFromProvider:
		return "provider"
	case ProxyDirect:
		return "direct"
	case ProxyAddress:
		return "proxy"
	}
	return ""
}
func parseProxy(value string, mode ProtocolMode) (*url.URL, error) {
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
	if address.Hostname() == "" || strings.ContainsAny(address.Hostname(), "\t\r\n /\\") || address.Opaque != "" || address.RawQuery != "" || address.ForceQuery || address.Fragment != "" || address.Path != "" && address.Path != "/" {
		return nil, failure(ErrInput, "proxy")
	}
	switch address.Scheme {
	case "http", "https", "socks4", "socks4a", "socks5", "socks5h":
	default:
		return nil, failure(ErrUnsupported, "proxy-scheme")
	}
	if (address.Scheme == "socks4" || address.Scheme == "socks4a") && address.User != nil {
		return nil, failure(ErrUnsupported, "socks4-credentials")
	}
	if mode == HTTP3Racing && address.Scheme != "socks5" && address.Scheme != "socks5h" {
		return nil, failure(ErrUnsupported, "proxy-http3")
	}
	port := address.Port()
	if port == "" {
		switch address.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return nil, failure(ErrInput, "proxy-port")
		}
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return nil, failure(ErrInput, "proxy-port", err)
	}
	address.Host = net.JoinHostPort(address.Hostname(), strconv.Itoa(number))
	address.Path, address.RawPath = "", ""
	return address, nil
}
func (client *Client) route(ctx context.Context, input []RequestOptionsV1) (routeChoice, error) {
	if len(input) > 1 {
		return routeChoice{}, failure(ErrInput, "request-options")
	}
	option := RequestOptionsV1{}
	if len(input) == 1 {
		option = input[0]
	}
	if option.Proxy > ProxyAddress || option.Proxy != ProxyAddress && option.ProxyURL != "" || option.Proxy == ProxyAddress && option.ProxyURL == "" {
		return routeChoice{}, failure(ErrInput, "proxy-selection")
	}
	extra := option.ConnectHeaders
	if value := ctx.Value(sdk.ContextKeyHeader{}); value != nil {
		if extra != nil {
			return routeChoice{}, failure(ErrInput, "connect-headers")
		}
		var ok bool
		extra, ok = value.(http.Header)
		if !ok {
			return routeChoice{}, failure(ErrInput, "connect-headers")
		}
	}
	if client.owner.settings.RoutingLocked && (option.Proxy != ProxyFromProvider || extra != nil) {
		return routeChoice{}, failure(ErrInput, "routing-locked")
	}
	choice := routeChoice{selection: option.Proxy, proxy: client.owner.settings.ProxyURL, headers: client.owner.native.ConnectHeaders.Clone()}
	if choice.headers == nil {
		choice.headers = make(http.Header)
	}
	if !connectHeadersFit(extra, client.owner.settings.MaxHeaderBytes) {
		return routeChoice{}, failure(ErrLimit, "connect-headers")
	}
	switch option.Proxy {
	case ProxyDirect:
		choice.proxy = ""
	case ProxyAddress:
		choice.proxy = option.ProxyURL
	}
	address, err := parseProxy(choice.proxy, client.owner.settings.Mode)
	if err != nil {
		return routeChoice{}, err
	}
	if address != nil && (address.Scheme == "http" || address.Scheme == "https") && address.User != nil && address.User.Username() != "" {
		password, _ := address.User.Password()
		setConnectHeader(choice.headers, "Proxy-Authorization", []string{"Basic " + base64.StdEncoding.EncodeToString([]byte(address.User.Username()+":"+password))})
	}
	for key, values := range extra {
		setConnectHeader(choice.headers, key, values)
	}
	if address != nil {
		choice.proxy = address.String()
	}
	if !headerFits(choice.headers, client.owner.settings.MaxHeaderBytes, true) {
		return routeChoice{}, failure(ErrLimit, "connect-headers")
	}
	native := client.owner.native
	if choice.proxy != "" && native.DialContext != nil {
		return routeChoice{}, failure(ErrInput, "native-proxy-conflict")
	}
	choice.digest = connectHeaderDigest(choice.headers)
	return choice, nil
}

func connectHeadersFit(header http.Header, maximum int64) bool {
	if !headerFits(header, maximum, true) {
		return false
	}
	seen := make(map[string]bool, len(header))
	for key := range header {
		lower := strings.ToLower(key)
		if seen[lower] {
			return false
		}
		seen[lower] = true
	}
	return true
}

func connectHeaderDigest(header http.Header) [32]byte {
	digest := sha256.New()
	var length [8]byte
	writeLength := func(value uint64) {
		binary.BigEndian.PutUint64(length[:], value)
		_, _ = digest.Write(length[:])
	}
	writeText := func(value string) {
		writeLength(uint64(len(value)))
		_, _ = digest.Write([]byte(value))
	}
	for _, key := range slices.Sorted(maps.Keys(header)) {
		writeText(key)
		writeLength(uint64(len(header[key])))
		for _, value := range header[key] {
			writeText(value)
		}
	}
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func setConnectHeader(header http.Header, key string, values []string) {
	for prior := range header {
		if strings.EqualFold(prior, key) {
			delete(header, prior)
		}
	}
	header[key] = append([]string(nil), values...)
}

func (client *Client) validateRoute(request *http.Request, choice routeChoice) error {
	if client.owner.settings.Mode == HTTP3Racing && request.URL.Scheme == "https" && strings.HasPrefix(choice.proxy, "socks5h:") && net.ParseIP(request.URL.Hostname()) == nil {
		return failure(ErrUnsupported, "http3-remote-dns")
	}
	return nil
}
