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

import "net/url"

// ProxySelection identifies the scope of a runtime route choice, not a Provider
// identity or a proxy-rotation policy. The zero value uses configured routing.
type ProxySelection uint8

const (
	ProxyFromProvider ProxySelection = iota
	ProxyDirect
	ProxyAddress
)

// RequestOptionsV1 supplies version 1 of per-call access inputs, not mutable
// instance configuration. At most one value may be supplied. ProxyFromProvider
// uses configured routing; ProxyDirect explicitly selects direct access;
// ProxyAddress requires a nonempty, valid ProxyURL. The other modes require an
// empty ProxyURL. RoutingLocked sources refuse non-default selection.
// A selected proxy is frozen before admission through redirects/native retries.
// A direct Connection instead binds its choice for its entire lifetime.
// The value may contain credentials and is not a diagnostic or durable DTO.
type RequestOptionsV1 struct {
	private
	Proxy    ProxySelection
	ProxyURL string
}

type proxyChoice struct {
	selection ProxySelection
	address   *url.URL
}

type boundProxyKey struct{}

func requestOptions(options []RequestOptionsV1, locked bool) (proxyChoice, error) {
	if len(options) > 1 {
		return proxyChoice{}, failure(ErrInput, "request-options")
	}
	if len(options) == 0 {
		return proxyChoice{}, nil
	}
	option := options[0]
	if option.Proxy > ProxyAddress || locked && option.Proxy != ProxyFromProvider {
		return proxyChoice{}, failure(ErrInput, "proxy-selection")
	}
	if option.Proxy != ProxyAddress {
		if option.ProxyURL != "" {
			return proxyChoice{}, failure(ErrInput, "proxy-selection")
		}
		return proxyChoice{selection: option.Proxy}, nil
	}
	value := option.ProxyURL
	if len(value) > 8192 {
		return proxyChoice{}, failure(ErrLimit, "proxy")
	}
	if value == "" {
		return proxyChoice{}, failure(ErrInput, "proxy")
	}
	address, err := url.Parse(value)
	if err != nil || !validProxy(address) {
		return proxyChoice{}, failure(ErrInput, "proxy", err)
	}
	return proxyChoice{selection: ProxyAddress, address: address}, nil
}
func (choice proxyChoice) mode() string {
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
