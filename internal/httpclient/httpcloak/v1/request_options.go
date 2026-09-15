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
	http "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
)

type ProxySelection uint8

const (
	ProxyFromProvider ProxySelection = iota
	ProxyDirect
	ProxyAddress
)

// RequestOptionsV1 is copied before admission. Routing never mutates the source.
// Explicit CONNECT headers are proxy-only. ExactHeaders keeps native ordering,
// casing and duplicates; it replaces the normal request-header pipeline.
// FollowRedirects is opt-in and still bounded by the instance MaxExchanges.
type RequestOptionsV1 struct {
	private
	Proxy              ProxySelection
	ProxyURL           string
	ConnectHeaders     http.Header
	HeaderOrder        []string
	ExactHeaders       []fingerprint.HeaderPair
	TLSOnly            *bool
	DisableClientHints bool
	FollowRedirects    bool
}
