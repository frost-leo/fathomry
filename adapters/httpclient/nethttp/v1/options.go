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
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/nethttp/v1"
)

const ProviderID = native.ProviderID

// Settings is strict-loadable configuration version 1, independent of SDK/API
// versions. Nil pointers select Internal defaults; explicit zero/false survive
// preparation. Durations are integer nanoseconds. Positive limits reject zero;
// queue zero disables queueing and continue-wait zero sends the body immediately.
// Explicit JSON serialization may disclose credentials; fmt/slog are redacted.
type Settings struct {
	Name                  string              `json:"name" mapstructure:"name"`
	Version               uint32              `json:"version" mapstructure:"version"`
	HTTP1                 *bool               `json:"http1" mapstructure:"http1"`
	HTTP2                 *bool               `json:"http2" mapstructure:"http2"`
	UnencryptedHTTP2      *bool               `json:"unencrypted_http2" mapstructure:"unencrypted_http2"`
	ProxyURL              string              `json:"proxy_url" mapstructure:"proxy_url"`
	RoutingLocked         *bool               `json:"routing_locked" mapstructure:"routing_locked"`
	ProxyConnectHeader    map[string][]string `json:"proxy_connect_header" mapstructure:"proxy_connect_header"`
	RootCAPEM             string              `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	ServerName            string              `json:"server_name" mapstructure:"server_name"`
	ClientCertPEM         string              `json:"client_cert_pem" mapstructure:"client_cert_pem"`
	ClientKeyPEM          string              `json:"client_key_pem" mapstructure:"client_key_pem"`
	MaxActive             *int                `json:"max_active" mapstructure:"max_active"`
	QueuedCalls           *int                `json:"queued_calls" mapstructure:"queued_calls"`
	MaxConnections        *int                `json:"max_connections" mapstructure:"max_connections"`
	MaxRequestBytes       *int64              `json:"max_request_bytes" mapstructure:"max_request_bytes"`
	MaxResponseBytes      *int64              `json:"max_response_bytes" mapstructure:"max_response_bytes"`
	MaxHeaderBytes        *int64              `json:"max_header_bytes" mapstructure:"max_header_bytes"`
	MaxExchanges          *int                `json:"max_exchanges" mapstructure:"max_exchanges"`
	AdmissionTimeout      *time.Duration      `json:"admission_timeout_ns" mapstructure:"admission_timeout_ns"`
	Timeout               *time.Duration      `json:"timeout_ns" mapstructure:"timeout_ns"`
	DialTimeout           *time.Duration      `json:"dial_timeout_ns" mapstructure:"dial_timeout_ns"`
	TLSHandshakeTimeout   *time.Duration      `json:"tls_handshake_timeout_ns" mapstructure:"tls_handshake_timeout_ns"`
	ResponseHeaderTimeout *time.Duration      `json:"response_header_timeout_ns" mapstructure:"response_header_timeout_ns"`
	ExpectContinueTimeout *time.Duration      `json:"expect_continue_timeout_ns" mapstructure:"expect_continue_timeout_ns"`
	IdleConnTimeout       *time.Duration      `json:"idle_conn_timeout_ns" mapstructure:"idle_conn_timeout_ns"`
	DisableCompression    *bool               `json:"disable_compression" mapstructure:"disable_compression"`
	DisableKeepAlives     *bool               `json:"disable_keep_alives" mapstructure:"disable_keep_alives"`
}

// NativeOptions borrows cooperative, concurrent runtime dependencies until source
// shutdown. TLS/HTTP2 containers are copied; keys, caches, callbacks and Jar remain
// borrowed. No arbitrary RoundTripper, httptrace, raw ClientConn or owning response
// is admitted. Hook arguments must not be retained or used for unaccounted work.
type NativeOptions struct {
	private
	TLS           *tls.Config
	HTTP2         *http.HTTP2Config
	DialContext   func(context.Context, string, string) (net.Conn, error)
	Proxy         func(*http.Request) (*url.URL, error)
	CheckRedirect func(*http.Request, []*http.Request) error
	Jar           http.CookieJar
	// Runs before every applicable exchange's pool selection, not just new dials.
	GetProxyConnectHeader  func(context.Context, *url.URL, string) (http.Header, error)
	OnProxyConnectResponse func(context.Context, ConnectResponse) error
}

// Dependencies separates caller-owned public mechanisms from native authority.
// Native is used by Open only; Prepared.Open uses its already frozen selection.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}

func nativeOptions(value NativeOptions) native.NativeOptionsV1 {
	result := native.NativeOptionsV1{TLS: value.TLS, HTTP2: value.HTTP2, DialContext: value.DialContext,
		Proxy: value.Proxy, CheckRedirect: value.CheckRedirect, Jar: value.Jar, GetProxyConnectHeader: value.GetProxyConnectHeader}
	if value.OnProxyConnectResponse != nil {
		result.OnProxyConnectResponse = func(ctx context.Context, response native.ConnectResponse) error {
			return value.OnProxyConnectResponse(ctx, ConnectResponse{native: response})
		}
	}
	return result
}

// Validate is offline data-only validation. Prepare additionally validates the
// exact borrowed native dependencies; neither invokes them or constructs a source.
func Validate(value Settings) error { _, err := Prepare(value, NativeOptions{}); return err }
