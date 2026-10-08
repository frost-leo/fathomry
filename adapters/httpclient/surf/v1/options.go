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
	"crypto/tls"
	"net"
	"time"

	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/surf/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	utls "github.com/refraction-networking/utls"
)

const ProviderID = native.ProviderID

type ProtocolMode string

const (
	Negotiated  ProtocolMode = "negotiated"
	HTTP1Only   ProtocolMode = "http1"
	HTTP2Only   ProtocolMode = "http2"
	PreferHTTP3 ProtocolMode = "prefer-http3"
	H2C         ProtocolMode = "h2c"
)

// Settings is strict-loadable format1 data. Durations are integer nanoseconds.
// Nil selects Internal defaults; explicit zero/false/empty mode survive validation.
// NativeRetries configures only the selected SDK's retry mechanism. Explicit
// serialization can disclose proxy credentials; ordinary diagnostics are redacted.
type Settings struct {
	Name                 string         `json:"name" mapstructure:"name"`
	Version              uint32         `json:"version" mapstructure:"version"`
	Mode                 *ProtocolMode  `json:"mode" mapstructure:"mode"`
	ProxyURL             string         `json:"proxy_url" mapstructure:"proxy_url"`
	RoutingLocked        *bool          `json:"routing_locked" mapstructure:"routing_locked"`
	DisableCompression   *bool          `json:"disable_compression" mapstructure:"disable_compression"`
	MaxActive            *int           `json:"max_active" mapstructure:"max_active"`
	QueuedCalls          *int           `json:"queued_calls" mapstructure:"queued_calls"`
	MaxRoutes            *int           `json:"max_routes" mapstructure:"max_routes"`
	MaxTCPConnections    *int           `json:"max_tcp_connections" mapstructure:"max_tcp_connections"`
	MaxUDPSockets        *int           `json:"max_udp_sockets" mapstructure:"max_udp_sockets"`
	MaxHTTP3Clients      *int           `json:"max_http3_clients" mapstructure:"max_http3_clients"`
	MaxProfileBytes      *int64         `json:"max_profile_bytes" mapstructure:"max_profile_bytes"`
	MaxHTTP2StreamBytes  *int64         `json:"max_http2_stream_bytes" mapstructure:"max_http2_stream_bytes"`
	MaxRequestBytes      *int64         `json:"max_request_bytes" mapstructure:"max_request_bytes"`
	MaxResponseBytes     *int64         `json:"max_response_bytes" mapstructure:"max_response_bytes"`
	MaxHeaderBytes       *int64         `json:"max_header_bytes" mapstructure:"max_header_bytes"`
	MaxNativeHeaderBytes *int64         `json:"max_native_header_bytes" mapstructure:"max_native_header_bytes"`
	MaxRoundTrips        *int           `json:"max_round_trips" mapstructure:"max_round_trips"`
	MaxReplays           *int           `json:"max_replays" mapstructure:"max_replays"`
	NativeRetries        *int           `json:"native_retries" mapstructure:"native_retries"`
	RetryCodes           []int          `json:"retry_codes" mapstructure:"retry_codes"`
	RetryDelay           *time.Duration `json:"retry_delay_ns" mapstructure:"retry_delay_ns"`
	AdmissionTimeout     *time.Duration `json:"admission_timeout_ns" mapstructure:"admission_timeout_ns"`
	Timeout              *time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	IdleConnTimeout      *time.Duration `json:"idle_conn_timeout_ns" mapstructure:"idle_conn_timeout_ns"`
}

// NativeOptions selects the SDK's client capabilities. Profile omission uses
// native standard TLS, not an automatic browser. Client-used data containers are
// copied; factories, keys, caches, writers, Jar and callbacks remain cooperative
// concurrent borrows through source release. Middleware has header/metadata-only
// authority. Context/owning body/transport mutation is not admitted.
type NativeOptions struct {
	private
	Profile            *profiles.Variant
	OS                 profiles.OSKey
	HelloSpecFactory   func(context.Context) (utls.ClientHelloSpec, error)
	TLSConfig          *tls.Config
	JAConfig           *utls.Config
	ProxyTLSConfig     *tls.Config
	Headers            http.Header
	Jar                http.CookieJar
	DialContext        func(context.Context, string, string) (net.Conn, error)
	ListenPacket       func(context.Context, string, string) (net.PacketConn, error)
	Resolver           *net.Resolver
	RequestMiddleware  []func(*sdk.Request) error
	ResponseMiddleware []func(*sdk.Response) error
	CheckRedirect      func(*http.Request, []*http.Request) error
}
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}

func nativeOptions(value NativeOptions) native.NativeOptionsV1 {
	return native.NativeOptionsV1{Profile: value.Profile, OS: value.OS, HelloSpecFactory: value.HelloSpecFactory, TLSConfig: value.TLSConfig, JAConfig: value.JAConfig, ProxyTLSConfig: value.ProxyTLSConfig,
		Headers: value.Headers, Jar: value.Jar, DialContext: value.DialContext, ListenPacket: value.ListenPacket, Resolver: value.Resolver, RequestMiddleware: value.RequestMiddleware, ResponseMiddleware: value.ResponseMiddleware, CheckRedirect: value.CheckRedirect}
}

// Validate checks strict data/default semantics, not separately supplied runtime dependencies.
func Validate(value Settings) error {
	data, err := settingsData(value)
	if err != nil {
		return err
	}
	return translate(native.ValidateDataV1(native.OptionsV1{Name: value.Name, Version: value.Version}, source.Layer{Kind: source.Local, Content: data}), "validate")
}
