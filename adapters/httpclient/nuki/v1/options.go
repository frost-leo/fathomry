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
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/nuki/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	http "github.com/nukilabs/http"
	quic "github.com/nukilabs/quic-go"
	sdk "github.com/nukilabs/tlsclient"
	"github.com/nukilabs/tlsclient/bandwidth"
	"github.com/nukilabs/tlsclient/profiles"
	tls "github.com/nukilabs/utls"
)

const ProviderID = native.ProviderID

type ProtocolMode string

const (
	Native          ProtocolMode = "native"
	HTTP1Only       ProtocolMode = "http1"
	HTTP2Negotiated ProtocolMode = "http1-http2"
	HTTP3Only       ProtocolMode = "http3"
)

// Settings is strict-loadable format 1 data. Durations are integer nanoseconds.
// Nil selects Internal defaults; explicit zero and false remain explicit.
// Proxy credentials may appear in deliberate JSON, never ordinary diagnostics.
type Settings struct {
	Name                 string         `json:"name" mapstructure:"name"`
	Version              uint32         `json:"version" mapstructure:"version"`
	Mode                 *ProtocolMode  `json:"mode" mapstructure:"mode"`
	ProxyURL             string         `json:"proxy_url" mapstructure:"proxy_url"`
	RoutingLocked        *bool          `json:"routing_locked" mapstructure:"routing_locked"`
	FollowRedirects      *bool          `json:"follow_redirects" mapstructure:"follow_redirects"`
	DisableDecompression *bool          `json:"disable_decompression" mapstructure:"disable_decompression"`
	MaxActive            *int           `json:"max_active" mapstructure:"max_active"`
	QueuedCalls          *int           `json:"queued_calls" mapstructure:"queued_calls"`
	MaxRoutes            *int           `json:"max_routes" mapstructure:"max_routes"`
	MaxConnections       *int           `json:"max_connections" mapstructure:"max_connections"`
	MaxOrigins           *int           `json:"max_origins" mapstructure:"max_origins"`
	MaxProfileBytes      *int64         `json:"max_profile_bytes" mapstructure:"max_profile_bytes"`
	MaxProxyTunnels      *int           `json:"max_proxy_tunnels" mapstructure:"max_proxy_tunnels"`
	MaxRequestBytes      *int64         `json:"max_request_bytes" mapstructure:"max_request_bytes"`
	MaxResponseBytes     *int64         `json:"max_response_bytes" mapstructure:"max_response_bytes"`
	MaxEncodedBytes      *int64         `json:"max_encoded_bytes" mapstructure:"max_encoded_bytes"`
	MaxHeaderBytes       *int64         `json:"max_header_bytes" mapstructure:"max_header_bytes"`
	MaxNativeHeaderBytes *int64         `json:"max_native_header_bytes" mapstructure:"max_native_header_bytes"`
	MaxExchanges         *int           `json:"max_exchanges" mapstructure:"max_exchanges"`
	MaxReplays           *int           `json:"max_replays" mapstructure:"max_replays"`
	AdmissionTimeout     *time.Duration `json:"admission_timeout_ns" mapstructure:"admission_timeout_ns"`
	Timeout              *time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	IdleConnTimeout      *time.Duration `json:"idle_conn_timeout_ns" mapstructure:"idle_conn_timeout_ns"`
}

// NativeOptions explicitly selects nukilabs capabilities, not bogdanfinn types.
// Profile is required. Containers freeze at Prepare; callbacks, keys, caches,
// immutable trust pools, Jar, Tracker and resolver/dial functions remain cooperative
// concurrent borrows through source shutdown. Clone trust pools before caller
// mutation and prepare a new source to adopt changed trust. ProxyTLS owns proxy
// identity separately from TLS.
// Dial/Listen callbacks transfer one physical socket; no socket escapes publicly.
type NativeOptions struct {
	private
	Profile                       *profiles.ClientProfile
	TLS                           *tls.Config
	ProxyTLS                      *tls.Config
	QUIC                          *quic.Config
	Transport                     *sdk.TransportOptions
	Pinner                        *sdk.Pinner
	Jar                           http.CookieJar
	Tracker                       bandwidth.Tracker
	DialContext                   func(context.Context, string, string) (net.Conn, error)
	ListenPacket                  func(context.Context, string, string) (net.PacketConn, error)
	Resolver                      *net.Resolver
	CheckRedirect                 func(*http.Request, []*http.Request) error
	AllowConnectionWindowIncrease func(context.Context, uint64) bool
	Before                        []func(context.Context, *http.Request) error
	After                         []func(context.Context, Metadata) error
}

// Dependencies borrows public mechanisms and explicit native selection. Native
// must be empty when opening a Prepared value or binding Using.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}

func nativeOptions(value NativeOptions) (native.NativeOptionsV1, error) {
	if len(value.Before) > native.MaxNativeHooks || len(value.After) > native.MaxNativeHooks {
		return native.NativeOptionsV1{}, fail(ErrLimit, "native-hooks")
	}
	result := native.NativeOptionsV1{
		Profile: value.Profile, TLS: value.TLS, ProxyTLS: value.ProxyTLS, QUIC: value.QUIC,
		Transport: value.Transport, Pinner: value.Pinner, Jar: value.Jar, Tracker: value.Tracker,
		DialContext: value.DialContext, ListenPacket: value.ListenPacket, Resolver: value.Resolver,
		CheckRedirect: value.CheckRedirect, AllowConnectionWindowIncrease: value.AllowConnectionWindowIncrease,
		Before: value.Before,
	}
	if value.After != nil {
		result.After = make([]func(context.Context, native.Metadata) error, len(value.After))
		for index, hook := range value.After {
			if hook == nil {
				return native.NativeOptionsV1{}, fail(ErrInput, "native-hooks")
			}
			result.After[index] = func(ctx context.Context, value native.Metadata) error {
				return hook(ctx, Metadata{native: value})
			}
		}
	}
	return result, nil
}

// Validate checks only strict data/default semantics, not native profile choice
// or remote readiness. Prepare validates the actual data/native combination.
func Validate(value Settings) error {
	data, err := settingsData(value)
	if err != nil {
		return err
	}
	return translate(native.ValidateDataV1(native.OptionsV1{Name: value.Name, Version: value.Version}, source.Layer{Kind: source.Local, Content: data}), "validate")
}
