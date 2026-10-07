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
	"time"

	http "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
)

const ProviderID = native.ProviderID

// ProtocolMode selects native behavior, not a browser-profile allowlist.
type ProtocolMode string

const (
	Negotiated  ProtocolMode = "negotiated"
	HTTP1Only   ProtocolMode = "http1"
	HTTP3Racing ProtocolMode = "race-http3"
)

// Settings is strict-loadable configuration version 1. Durations are integer
// nanoseconds; nil pointers select Internal defaults, explicit zero/false survive.
// Positive bounds reject zero, except queue zero disables queueing. Mode omission
// selects negotiation, never an automatic fingerprint. Explicit serialization can
// disclose proxy credentials; ordinary fmt/slog diagnostics are redacted.
type Settings struct {
	Name                    string         `json:"name" mapstructure:"name"`
	Version                 uint32         `json:"version" mapstructure:"version"`
	Mode                    *ProtocolMode  `json:"mode" mapstructure:"mode"`
	ProxyURL                string         `json:"proxy_url" mapstructure:"proxy_url"`
	RoutingLocked           *bool          `json:"routing_locked" mapstructure:"routing_locked"`
	ServerName              string         `json:"server_name" mapstructure:"server_name"`
	InsecureSkipVerify      *bool          `json:"insecure_skip_verify" mapstructure:"insecure_skip_verify"`
	RandomTLSExtensionOrder *bool          `json:"random_tls_extension_order" mapstructure:"random_tls_extension_order"`
	DisableSessionTickets   *bool          `json:"disable_session_tickets" mapstructure:"disable_session_tickets"`
	DisableIPV4             *bool          `json:"disable_ipv4" mapstructure:"disable_ipv4"`
	DisableIPV6             *bool          `json:"disable_ipv6" mapstructure:"disable_ipv6"`
	Bandwidth               *bool          `json:"bandwidth" mapstructure:"bandwidth"`
	MaxActive               *int           `json:"max_active" mapstructure:"max_active"`
	QueuedCalls             *int           `json:"queued_calls" mapstructure:"queued_calls"`
	MaxBindings             *int           `json:"max_bindings" mapstructure:"max_bindings"`
	MaxTCPConnections       *int           `json:"max_tcp_connections" mapstructure:"max_tcp_connections"`
	MaxHTTP3Transports      *int           `json:"max_http3_transports" mapstructure:"max_http3_transports"`
	MaxRequestBytes         *int64         `json:"max_request_bytes" mapstructure:"max_request_bytes"`
	MaxResponseBytes        *int64         `json:"max_response_bytes" mapstructure:"max_response_bytes"`
	MaxHeaderBytes          *int64         `json:"max_header_bytes" mapstructure:"max_header_bytes"`
	MaxNativeHeaderBytes    *int64         `json:"max_native_header_bytes" mapstructure:"max_native_header_bytes"`
	MaxProfileBytes         *int64         `json:"max_profile_bytes" mapstructure:"max_profile_bytes"`
	MaxExchanges            *int           `json:"max_exchanges" mapstructure:"max_exchanges"`
	MaxReplays              *int           `json:"max_replays" mapstructure:"max_replays"`
	AdmissionTimeout        *time.Duration `json:"admission_timeout_ns" mapstructure:"admission_timeout_ns"`
	Timeout                 *time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	IdleConnTimeout         *time.Duration `json:"idle_conn_timeout_ns" mapstructure:"idle_conn_timeout_ns"`
}

// NativeOptions requires an explicit SDK profile. Containers are frozen offline;
// fresh spec factories, extension callbacks, keys, writers, resolver/control/dial
// callbacks and Jar are cooperative concurrent borrows until source release.
// Hooks see controlled metadata, not owning bodies/transports; they cannot retain
// arguments or start unaccounted work. Native fhttp order keys are preserved.
type NativeOptions struct {
	private
	Profile            *profiles.ClientProfile
	Transport          *sdk.TransportOptions
	DialContext        func(context.Context, string, string) (net.Conn, error)
	Dialer             *net.Dialer
	LocalAddr          *net.TCPAddr
	ProxyDialerFactory sdk.ProxyDialerFactory
	Jar                http.CookieJar
	DefaultHeaders     http.Header
	ConnectHeaders     http.Header
	CertificatePins    map[string][]string
	BadPinHandler      sdk.BadPinHandlerFunc
	CheckRedirect      func(*http.Request, []*http.Request) error
	PreHooks           []sdk.PreRequestHookFunc
	PostHooks          []sdk.PostResponseHookFunc
}

// Dependencies are caller-owned operation mechanisms and explicit native inputs.
// Prepared.Open rejects a second native selection.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}

func nativeOptions(value NativeOptions) native.NativeOptionsV1 {
	return native.NativeOptionsV1{Profile: value.Profile, Transport: value.Transport, DialContext: value.DialContext,
		Dialer: value.Dialer, LocalAddr: value.LocalAddr, ProxyDialerFactory: value.ProxyDialerFactory, Jar: value.Jar,
		DefaultHeaders: value.DefaultHeaders, ConnectHeaders: value.ConnectHeaders, CertificatePins: value.CertificatePins,
		BadPinHandler: value.BadPinHandler, CheckRedirect: value.CheckRedirect, PreHooks: value.PreHooks, PostHooks: value.PostHooks}
}

// Validate checks strict data/default semantics only. It neither chooses a profile
// nor asserts compatibility with separately supplied native dependencies.
func Validate(value Settings) error {
	data, err := settingsData(value)
	if err != nil {
		return err
	}
	return translate(native.ValidateDataV1(native.OptionsV1{Name: value.Name, Version: value.Version}, source.Layer{Kind: source.Local, Content: data}), "validate")
}
