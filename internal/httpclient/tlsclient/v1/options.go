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
	"github.com/frost-leo/fathomry/internal/resource"
)

// ProtocolMode chooses native behavior, not a fingerprint/profile allowlist.
type ProtocolMode string

const (
	Negotiated  ProtocolMode = "negotiated"
	HTTP1Only   ProtocolMode = "http1"
	HTTP3Racing ProtocolMode = "race-http3"
)

// NativeOptionsV1 supplies native runtime objects, never durable configuration.
// Profile is required and may be any valid native or custom profile. Containers
// are copied; functions, private keys, key-log writers, dialers and Jar are borrowed
// until assembly release. They must be concurrently usable, may not retain callback
// arguments or start unaccounted work, and must obey their native contracts.
// Hooks receive metadata-only views per native exchange; owning handles do not escape.
// Native response header limits must be positive or zero for the Provider limit.
// The SDK's negative H3 setting also disables its receive bound and is refused;
// the enforced positive limit is part of the effective wire SETTINGS profile.
// Root pools use native Clone; certificates already added to those pools must
// remain immutable. Copying a native pool cannot freeze its opaque lazy objects.
type NativeOptionsV1 struct {
	private
	Profile            *profiles.ClientProfile
	Transport          *sdk.TransportOptions
	DialContext        func(context.Context, string, string) (net.Conn, error)
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

// OptionsV1 configures one named instance. Version zero selects format 1.
// Native profile selection is external; zero values select technical bounds only.
// Durations and layered *_ns fields are nanoseconds. Layer values are validated
// as supplied rather than silently receiving Go zero-value defaults again.
type OptionsV1 struct {
	private
	Name                    string
	Version                 uint32
	Mode                    ProtocolMode
	ProxyURL                string
	RoutingLocked           bool
	ServerName              string
	InsecureSkipVerify      bool
	RandomTLSExtensionOrder bool
	DisableSessionTickets   bool
	DisableIPV4             bool
	DisableIPV6             bool
	Native                  NativeOptionsV1
	MaxActive               int
	QueuedCalls             int
	MaxBindings             int
	MaxTCPConnections       int
	MaxHTTP3Transports      int
	MaxRequestBytes         int64
	MaxResponseBytes        int64
	MaxHeaderBytes          int64
	MaxNativeHeaderBytes    int64
	MaxExchanges            int
	MaxReplays              int
	AdmissionTimeout        time.Duration
	Timeout                 time.Duration
	IdleConnTimeout         time.Duration
}
type settings struct {
	Mode                    ProtocolMode  `json:"mode"`
	ProxyURL                string        `json:"proxy_url"`
	RoutingLocked           bool          `json:"routing_locked"`
	ServerName              string        `json:"server_name"`
	InsecureSkipVerify      bool          `json:"insecure_skip_verify"`
	RandomTLSExtensionOrder bool          `json:"random_tls_extension_order"`
	DisableSessionTickets   bool          `json:"disable_session_tickets"`
	DisableIPV4             bool          `json:"disable_ipv4"`
	DisableIPV6             bool          `json:"disable_ipv6"`
	MaxActive               int           `json:"max_active"`
	QueuedCalls             int           `json:"queued_calls"`
	MaxBindings             int           `json:"max_bindings"`
	MaxTCPConnections       int           `json:"max_tcp_connections"`
	MaxHTTP3Transports      int           `json:"max_http3_transports"`
	MaxRequestBytes         int64         `json:"max_request_bytes"`
	MaxResponseBytes        int64         `json:"max_response_bytes"`
	MaxHeaderBytes          int64         `json:"max_header_bytes"`
	MaxNativeHeaderBytes    int64         `json:"max_native_header_bytes"`
	MaxExchanges            int           `json:"max_exchanges"`
	MaxReplays              int           `json:"max_replays"`
	AdmissionTimeout        time.Duration `json:"admission_timeout_ns"`
	Timeout                 time.Duration `json:"timeout_ns"`
	IdleConnTimeout         time.Duration `json:"idle_conn_timeout_ns"`
}

func defaults(option OptionsV1) settings {
	value := settings{Mode: option.Mode, ProxyURL: option.ProxyURL, RoutingLocked: option.RoutingLocked, ServerName: option.ServerName,
		InsecureSkipVerify: option.InsecureSkipVerify, RandomTLSExtensionOrder: option.RandomTLSExtensionOrder, DisableSessionTickets: option.DisableSessionTickets,
		DisableIPV4: option.DisableIPV4, DisableIPV6: option.DisableIPV6, MaxActive: option.MaxActive, QueuedCalls: option.QueuedCalls, MaxBindings: option.MaxBindings,
		MaxTCPConnections: option.MaxTCPConnections, MaxHTTP3Transports: option.MaxHTTP3Transports, MaxRequestBytes: option.MaxRequestBytes, MaxResponseBytes: option.MaxResponseBytes,
		MaxHeaderBytes: option.MaxHeaderBytes, MaxNativeHeaderBytes: option.MaxNativeHeaderBytes, MaxExchanges: option.MaxExchanges, MaxReplays: option.MaxReplays, AdmissionTimeout: option.AdmissionTimeout, Timeout: option.Timeout, IdleConnTimeout: option.IdleConnTimeout}
	if value.Mode == "" {
		value.Mode = Negotiated
	}
	if value.MaxActive == 0 {
		value.MaxActive = 8
	}
	if value.MaxBindings == 0 {
		value.MaxBindings = 16
	}
	if value.MaxTCPConnections == 0 {
		value.MaxTCPConnections = 32
	}
	if value.MaxHTTP3Transports == 0 {
		value.MaxHTTP3Transports = 16
	}
	if value.MaxRequestBytes == 0 {
		value.MaxRequestBytes = 8 << 20
	}
	if value.MaxResponseBytes == 0 {
		value.MaxResponseBytes = 8 << 20
	}
	if value.MaxHeaderBytes == 0 {
		value.MaxHeaderBytes = 64 << 10
	}
	if value.MaxNativeHeaderBytes == 0 {
		value.MaxNativeHeaderBytes = 10 << 20
	}
	if value.MaxExchanges == 0 {
		value.MaxExchanges = 10
	}
	if value.MaxReplays == 0 {
		value.MaxReplays = 32
	}
	if value.AdmissionTimeout == 0 {
		value.AdmissionTimeout = 30 * time.Second
	}
	if value.Timeout == 0 {
		value.Timeout = 30 * time.Second
	}
	if value.IdleConnTimeout == 0 {
		value.IdleConnTimeout = 90 * time.Second
	}
	return value
}
func validate(value settings) error {
	if value.Mode != Negotiated && value.Mode != HTTP1Only && value.Mode != HTTP3Racing ||
		value.MaxActive < 1 || value.MaxActive > 1024 || value.QueuedCalls < 0 || value.QueuedCalls > 4096 ||
		value.MaxBindings < 1 || value.MaxBindings > 1024 || value.MaxTCPConnections < 1 || value.MaxTCPConnections > 4096 ||
		value.MaxHTTP3Transports < 1 || value.MaxHTTP3Transports > 1024 || value.MaxRequestBytes < 1 || value.MaxRequestBytes > 1<<30 ||
		value.MaxResponseBytes < 1 || value.MaxResponseBytes > 1<<30 || value.MaxHeaderBytes < 1024 || value.MaxHeaderBytes > 1<<20 ||
		value.MaxNativeHeaderBytes < value.MaxHeaderBytes || value.MaxNativeHeaderBytes > 64<<20 ||
		value.MaxExchanges < 1 || value.MaxExchanges > 128 || value.MaxReplays < 1 || value.MaxReplays > 1024 || value.DisableIPV4 && value.DisableIPV6 {
		return failure(ErrInput, "options")
	}
	for _, duration := range []time.Duration{value.AdmissionTimeout, value.Timeout, value.IdleConnTimeout} {
		if duration < time.Millisecond || duration > 24*time.Hour {
			return failure(ErrInput, "timeout")
		}
	}
	if len(value.ServerName) > 8192 || !fieldValue(value.ServerName) {
		return failure(ErrInput, "server-name")
	}
	_, err := parseProxy(value.ProxyURL, value.Mode)
	return err
}
func (value settings) reservation() int64 {
	return value.MaxRequestBytes + 2*value.MaxResponseBytes + 2*value.MaxNativeHeaderBytes + value.MaxHeaderBytes*int64(8*value.MaxExchanges+12) + int64(value.MaxReplays)*256 + 64<<10
}
func (value settings) evidenceBytes() int64 {
	return value.MaxResponseBytes + 4*value.MaxHeaderBytes + int64(value.MaxExchanges)*1024 + 4096
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls, Bytes: int64(value.MaxActive) * value.reservation(), QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 1}
}
