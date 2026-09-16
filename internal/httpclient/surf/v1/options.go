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
	"github.com/frost-leo/fathomry/internal/resource"
	utls "github.com/refraction-networking/utls"
)

// ProtocolMode chooses transport behavior, not a business/browser profile.
type ProtocolMode string

const (
	Negotiated  ProtocolMode = "negotiated"
	HTTP1Only   ProtocolMode = "http1"
	HTTP2Only   ProtocolMode = "http2"
	PreferHTTP3 ProtocolMode = "prefer-http3"
)

// NativeOptionsV1 keeps SDK-specific extension points outside layered settings.
// Containers are copied. Functions, session caches, private keys, writers, Jar,
// dialers and resolvers are borrowed through assembly release and must be safe
// for concurrent use. They must not retain arguments or launch unowned work.
// Profile is optional: absence means native standard TLS, not a default browser.
// HelloSpecFactory overrides the profile's Hello and transfers a fresh mutable
// spec per handshake, preserving opaque/custom extensions without unsafe copying.
// JAConfig is uTLS-specific; TLSConfig governs standard TLS and H3. H3 does not
// use the JA spec. Middleware receives native metadata views, never owning handles.
// Managed callback panics retain error-valued causes without formatting payloads.
// A cache/clock panic has no native error return, so it disables that route and
// remains observable through assembly cleanup, including after a call has ended.
// Callbacks must not exit their goroutine/process. Opaque native connections and
// private keys must obey their native interface contracts, including non-panicking
// methods; transferred Close operations must terminate I/O even on error.
type NativeOptionsV1 struct {
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

// OptionsV1 configures one named instance. Version zero selects format 1.
// Numeric defaults are technical bounds only. Retry settings configure Surf's
// native retry mechanism; zero retries does not add an application retry policy.
// Duration and layered *_ns values are nanoseconds. Overrides are validated as
// supplied, without silently restoring Go defaults.
type OptionsV1 struct {
	private
	Name                 string
	Version              uint32
	Mode                 ProtocolMode
	ProxyURL             string
	RoutingLocked        bool
	DisableCompression   bool
	Native               NativeOptionsV1
	MaxActive            int
	QueuedCalls          int
	MaxRoutes            int
	MaxTCPConnections    int
	MaxUDPSockets        int
	MaxRequestBytes      int64
	MaxResponseBytes     int64
	MaxHeaderBytes       int64
	MaxNativeHeaderBytes int64
	MaxRoundTrips        int
	MaxReplays           int
	NativeRetries        int
	RetryCodes           []int
	RetryDelay           time.Duration
	AdmissionTimeout     time.Duration
	Timeout              time.Duration
	IdleConnTimeout      time.Duration
}

type settings struct {
	Mode                 ProtocolMode  `json:"mode"`
	ProxyURL             string        `json:"proxy_url"`
	RoutingLocked        bool          `json:"routing_locked"`
	DisableCompression   bool          `json:"disable_compression"`
	MaxActive            int           `json:"max_active"`
	QueuedCalls          int           `json:"queued_calls"`
	MaxRoutes            int           `json:"max_routes"`
	MaxTCPConnections    int           `json:"max_tcp_connections"`
	MaxUDPSockets        int           `json:"max_udp_sockets"`
	MaxRequestBytes      int64         `json:"max_request_bytes"`
	MaxResponseBytes     int64         `json:"max_response_bytes"`
	MaxHeaderBytes       int64         `json:"max_header_bytes"`
	MaxNativeHeaderBytes int64         `json:"max_native_header_bytes"`
	MaxRoundTrips        int           `json:"max_round_trips"`
	MaxReplays           int           `json:"max_replays"`
	NativeRetries        int           `json:"native_retries"`
	RetryCodes           []int         `json:"retry_codes"`
	RetryDelay           time.Duration `json:"retry_delay_ns"`
	AdmissionTimeout     time.Duration `json:"admission_timeout_ns"`
	Timeout              time.Duration `json:"timeout_ns"`
	IdleConnTimeout      time.Duration `json:"idle_conn_timeout_ns"`
}

func defaults(options OptionsV1) settings {
	value := settings{Mode: options.Mode, ProxyURL: options.ProxyURL, RoutingLocked: options.RoutingLocked, DisableCompression: options.DisableCompression,
		MaxActive: options.MaxActive, QueuedCalls: options.QueuedCalls, MaxRoutes: options.MaxRoutes,
		MaxTCPConnections: options.MaxTCPConnections, MaxUDPSockets: options.MaxUDPSockets,
		MaxRequestBytes: options.MaxRequestBytes, MaxResponseBytes: options.MaxResponseBytes, MaxHeaderBytes: options.MaxHeaderBytes, MaxNativeHeaderBytes: options.MaxNativeHeaderBytes,
		MaxRoundTrips: options.MaxRoundTrips, MaxReplays: options.MaxReplays, NativeRetries: options.NativeRetries,
		RetryCodes: append([]int(nil), options.RetryCodes...), RetryDelay: options.RetryDelay,
		AdmissionTimeout: options.AdmissionTimeout, Timeout: options.Timeout, IdleConnTimeout: options.IdleConnTimeout}
	if value.Mode == "" {
		value.Mode = Negotiated
	}
	if value.MaxActive == 0 {
		value.MaxActive = 8
	}
	if value.MaxRoutes == 0 {
		value.MaxRoutes = 16
	}
	if value.MaxTCPConnections == 0 {
		value.MaxTCPConnections = 32
	}
	if value.MaxUDPSockets == 0 {
		value.MaxUDPSockets = 16
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
	if value.MaxRoundTrips == 0 {
		value.MaxRoundTrips = 10
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
	if value.Mode != Negotiated && value.Mode != HTTP1Only && value.Mode != HTTP2Only && value.Mode != PreferHTTP3 ||
		value.MaxActive < 1 || value.MaxActive > 1024 || value.QueuedCalls < 0 || value.QueuedCalls > 4096 ||
		value.MaxRoutes < 1 || value.MaxRoutes > 1024 || value.MaxTCPConnections < 1 || value.MaxTCPConnections > 4096 ||
		value.MaxUDPSockets < 1 || value.MaxUDPSockets > 1024 || value.MaxRequestBytes < 1 || value.MaxRequestBytes > 1<<30 ||
		value.MaxResponseBytes < 1 || value.MaxResponseBytes > 1<<30 || value.MaxHeaderBytes < 1024 || value.MaxHeaderBytes > 1<<20 ||
		value.MaxNativeHeaderBytes < value.MaxHeaderBytes || value.MaxNativeHeaderBytes > 64<<20 ||
		value.MaxRoundTrips < 1 || value.MaxRoundTrips > 128 || value.MaxReplays < 1 || value.MaxReplays > 1024 ||
		value.NativeRetries < 0 || value.NativeRetries >= value.MaxRoundTrips || len(value.RetryCodes) > 64 ||
		value.RetryDelay < 0 || value.RetryDelay > 24*time.Hour {
		return failure(ErrInput, "options")
	}
	for _, code := range value.RetryCodes {
		if code < 100 || code > 599 {
			return failure(ErrInput, "retry-code")
		}
	}
	for _, duration := range []time.Duration{value.Timeout, value.AdmissionTimeout, value.IdleConnTimeout} {
		if duration < time.Millisecond || duration > 24*time.Hour {
			return failure(ErrInput, "timeout")
		}
	}
	_, err := parseProxy(value.ProxyURL)
	return err
}
func (value settings) reservation() int64 {
	return 3*value.MaxRequestBytes + 3*value.MaxResponseBytes + 2*value.MaxNativeHeaderBytes +
		value.MaxHeaderBytes*int64(4*value.MaxRoundTrips+12) + int64(value.MaxReplays)*256 + 64<<10
}
func (value settings) evidenceBytes() int64 {
	return value.MaxResponseBytes + 4*value.MaxHeaderBytes + int64(value.MaxRoundTrips)*256 + 4096
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls, Bytes: int64(value.MaxActive) * value.reservation(),
		QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 1}
}
