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

	"github.com/frost-leo/fathomry/internal/resource"
	http "github.com/nukilabs/http"
	quic "github.com/nukilabs/quic-go"
	sdk "github.com/nukilabs/tlsclient"
	"github.com/nukilabs/tlsclient/bandwidth"
	"github.com/nukilabs/tlsclient/profiles"
	tls "github.com/nukilabs/utls"
)

// ProtocolMode controls native transport selection, never a profile catalog.
type ProtocolMode string

const (
	Native          ProtocolMode = "native"
	HTTP1Only       ProtocolMode = "http1"
	HTTP2Negotiated ProtocolMode = "http1-http2"
	HTTP3Only       ProtocolMode = "http3"
)

// NativeOptionsV1 is runtime contract 1. Profile is required. Containers are
// copied, while functions, private keys, randomness, session caches, Jar and
// Tracker are borrowed until assembly release. Borrowed objects must support
// concurrent use and may not retain callback arguments or start unaccounted work.
// Profile factories must transfer fresh, independent mutable ClientHello specs.
// Dial/Listen callbacks transfer one physical socket and its full ownership.
// ListenPacket receives the native remote-address hint, not a local bind address.
// Before hooks receive metadata-only requests; After sees copied metadata.
// No hook receives an SDK client, transport, connection or native response body.
// Native TLS forces OmitEmptyPsk; H3 forces datagrams and disables path-MTU
// discovery. Protocol selection and source ceilings constrain native inputs.
type NativeOptionsV1 struct {
	private
	Profile       *profiles.ClientProfile
	TLS           *tls.Config
	QUIC          *quic.Config
	Transport     *sdk.TransportOptions
	Pinner        *sdk.Pinner
	Jar           http.CookieJar
	Tracker       bandwidth.Tracker
	DialContext   func(context.Context, string, string) (net.Conn, error)
	ListenPacket  func(context.Context, string, string) (net.PacketConn, error)
	CheckRedirect func(*http.Request, []*http.Request) error
	// This replaces QUIC's owning-connection callback with a controlled view.
	AllowConnectionWindowIncrease func(context.Context, uint64) bool
	Before                        []func(context.Context, *http.Request) error
	After                         []func(context.Context, Metadata) error
}

// OptionsV1 configures one externally named source. Version zero selects 1.
// Native profile selection is required and never inferred from the Name.
// Go zero values choose technical defaults; explicit zero layer values are
// validated as supplied. Durations and *_ns layer fields use nanoseconds.
type OptionsV1 struct {
	private
	Name                 string
	Version              uint32
	Mode                 ProtocolMode
	ProxyURL             string
	RoutingLocked        bool
	FollowRedirects      bool
	DisableDecompression bool
	Native               NativeOptionsV1
	MaxActive            int
	QueuedCalls          int
	MaxRoutes            int
	MaxConnections       int
	MaxOrigins           int
	MaxRequestBytes      int64
	MaxResponseBytes     int64
	MaxEncodedBytes      int64
	MaxHeaderBytes       int64
	MaxNativeHeaderBytes int64
	MaxExchanges         int
	MaxReplays           int
	AdmissionTimeout     time.Duration
	Timeout              time.Duration
	IdleConnTimeout      time.Duration
}

type settings struct {
	Mode                 ProtocolMode  `json:"mode"`
	ProxyURL             string        `json:"proxy_url"`
	RoutingLocked        bool          `json:"routing_locked"`
	FollowRedirects      bool          `json:"follow_redirects"`
	DisableDecompression bool          `json:"disable_decompression"`
	MaxActive            int           `json:"max_active"`
	QueuedCalls          int           `json:"queued_calls"`
	MaxRoutes            int           `json:"max_routes"`
	MaxConnections       int           `json:"max_connections"`
	MaxOrigins           int           `json:"max_origins"`
	MaxRequestBytes      int64         `json:"max_request_bytes"`
	MaxResponseBytes     int64         `json:"max_response_bytes"`
	MaxEncodedBytes      int64         `json:"max_encoded_bytes"`
	MaxHeaderBytes       int64         `json:"max_header_bytes"`
	MaxNativeHeaderBytes int64         `json:"max_native_header_bytes"`
	MaxExchanges         int           `json:"max_exchanges"`
	MaxReplays           int           `json:"max_replays"`
	AdmissionTimeout     time.Duration `json:"admission_timeout_ns"`
	Timeout              time.Duration `json:"timeout_ns"`
	IdleConnTimeout      time.Duration `json:"idle_conn_timeout_ns"`
}

func defaults(options OptionsV1) settings {
	value := settings{Mode: options.Mode, ProxyURL: options.ProxyURL, RoutingLocked: options.RoutingLocked,
		FollowRedirects: options.FollowRedirects, DisableDecompression: options.DisableDecompression,
		MaxActive: options.MaxActive, QueuedCalls: options.QueuedCalls, MaxRoutes: options.MaxRoutes,
		MaxConnections: options.MaxConnections, MaxOrigins: options.MaxOrigins,
		MaxRequestBytes: options.MaxRequestBytes, MaxResponseBytes: options.MaxResponseBytes,
		MaxEncodedBytes: options.MaxEncodedBytes, MaxHeaderBytes: options.MaxHeaderBytes,
		MaxNativeHeaderBytes: options.MaxNativeHeaderBytes, MaxExchanges: options.MaxExchanges, MaxReplays: options.MaxReplays,
		AdmissionTimeout: options.AdmissionTimeout, Timeout: options.Timeout, IdleConnTimeout: options.IdleConnTimeout}
	if value.Mode == "" {
		value.Mode = Native
	}
	if value.MaxActive == 0 {
		value.MaxActive = 8
	}
	if value.MaxRoutes == 0 {
		value.MaxRoutes = 16
	}
	if value.MaxConnections == 0 {
		value.MaxConnections = 32
	}
	if value.MaxOrigins == 0 {
		value.MaxOrigins = 64
	}
	if value.MaxRequestBytes == 0 {
		value.MaxRequestBytes = 8 << 20
	}
	if value.MaxResponseBytes == 0 {
		value.MaxResponseBytes = 8 << 20
	}
	if value.MaxEncodedBytes == 0 {
		value.MaxEncodedBytes = 8 << 20
	}
	if value.MaxHeaderBytes == 0 {
		value.MaxHeaderBytes = 64 << 10
	}
	if value.MaxNativeHeaderBytes == 0 {
		value.MaxNativeHeaderBytes = 1 << 20
	}
	if value.MaxExchanges == 0 {
		value.MaxExchanges = 10
	}
	if value.MaxReplays == 0 {
		value.MaxReplays = 16
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
	if value.Mode != Native && value.Mode != HTTP1Only && value.Mode != HTTP2Negotiated && value.Mode != HTTP3Only ||
		value.MaxActive < 1 || value.MaxActive > 1024 || value.QueuedCalls < 0 || value.QueuedCalls > 4096 ||
		value.MaxRoutes < 1 || value.MaxRoutes > 1024 || value.MaxConnections < 1 || value.MaxConnections > 4096 ||
		value.MaxOrigins < 1 || value.MaxOrigins > 1024 || value.MaxExchanges < 1 || value.MaxExchanges > 128 ||
		value.MaxReplays < 1 || value.MaxReplays > 128 ||
		value.MaxHeaderBytes < 1024 || value.MaxHeaderBytes > 1<<20 ||
		value.MaxNativeHeaderBytes < value.MaxHeaderBytes || value.MaxNativeHeaderBytes > 16<<20 {
		return failure(ErrInput, "options")
	}
	for _, limit := range []int64{value.MaxRequestBytes, value.MaxResponseBytes, value.MaxEncodedBytes} {
		if limit < 1 || limit > 1<<30 {
			return failure(ErrInput, "bytes")
		}
	}
	for _, limit := range []time.Duration{value.AdmissionTimeout, value.Timeout, value.IdleConnTimeout} {
		if limit < time.Millisecond || limit > 24*time.Hour {
			return failure(ErrInput, "timeout")
		}
	}
	_, err := parseProxy(value.ProxyURL)
	return err
}

func (value settings) reservation() int64 {
	return value.MaxRequestBytes + 2*value.MaxResponseBytes + value.MaxEncodedBytes +
		2*value.MaxNativeHeaderBytes + value.MaxHeaderBytes*int64(4*value.MaxExchanges+8) + 64<<20
}
func (value settings) evidenceBytes() int64 {
	return value.MaxResponseBytes + 4*value.MaxHeaderBytes + int64(value.MaxReplays+value.MaxExchanges)*1024 + 4096
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls, Bytes: int64(value.MaxActive) * value.reservation(),
		QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 1}
}
