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
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

// NativeOptionsV1 supplies version 1 of the trusted runtime-extension contract,
// never YAML data. This version is independent of the native Go toolchain.
// Callbacks, private keys, session caches and Jar are borrowed until assembly
// release and must be concurrently usable. No callback may retain arguments or
// start unaccounted work. Cancellation is cooperative, not a Go-code sandbox.
// Time, session-cache and CountError hooks must return promptly without blocking;
// the SDK cannot report an admission error through their signatures. They retain
// lifetime accounting, but do not substitute invented time/cache values at capacity.
// TLS and HTTP2 containers are copied; native object borrowing is documented by
// Select. No native client, transport or connection is returned to consumers.
type NativeOptionsV1 struct {
	private
	TLS           *tls.Config
	HTTP2         *http.HTTP2Config
	DialContext   func(context.Context, string, string) (net.Conn, error)
	Proxy         func(*http.Request) (*url.URL, error)
	CheckRedirect func(*http.Request, []*http.Request) error
	Jar           http.CookieJar
}

// OptionsV1 is configuration contract version 1, independent of the integration
// major and native Go toolchain. It configures one named instance.
// Select borrows it only for preparation;
// later layer overrides are validated before any resource construction.
// Version zero selects format 1. Durations and YAML *_ns fields are nanoseconds.
// Zero Go option values receive the documented technical defaults. Explicit
// zero layer values are validated as supplied, not replaced by defaults.
type OptionsV1 struct {
	private
	Name             string
	Version          uint32
	HTTP1            bool
	HTTP2            bool
	UnencryptedHTTP2 bool
	ProxyURL         string
	// RoutingLocked requires every call to use the configured routing behavior.
	// Otherwise ProxyURL is a default, not a mandatory egress restriction. A
	// configured native Proxy hook keeps its own per-destination semantics.
	RoutingLocked bool
	RootCAPEM     string
	ServerName    string
	ClientCertPEM string
	ClientKeyPEM  string
	// Native TLS cannot be combined with the four textual TLS configuration fields.
	Native NativeOptionsV1
	// MaxActive defaults to 8; QueuedCalls to zero; MaxConnections to 32.
	// Connection capacity is source-wide, including direct connections and dials.
	// Exhaustion closes only idle pooled connections, then rejects if still full.
	// At most MaxConnections route transports are cached or being retired.
	// Failed socket cleanup stays owned and charged until confirmed.
	// Fallible native dependency callbacks have a 4*MaxConnections admission
	// ceiling; prompt, nonblocking native time/cache/diagnostic hooks also retain
	// lifetime accounting but cannot report an admission error through their APIs.
	MaxActive      int
	QueuedCalls    int
	MaxConnections int
	// MaxRequestBytes defaults to 8 MiB, cumulatively across body reads/replays.
	// MaxResponseBytes defaults to 8 MiB, across decoded response/redirect bodies.
	// MaxHeaderBytes defaults to 64 KiB, including conservative metadata overhead.
	MaxRequestBytes  int64
	MaxResponseBytes int64
	MaxHeaderBytes   int64
	// MaxExchanges defaults to 10, bounding client exchanges and separately the
	// count of replay readers. It is not a count of all native wire attempts.
	MaxExchanges int
	// AdmissionTimeout and Timeout default to 30 s; DialTimeout and TLSHandshakeTimeout
	// to 10 s; ResponseHeaderTimeout to 30 s; IdleConnTimeout to 90 s.
	AdmissionTimeout      time.Duration
	Timeout               time.Duration
	DialTimeout           time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	IdleConnTimeout       time.Duration
	// DisableCompression and DisableKeepAlives retain native transport meanings.
	DisableCompression bool
	DisableKeepAlives  bool
}

type settings struct {
	HTTP1                 bool          `json:"http1"`
	HTTP2                 bool          `json:"http2"`
	UnencryptedHTTP2      bool          `json:"unencrypted_http2"`
	ProxyURL              string        `json:"proxy_url"`
	RoutingLocked         bool          `json:"routing_locked"`
	RootCAPEM             string        `json:"root_ca_pem"`
	ServerName            string        `json:"server_name"`
	ClientCertPEM         string        `json:"client_cert_pem"`
	ClientKeyPEM          string        `json:"client_key_pem"`
	MaxActive             int           `json:"max_active"`
	QueuedCalls           int           `json:"queued_calls"`
	MaxConnections        int           `json:"max_connections"`
	MaxRequestBytes       int64         `json:"max_request_bytes"`
	MaxResponseBytes      int64         `json:"max_response_bytes"`
	MaxHeaderBytes        int64         `json:"max_header_bytes"`
	MaxExchanges          int           `json:"max_exchanges"`
	AdmissionTimeout      time.Duration `json:"admission_timeout_ns"`
	Timeout               time.Duration `json:"timeout_ns"`
	DialTimeout           time.Duration `json:"dial_timeout_ns"`
	TLSHandshakeTimeout   time.Duration `json:"tls_handshake_timeout_ns"`
	ResponseHeaderTimeout time.Duration `json:"response_header_timeout_ns"`
	IdleConnTimeout       time.Duration `json:"idle_conn_timeout_ns"`
	DisableCompression    bool          `json:"disable_compression"`
	DisableKeepAlives     bool          `json:"disable_keep_alives"`
}

func defaults(options OptionsV1) settings {
	value := settings{
		HTTP1: options.HTTP1, HTTP2: options.HTTP2, UnencryptedHTTP2: options.UnencryptedHTTP2,
		ProxyURL: options.ProxyURL, RoutingLocked: options.RoutingLocked, RootCAPEM: options.RootCAPEM, ServerName: options.ServerName,
		ClientCertPEM: options.ClientCertPEM, ClientKeyPEM: options.ClientKeyPEM,
		MaxActive: options.MaxActive, QueuedCalls: options.QueuedCalls, MaxConnections: options.MaxConnections,
		MaxRequestBytes: options.MaxRequestBytes, MaxResponseBytes: options.MaxResponseBytes,
		MaxHeaderBytes: options.MaxHeaderBytes, MaxExchanges: options.MaxExchanges,
		AdmissionTimeout: options.AdmissionTimeout, Timeout: options.Timeout, DialTimeout: options.DialTimeout,
		TLSHandshakeTimeout: options.TLSHandshakeTimeout, ResponseHeaderTimeout: options.ResponseHeaderTimeout,
		IdleConnTimeout: options.IdleConnTimeout, DisableCompression: options.DisableCompression,
		DisableKeepAlives: options.DisableKeepAlives,
	}
	if !value.HTTP1 && !value.HTTP2 && !value.UnencryptedHTTP2 {
		value.HTTP1, value.HTTP2 = true, true
	}
	if value.MaxActive == 0 {
		value.MaxActive = 8
	}
	if value.MaxConnections == 0 {
		value.MaxConnections = 32
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
	if value.MaxExchanges == 0 {
		value.MaxExchanges = 10
	}
	if value.AdmissionTimeout == 0 {
		value.AdmissionTimeout = 30 * time.Second
	}
	if value.Timeout == 0 {
		value.Timeout = 30 * time.Second
	}
	if value.DialTimeout == 0 {
		value.DialTimeout = 10 * time.Second
	}
	if value.TLSHandshakeTimeout == 0 {
		value.TLSHandshakeTimeout = 10 * time.Second
	}
	if value.ResponseHeaderTimeout == 0 {
		value.ResponseHeaderTimeout = 30 * time.Second
	}
	if value.IdleConnTimeout == 0 {
		value.IdleConnTimeout = 90 * time.Second
	}
	return value
}

func validate(value settings) error {
	if !value.HTTP1 && !value.HTTP2 && !value.UnencryptedHTTP2 ||
		value.HTTP1 && value.UnencryptedHTTP2 ||
		value.MaxActive < 1 || value.MaxActive > 1024 || value.QueuedCalls < 0 || value.QueuedCalls > 4096 ||
		value.MaxConnections < 1 || value.MaxConnections > 4096 ||
		value.MaxRequestBytes < 1 || value.MaxRequestBytes > 1<<30 ||
		value.MaxResponseBytes < 1 || value.MaxResponseBytes > 1<<30 ||
		value.MaxHeaderBytes < 1024 || value.MaxHeaderBytes > 1<<20 ||
		value.MaxExchanges < 1 || value.MaxExchanges > 128 {
		return failure(ErrInput, "options")
	}
	for _, duration := range []time.Duration{value.AdmissionTimeout, value.Timeout, value.DialTimeout, value.TLSHandshakeTimeout, value.ResponseHeaderTimeout, value.IdleConnTimeout} {
		if duration <= 0 || duration > 24*time.Hour {
			return failure(ErrInput, "timeout")
		}
	}
	if value.ProxyURL != "" {
		proxy, err := url.Parse(value.ProxyURL)
		if err != nil || !validProxy(proxy) {
			return failure(ErrInput, "proxy", err)
		}
	}
	_, err := configuredTLS(value)
	return err
}

func validProxy(proxy *url.URL) bool {
	if proxy == nil {
		return true
	}
	return (proxy.Scheme == "http" || proxy.Scheme == "https" || proxy.Scheme == "socks5" || proxy.Scheme == "socks5h") &&
		proxy.Hostname() != "" && urlFits(proxy, 8192) && proxy.Fragment == "" && proxy.RawQuery == "" &&
		(proxy.Path == "" || proxy.Path == "/")
}

func configuredTLS(value settings) (*tls.Config, error) {
	config := &tls.Config{ServerName: value.ServerName}
	if len(value.RootCAPEM) > 1<<20 || len(value.ClientCertPEM) > 1<<20 || len(value.ClientKeyPEM) > 1<<20 ||
		strings.ContainsAny(value.ServerName, "\r\n\x00") {
		return nil, failure(ErrInput, "tls")
	}
	if value.RootCAPEM != "" {
		config.RootCAs = x509.NewCertPool()
		remaining := strings.TrimSpace(value.RootCAPEM)
		if remaining == "" {
			return nil, failure(ErrInput, "roots")
		}
		for remaining != "" {
			if !strings.HasPrefix(remaining, "-----BEGIN CERTIFICATE-----") {
				return nil, failure(ErrInput, "roots")
			}
			block, rest := pem.Decode([]byte(remaining))
			if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
				return nil, failure(ErrInput, "roots")
			}
			certificate, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, failure(ErrInput, "roots", err)
			}
			config.RootCAs.AddCert(certificate)
			remaining = strings.TrimSpace(string(rest))
		}
	}
	if value.ClientCertPEM != "" || value.ClientKeyPEM != "" {
		certificate, err := tls.X509KeyPair([]byte(value.ClientCertPEM), []byte(value.ClientKeyPEM))
		if err != nil {
			return nil, failure(ErrInput, "certificate", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func (value settings) reservation() int64 {
	return value.MaxRequestBytes + 2*value.MaxResponseBytes + value.MaxHeaderBytes*int64(4*value.MaxExchanges+8) + 64<<10
}
func (value settings) evidenceBytes() int64 {
	return value.MaxResponseBytes + 4*value.MaxHeaderBytes + 4096
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls, Bytes: int64(value.MaxActive) * value.reservation(),
		QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 2}
}
