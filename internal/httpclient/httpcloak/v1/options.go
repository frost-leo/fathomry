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
	"github.com/frost-leo/fathomry/internal/resource"
	http "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
	"net"
	"strconv"
	"time"
)

// ProtocolMode selects an actual wire protocol, not an experimental recipe.
// Automatic racing, protocol downgrades and UDP proxies are not silently enabled.
type ProtocolMode string

const (
	HTTP1 ProtocolMode = "http1"
	HTTP2 ProtocolMode = "http2"
	HTTP3 ProtocolMode = "http3"
)

// NativeOptionsV1 retains the native fingerprint and transport configuration.
// Data containers and verification wrappers are copied. Verification hooks,
// immutable RootCAs pools, Jar and key-log writers are borrowed
// through source release, must support concurrent calls, and must not start
// unaccounted work. Preset, PresetName and PresetJSON are mutually exclusive.
// Distributed session-cache backends and native mutable Setters are not exposed.
type NativeOptionsV1 struct {
	private
	Preset        *fingerprint.Preset
	Transport     *transport.TransportConfig
	Verify        *transport.TLSVerify
	ProxyVerify   *transport.TLSVerify
	Jar           http.CookieJar
	CheckRedirect func(*http.Request, []*http.Request) error
}

// OptionsV1 defines one externally named instance. Version zero means format 1.
// No browser preset is defaulted. Zero numerical fields select technical bounds;
// layer overrides are validated as supplied. Durations are nanoseconds.
type OptionsV1 struct {
	private
	Name                 string
	Version              uint32
	PresetName           string
	PresetJSON           string
	Protocol             ProtocolMode
	ProxyURL             string
	RoutingLocked        bool
	InsecureSkipVerify   bool
	DisableECH           bool
	ResolverAddress      string
	ResolverNetwork      string
	ResolverTimeout      time.Duration
	MaxDNSActive         int
	MaxResolvedAddresses int
	MaxAddressRaces      int
	AddressRaceDelay     time.Duration
	MaxECHEntries        int
	MaxECHConfigBytes    int
	MaxQUICConnections   int
	MaxControlStreams    int
	Native               NativeOptionsV1
	MaxActive            int
	QueuedCalls          int
	MaxConnections       int
	MaxBindings          int
	MaxRequestBytes      int64
	MaxResponseBytes     int64
	MaxWireBytes         int64
	MaxHeaderBytes       int64
	MaxExchanges         int
	MaxReplays           int
	AdmissionTimeout     time.Duration
	Timeout              time.Duration
}
type settings struct {
	PresetName           string        `json:"preset_name"`
	PresetJSON           string        `json:"preset_json"`
	Protocol             ProtocolMode  `json:"protocol"`
	ProxyURL             string        `json:"proxy_url"`
	RoutingLocked        bool          `json:"routing_locked"`
	InsecureSkipVerify   bool          `json:"insecure_skip_verify"`
	DisableECH           bool          `json:"disable_ech"`
	ResolverAddress      string        `json:"resolver_address"`
	ResolverNetwork      string        `json:"resolver_network"`
	ResolverTimeout      time.Duration `json:"resolver_timeout_ns"`
	MaxDNSActive         int           `json:"max_dns_active"`
	MaxResolvedAddresses int           `json:"max_resolved_addresses"`
	MaxAddressRaces      int           `json:"max_address_races"`
	AddressRaceDelay     time.Duration `json:"address_race_delay_ns"`
	MaxECHEntries        int           `json:"max_ech_entries"`
	MaxECHConfigBytes    int           `json:"max_ech_config_bytes"`
	MaxQUICConnections   int           `json:"max_quic_connections"`
	MaxControlStreams    int           `json:"max_control_streams"`
	MaxActive            int           `json:"max_active"`
	QueuedCalls          int           `json:"queued_calls"`
	MaxConnections       int           `json:"max_connections"`
	MaxBindings          int           `json:"max_bindings"`
	MaxRequestBytes      int64         `json:"max_request_bytes"`
	MaxResponseBytes     int64         `json:"max_response_bytes"`
	MaxWireBytes         int64         `json:"max_wire_bytes"`
	MaxHeaderBytes       int64         `json:"max_header_bytes"`
	MaxExchanges         int           `json:"max_exchanges"`
	MaxReplays           int           `json:"max_replays"`
	AdmissionTimeout     time.Duration `json:"admission_timeout_ns"`
	Timeout              time.Duration `json:"timeout_ns"`
}

func defaults(input OptionsV1) settings {
	value := settings{PresetName: input.PresetName, PresetJSON: input.PresetJSON, Protocol: input.Protocol, ProxyURL: input.ProxyURL, RoutingLocked: input.RoutingLocked, InsecureSkipVerify: input.InsecureSkipVerify, DisableECH: input.DisableECH, MaxActive: input.MaxActive, QueuedCalls: input.QueuedCalls, MaxConnections: input.MaxConnections, MaxBindings: input.MaxBindings, MaxRequestBytes: input.MaxRequestBytes, MaxResponseBytes: input.MaxResponseBytes, MaxWireBytes: input.MaxWireBytes, MaxHeaderBytes: input.MaxHeaderBytes, MaxExchanges: input.MaxExchanges, MaxReplays: input.MaxReplays, AdmissionTimeout: input.AdmissionTimeout, Timeout: input.Timeout}
	value.ResolverAddress, value.ResolverNetwork, value.ResolverTimeout = input.ResolverAddress, input.ResolverNetwork, input.ResolverTimeout
	value.MaxDNSActive, value.MaxResolvedAddresses, value.MaxAddressRaces = input.MaxDNSActive, input.MaxResolvedAddresses, input.MaxAddressRaces
	value.AddressRaceDelay, value.MaxECHEntries, value.MaxECHConfigBytes = input.AddressRaceDelay, input.MaxECHEntries, input.MaxECHConfigBytes
	value.MaxQUICConnections, value.MaxControlStreams = input.MaxQUICConnections, input.MaxControlStreams
	if value.ResolverNetwork == "" {
		value.ResolverNetwork = "udp"
	}
	if value.ResolverTimeout == 0 {
		value.ResolverTimeout = 5 * time.Second
	}
	if value.MaxDNSActive == 0 {
		value.MaxDNSActive = 4
	}
	if value.MaxResolvedAddresses == 0 {
		value.MaxResolvedAddresses = 16
	}
	if value.MaxAddressRaces == 0 {
		value.MaxAddressRaces = 2
	}
	if value.AddressRaceDelay == 0 {
		value.AddressRaceDelay = 250 * time.Millisecond
	}
	if value.MaxECHEntries == 0 {
		value.MaxECHEntries = 64
	}
	if value.MaxECHConfigBytes == 0 {
		value.MaxECHConfigBytes = 64 << 10
	}
	if value.MaxQUICConnections == 0 {
		value.MaxQUICConnections = 32
	}
	if value.MaxControlStreams == 0 {
		value.MaxControlStreams = 16
	}
	if value.Protocol == "" {
		value.Protocol = HTTP2
	}
	if value.MaxActive == 0 {
		value.MaxActive = 8
	}
	if value.MaxConnections == 0 {
		value.MaxConnections = 16
	}
	if value.MaxBindings == 0 {
		value.MaxBindings = 16
	}
	if value.MaxRequestBytes == 0 {
		value.MaxRequestBytes = 8 << 20
	}
	if value.MaxResponseBytes == 0 {
		value.MaxResponseBytes = 8 << 20
	}
	if value.MaxWireBytes == 0 {
		value.MaxWireBytes = 8 << 20
	}
	if value.MaxHeaderBytes == 0 {
		value.MaxHeaderBytes = 64 << 10
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
	return value
}
func validate(value settings) error {
	if value.Protocol != HTTP1 && value.Protocol != HTTP2 && value.Protocol != HTTP3 ||
		value.MaxActive < 1 || value.MaxActive > 1024 || value.QueuedCalls < 0 || value.QueuedCalls > 4096 ||
		value.MaxConnections < 1 || value.MaxConnections > 4096 || value.MaxBindings < 1 || value.MaxBindings > 1024 ||
		value.MaxRequestBytes < 1 || value.MaxRequestBytes > 1<<30 || value.MaxResponseBytes < 1 || value.MaxResponseBytes > 1<<30 ||
		value.MaxWireBytes < 1 || value.MaxWireBytes > 1<<30 || value.MaxHeaderBytes < 1024 || value.MaxHeaderBytes > 1<<20 ||
		value.MaxExchanges < 1 || value.MaxExchanges > 128 || value.MaxReplays < 1 || value.MaxReplays > 1024 ||
		len(value.PresetName) > 256 || len(value.PresetJSON) > 1<<20 {
		return failure(ErrInput, "options")
	}
	for _, duration := range []time.Duration{value.Timeout, value.AdmissionTimeout} {
		if duration < time.Millisecond || duration > 24*time.Hour {
			return failure(ErrInput, "timeout")
		}
	}
	if value.ResolverNetwork != "udp" && value.ResolverNetwork != "tcp" || value.ResolverTimeout < time.Millisecond || value.ResolverTimeout > time.Minute || value.MaxDNSActive < 1 || value.MaxDNSActive > 1024 || value.MaxResolvedAddresses < 1 || value.MaxResolvedAddresses > 256 || value.MaxAddressRaces < 1 || value.MaxAddressRaces > value.MaxResolvedAddresses || value.AddressRaceDelay < time.Millisecond || value.AddressRaceDelay > time.Second || value.MaxECHEntries < 1 || value.MaxECHEntries > 4096 || value.MaxECHConfigBytes < 1 || value.MaxECHConfigBytes > 1<<16 || value.MaxQUICConnections < 1 || value.MaxQUICConnections > 4096 || value.MaxControlStreams < 1 || value.MaxControlStreams > 1024 {
		return failure(ErrInput, "resolver-limits")
	}
	if value.ResolverAddress != "" {
		host, port, err := net.SplitHostPort(value.ResolverAddress)
		service, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || net.ParseIP(host) == nil || service < 1 || service > 65535 {
			return failure(ErrInput, "resolver-address")
		}
	}
	proxy, err := parseProxy(value.ProxyURL)
	if err != nil {
		return err
	}
	if proxy != nil && (value.Protocol == HTTP3 && (proxy.Scheme == "http" || proxy.Scheme == "https") || value.Protocol != HTTP3 && proxy.Scheme == "masque") {
		return failure(ErrUnsupported, "proxy-protocol")
	}
	return nil
}
func (value settings) reservation() int64 {
	return value.MaxRequestBytes + 2*value.MaxResponseBytes + value.MaxWireBytes + value.MaxHeaderBytes*int64(8*value.MaxExchanges+16) + int64(value.MaxReplays)*512 + 256<<10
}
func (value settings) evidenceBytes() int64 {
	return value.MaxResponseBytes + 4*value.MaxHeaderBytes + 4096
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls, Bytes: int64(value.MaxActive) * value.reservation(), QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 1}
}
