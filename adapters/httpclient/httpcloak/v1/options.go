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
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	http "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
)

const ProviderID = native.ProviderID

// ProtocolMode selects an explicit protocol, never automatic racing or downgrade.
type ProtocolMode string

const (
	HTTP1 ProtocolMode = "http1"
	HTTP2 ProtocolMode = "http2"
	HTTP3 ProtocolMode = "http3"
)

// Settings is strict-loadable format 1 data. Durations are integer nanoseconds;
// nil chooses an Internal default and explicit zero/false remains explicit.
// Exactly one named, strict-JSON or NativeOptions preset is required.
// Explicit serialization may disclose proxy credentials; diagnostics are redacted.
type Settings struct {
	Name                 string         `json:"name" mapstructure:"name"`
	Version              uint32         `json:"version" mapstructure:"version"`
	PresetName           string         `json:"preset_name" mapstructure:"preset_name"`
	PresetJSON           string         `json:"preset_json" mapstructure:"preset_json"`
	Protocol             *ProtocolMode  `json:"protocol" mapstructure:"protocol"`
	ProxyURL             string         `json:"proxy_url" mapstructure:"proxy_url"`
	RoutingLocked        *bool          `json:"routing_locked" mapstructure:"routing_locked"`
	InsecureSkipVerify   *bool          `json:"insecure_skip_verify" mapstructure:"insecure_skip_verify"`
	DisableECH           *bool          `json:"disable_ech" mapstructure:"disable_ech"`
	ResolverAddress      string         `json:"resolver_address" mapstructure:"resolver_address"`
	ResolverNetwork      *string        `json:"resolver_network" mapstructure:"resolver_network"`
	ResolverTimeout      *time.Duration `json:"resolver_timeout_ns" mapstructure:"resolver_timeout_ns"`
	MaxDNSActive         *int           `json:"max_dns_active" mapstructure:"max_dns_active"`
	MaxResolvedAddresses *int           `json:"max_resolved_addresses" mapstructure:"max_resolved_addresses"`
	MaxAddressRaces      *int           `json:"max_address_races" mapstructure:"max_address_races"`
	AddressRaceDelay     *time.Duration `json:"address_race_delay_ns" mapstructure:"address_race_delay_ns"`
	MaxECHEntries        *int           `json:"max_ech_entries" mapstructure:"max_ech_entries"`
	MaxECHConfigBytes    *int           `json:"max_ech_config_bytes" mapstructure:"max_ech_config_bytes"`
	MaxQUICConnections   *int           `json:"max_quic_connections" mapstructure:"max_quic_connections"`
	MaxControlStreams    *int           `json:"max_control_streams" mapstructure:"max_control_streams"`
	MaxActive            *int           `json:"max_active" mapstructure:"max_active"`
	QueuedCalls          *int           `json:"queued_calls" mapstructure:"queued_calls"`
	MaxConnections       *int           `json:"max_connections" mapstructure:"max_connections"`
	MaxBindings          *int           `json:"max_bindings" mapstructure:"max_bindings"`
	MaxRequestBytes      *int64         `json:"max_request_bytes" mapstructure:"max_request_bytes"`
	MaxResponseBytes     *int64         `json:"max_response_bytes" mapstructure:"max_response_bytes"`
	MaxWireBytes         *int64         `json:"max_wire_bytes" mapstructure:"max_wire_bytes"`
	MaxHeaderBytes       *int64         `json:"max_header_bytes" mapstructure:"max_header_bytes"`
	MaxExchanges         *int           `json:"max_exchanges" mapstructure:"max_exchanges"`
	MaxReplays           *int           `json:"max_replays" mapstructure:"max_replays"`
	AdmissionTimeout     *time.Duration `json:"admission_timeout_ns" mapstructure:"admission_timeout_ns"`
	Timeout              *time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
}

// NativeOptions retains the selected native preset and transport settings, not
// the SDK's Client/Session platform. Containers are frozen by Prepare; Jar,
// verification/redirect callbacks and key-log writers remain cooperative borrows
// through source release. They must support concurrent use and not retain owning
// handles or start unaccounted work. ProxyVerify and Verify have distinct authority.
// Their RootCAs pools are borrowed immutable authority, not cloned or enumerated;
// callers requiring an independently mutable root set supply their own Clone.
// Callbacks must not synchronously close their own source or operation.
type NativeOptions struct {
	private
	Preset        *fingerprint.Preset
	Transport     *transport.TransportConfig
	Verify        *transport.TLSVerify
	ProxyVerify   *transport.TLSVerify
	Jar           http.CookieJar
	CheckRedirect func(*http.Request, []*http.Request) error
}

type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}

func nativeOptions(value NativeOptions) native.NativeOptionsV1 {
	return native.NativeOptionsV1{Preset: value.Preset, Transport: value.Transport,
		Verify: value.Verify, ProxyVerify: value.ProxyVerify, Jar: value.Jar,
		CheckRedirect: value.CheckRedirect}
}

// Validate checks strict data semantics, not separately supplied native authority
// or network readiness. Prepare checks the complete final selection.
func Validate(value Settings) error {
	data, err := settingsData(value)
	if err != nil {
		return err
	}
	return translate(native.ValidateDataV1(native.OptionsV1{Name: value.Name, Version: value.Version},
		source.Layer{Kind: source.Local, Content: data}), "validate")
}
