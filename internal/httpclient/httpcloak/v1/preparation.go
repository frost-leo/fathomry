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
	"context"
	"encoding/json"
	"net"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
	dnswire "github.com/miekg/dns"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
)

// Prepared retains the exact inert preset/settings/native selection used for
// both budget metadata and source construction. Borrowed hooks remain borrowed.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	native        NativeOptionsV1
	metadata      Budget
	policy        RequestPolicyV1
}

// Budget is an authoritative declared envelope, not hard RSS or peer capacity.
// SourceBytes includes persistent bindings, DNS/cache and native QUIC resources.
// WorkBytes also covers one RetainedBindingBytes graph per possible exchange:
// a closed redirect body can retain an evicted binding through operation release.
type Budget struct {
	DNSWorkBytes                                                            int64
	DNSMessageBytes                                                         int
	Limits                                                                  resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes                                   int64
	RetainedBindingBytes                                                    int64
	Timeout, AdmissionTimeout                                               time.Duration
	MaxConnections, MaxBindings, MaxExchanges, MaxReplays                   int
	MaxDNSActive, MaxResolvedAddresses, MaxAddressRaces                     int
	MaxECHEntries, MaxECHConfigBytes, MaxQUICConnections, MaxControlStreams int
	Native                                                                  transport.FathomryBudget
}

func (prepared Prepared) Metadata() Budget { return prepared.metadata }
func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}
func (prepared Prepared) RequestPolicy() RequestPolicyV1 { return prepared.policy }

// ValidateDataInputV1 checks the inert caller-owned envelope before any copying
// or serialization. Resolved field semantics and overlays are checked later.
func ValidateDataInputV1(options OptionsV1) error {
	if len(options.Name) > 64 || len(options.PresetName) > 256 || len(options.PresetJSON) > 1<<20 || len(options.ProxyURL) > 8192 || len(options.ResolverAddress) > 64 || len(options.ResolverNetwork) > 3 || len(options.Protocol) > 16 {
		return failure(ErrLimit, "data-input")
	}
	if options.Version > 1 {
		return failure(ErrInput, "data-version")
	}
	return nil
}

func validateDataEnvelope(options OptionsV1, layers []resource.Layer) error {
	if err := ValidateDataInputV1(options); err != nil {
		return err
	}
	if len(layers) > 4 {
		return failure(ErrLimit, "data-layers")
	}
	for _, layer := range layers {
		if len(layer.Content) > 1<<20 {
			return failure(ErrLimit, "data-layer")
		}
	}
	return nil
}

// ValidateDataV1 checks only strict data, without resolving a native selection or
// invoking borrowed runtime authority. PrepareV1 additionally validates native conflicts.
func ValidateDataV1(options OptionsV1, layers ...resource.Layer) error {
	if err := validateDataEnvelope(options, layers); err != nil {
		return err
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	_, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	return err
}

// PrepareV1 resolves the preset exactly once, before construction or network I/O.
func PrepareV1(options OptionsV1, layers ...resource.Layer) (Prepared, error) {
	if err := validateDataEnvelope(options, layers); err != nil {
		return Prepared{}, err
	}
	native, err := copyNative(options.Native)
	if err != nil {
		return Prepared{}, err
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	var preset *fingerprint.Preset
	var metadata Budget
	var policy RequestPolicyV1
	configuration, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value); err != nil {
			return err
		}
		var err error
		preset, err = choosePreset(value, native)
		if err != nil {
			return err
		}
		if value.Protocol == HTTP1 && (len(native.Transport.ECHConfig) > 0 || native.Transport.ECHConfigDomain != "") {
			return failure(ErrUnsupported, "http1-ech")
		}
		if value.Protocol != HTTP2 && native.Transport.CustomH2Settings != nil || value.Protocol == HTTP1 && len(native.Transport.CustomPseudoOrder) > 0 || value.Protocol == HTTP3 && (native.Transport.CustomJA3 != "" || native.Transport.CustomJA3Extras != nil) || native.Transport.CustomJA3Extras != nil && native.Transport.CustomJA3 == "" {
			return failure(ErrUnsupported, "protocol-options")
		}
		if value.Protocol != HTTP3 && native.Transport.QuicIdleTimeout != 0 {
			return failure(ErrUnsupported, "quic-idle-timeout")
		}
		if len(native.Transport.ECHConfig) > value.MaxECHConfigBytes {
			return failure(ErrLimit, "ech-config")
		}
		if domain := native.Transport.ECHConfigDomain; domain != "" {
			if _, ok := dnswire.IsDomainName(domain); !ok || net.ParseIP(domain) != nil {
				return failure(ErrInput, "ech-domain")
			}
		}
		if !value.DisableECH && native.Transport.ECHConfigDomain != "" && len(native.Transport.ECHConfig) == 0 && value.ResolverAddress == "" {
			return failure(ErrInput, "ech-resolver-authority")
		}
		metadata, err = value.budget(native, preset)
		policy = requestPolicy(value, native)
		return err
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return Prepared{}, err
	}
	native.Preset = fingerprint.Clone(preset)
	return Prepared{configuration: configuration, native: native, metadata: metadata, policy: policy}, nil
}

func protocolNative(value ProtocolMode) transport.Protocol {
	switch value {
	case HTTP1:
		return transport.ProtocolHTTP1
	case HTTP3:
		return transport.ProtocolHTTP3
	default:
		return transport.ProtocolHTTP2
	}
}

func (value settings) budget(native NativeOptionsV1, preset *fingerprint.Preset) (Budget, error) {
	nativeBudget, err := transport.FathomryNativeBudget(preset, native.Transport, protocolNative(value.Protocol), value.MaxHeaderBytes)
	if err != nil {
		return Budget{}, failure(ErrInput, "native-budget", err)
	}
	presetBytes, err := nativeDataBytes(preset, 1<<20)
	if err != nil {
		return Budget{}, err
	}
	containerBytes, err := nativeContainerBytes(native)
	if err != nil {
		return Budget{}, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return Budget{}, failure(ErrInput, "configuration", err)
	}
	retainedBinding := nativeBudget.BindingBytes + presetBytes + int64(len(native.Transport.ECHConfig))
	work := value.reservation() + nativeBudget.WorkBytes + int64(value.MaxExchanges)*retainedBinding
	source := int64(2*len(encoded)) + 2*(presetBytes+containerBytes) + 64<<10
	source += int64(value.MaxBindings) * retainedBinding
	source += int64(value.MaxConnections)*(64<<10) + int64(value.MaxDNSActive)*dnsResidenceBytes
	source += int64(value.MaxECHEntries) * int64(value.MaxECHConfigBytes+512)
	if value.Protocol == HTTP3 {
		source += int64(value.MaxQUICConnections)*nativeBudget.QUICBytes + int64(value.MaxControlStreams)*(132*65527+256<<10+nativeBudget.MASQUEParserBytes)
	}
	if source > 1<<40 || work > 1<<40 {
		return Budget{}, failure(ErrLimit, "native-budget")
	}
	limits := value.limits()
	limits.Bytes, limits.QueuedBytes = int64(value.MaxActive)*work, int64(value.QueuedCalls)*work
	return Budget{Limits: limits, WorkBytes: work, EvidenceBytes: value.evidenceBytes(), SourceBytes: source, RetainedBindingBytes: retainedBinding, DNSWorkBytes: dnsResidenceBytes, DNSMessageBytes: dnswire.MaxMsgSize, Timeout: value.Timeout, AdmissionTimeout: value.AdmissionTimeout,
		MaxConnections: value.MaxConnections, MaxBindings: value.MaxBindings, MaxExchanges: value.MaxExchanges, MaxReplays: value.MaxReplays,
		MaxDNSActive: value.MaxDNSActive, MaxResolvedAddresses: value.MaxResolvedAddresses, MaxAddressRaces: value.MaxAddressRaces,
		MaxECHEntries: value.MaxECHEntries, MaxECHConfigBytes: value.MaxECHConfigBytes, MaxQUICConnections: value.MaxQUICConnections, MaxControlStreams: value.MaxControlStreams, Native: nativeBudget}, nil
}

// Select reuses the frozen object; it does not consult the global preset registry.
func (prepared Prepared) Select() resource.Selection[Source] {
	return resource.Select(prepared.configuration, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if err := ctx.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "construct", err, context.Cause(ctx))
		}
		native, err := copyNative(prepared.native)
		if err != nil {
			return resource.Resource[Source]{}, err
		}
		own := &owner{settings: value, native: native, budget: prepared.metadata, done: make(chan struct{}), sockets: make(map[*trackedConn]struct{}), packets: make(map[*net.UDPConn]func()), ech: make(map[string]echEntry)}
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: own}, Release: own.release}, nil
	})
}
