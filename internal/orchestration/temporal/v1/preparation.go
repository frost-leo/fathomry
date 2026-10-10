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

package temporal

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

// Prepared is an immutable, offline selection. Containers are frozen; native
// extension objects remain borrowed until every constructed source has released.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	value         settings
	runtime       RuntimeOptions
	metadata      Metadata
	parent        *Client
	parentBytes   int64
}

// Metadata describes the same frozen source selected by Selection. Limits bound
// admitted process work, not remote executions. SourceBytes is the declared
// source setup/lifetime envelope; WorkBytes bounds one operation's protobuf
// working envelope. Neither bounds arbitrary borrowed callbacks, caller-owned
// results, native error graphs or process RSS.
type Metadata struct {
	Limits                                                    resource.Limits
	WorkBytes, EvidenceBytes, RPCEvidenceBytes, SourceBytes   int64
	MaxActive, QueuedCalls, MaxRequestBytes, MaxResponseBytes int
	MaxTransportConnections, MaxCallOptions                   int
	ConnectTimeout, RPCTimeout, AdmissionTimeout              time.Duration
}

// PrepareV1 resolves settings and structured native options without dialing,
// constructing native clients, or invoking any borrowed extension.
func PrepareV1(options OptionsV1, runtime RuntimeOptions, layers ...resource.Layer) (Prepared, error) {
	return prepareOptions(options, runtime, nil, layers...)
}

func prepareOptions(options OptionsV1, runtime RuntimeOptions, inheritedSchemes []string, layers ...resource.Layer) (Prepared, error) {
	runtime, err := freezeRuntime(runtime)
	if err != nil {
		return Prepared{}, err
	}
	format := options.Version
	if format == 0 {
		format = 1
	}
	schemes := slices.Clone(inheritedSchemes)
	for _, binding := range runtime.ResolverBuilders {
		schemes = append(schemes, binding.Scheme)
	}
	var resolved settings
	configuration, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value, schemes...); err != nil {
			return err
		}
		if _, err := nativeOptions(value, nil, runtime); err != nil {
			return err
		}
		resolved = value
		return nil
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: format, Layers: layers})
	if err != nil {
		return Prepared{}, err
	}
	encoded, err := json.Marshal(resolved)
	if err != nil {
		return Prepared{}, failure(ErrInput, "configuration", err)
	}
	work := resolved.reservation()
	// Frozen preparation, construction/plugin snapshots and at most two selected
	// TLS option copies per active polling client have a declared container charge.
	tlsBytes := tlsContainerBytes(runtime.ConnectionOptions.TLS) * (8 + int64(len(runtime.Plugins)) + 2*int64(resolved.MaxActive))
	limits := resource.Limits{Active: resolved.MaxActive, Queued: resolved.QueuedCalls, Bytes: int64(resolved.MaxActive) * work,
		QueuedBytes: int64(resolved.QueuedCalls) * work, MaxLeases: 1024}
	metadata := Metadata{Limits: limits, WorkBytes: work, EvidenceBytes: ExecutionEvidenceBytes, RPCEvidenceBytes: 1024,
		SourceBytes:             int64(2*len(encoded)) + 4*work + 256<<10 + MaxTransportConnections*256 + tlsBytes,
		MaxTransportConnections: MaxTransportConnections, MaxCallOptions: MaxCallOptions,
		MaxActive: resolved.MaxActive, QueuedCalls: resolved.QueuedCalls, MaxRequestBytes: resolved.MaxRequestBytes, MaxResponseBytes: resolved.MaxResponseBytes,
		ConnectTimeout: resolved.ConnectTimeout, RPCTimeout: resolved.RPCTimeout, AdmissionTimeout: resolved.AdmissionTimeout}
	return Prepared{configuration: configuration, value: resolved, runtime: runtime, metadata: metadata}, nil
}

func (prepared Prepared) Metadata() Metadata { return prepared.metadata }

func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}

// Options returns detached, deliberately sensitive effective settings, including
// credentials. It never contains live runtime extensions.
func (prepared Prepared) Options() OptionsV1 {
	value := prepared.value
	return OptionsV1{Name: prepared.Description().Identity.Name, Version: 1,
		Endpoint: value.Endpoint, Namespace: value.Namespace, Identity: value.Identity, Lazy: value.Lazy,
		Plaintext: value.Plaintext, RootCAPEM: value.RootCAPEM, CertificatePEM: value.CertificatePEM, PrivateKeyPEM: value.PrivateKeyPEM,
		ServerName: value.ServerName, APIKey: value.APIKey, RPCs: slices.Clone(value.RPCs),
		ConnectTimeout: value.ConnectTimeout, RPCTimeout: value.RPCTimeout, AdmissionTimeout: value.AdmissionTimeout,
		MaxActive: value.MaxActive, QueuedCalls: value.QueuedCalls, MaxRequestBytes: value.MaxRequestBytes, MaxResponseBytes: value.MaxResponseBytes}
}

// Selection returns an inert source with authoritative default limits. Explicit
// composition may replace those limits with a compatible resource.WithLimits.
func (prepared Prepared) Selection() resource.Selection[Source] {
	return resource.WithLimits(sourceSelection(prepared.configuration, prepared.runtime, prepared.parent, prepared.parentBytes), prepared.metadata.Limits)
}

// PrepareFromExistingV1 freezes a derived namespace source. Omitted endpoint and
// wire limits inherit the parent, and transport/authentication always inherit.
// It retains no lease and enters no extension until Selection is assembled.
func PrepareFromExistingV1(options OptionsV1, runtime RuntimeOptions, parent *Client, parentBytes int64, layers ...resource.Layer) (Prepared, error) {
	options, err := inheritSourceOptions(options, parent, parentBytes)
	if err != nil {
		return Prepared{}, err
	}
	prepared, err := prepareOptions(options, runtime, parentResolverSchemes(parent), layers...)
	if err != nil {
		return Prepared{}, err
	}
	if err := validateSharedSource(prepared.value, prepared.runtime, parent, parentBytes); err != nil {
		return Prepared{}, err
	}
	if prepared.metadata.Limits.Bytes > parentBytes {
		return Prepared{}, failure(ErrInput, "shared-envelope")
	}
	prepared.parent, prepared.parentBytes = parent, parentBytes
	return prepared, nil
}

// SelectionFromExisting preserves this exact prepared namespace/settings while
// borrowing the parent's transport and authentication. The endpoint must already
// match; no extension hook runs while validating the derivation.
func (prepared Prepared) SelectionFromExisting(parent *Client, parentBytes int64) (resource.Selection[Source], error) {
	if prepared.parent != nil && (parent == nil || prepared.parent.owner != parent.owner ||
		!prepared.parent.access.SameScope(parent.access) || prepared.parentBytes != parentBytes) {
		return resource.Selection[Source]{}, failure(ErrAuthority, "shared-origin")
	}
	if err := validateSharedSource(prepared.value, prepared.runtime, parent, parentBytes); err != nil {
		return resource.Selection[Source]{}, err
	}
	if prepared.metadata.Limits.Bytes > parentBytes {
		return resource.Selection[Source]{}, failure(ErrInput, "shared-envelope")
	}
	return resource.WithLimits(sourceSelection(prepared.configuration, prepared.runtime, parent, parentBytes), prepared.metadata.Limits), nil
}

func inheritSourceOptions(options OptionsV1, parent *Client, parentBytes int64) (OptionsV1, error) {
	if parent == nil || parent.owner == nil || parent.access == nil || parentBytes < 1 || options.Lazy {
		return OptionsV1{}, failure(ErrInput, "shared-client")
	}
	if options.Endpoint == "" {
		options.Endpoint = parent.owner.settings.Endpoint
	}
	if options.Plaintext && !parent.owner.settings.Plaintext {
		return OptionsV1{}, failure(ErrAuthority, "shared-transport")
	}
	options.Plaintext = parent.owner.settings.Plaintext
	if options.MaxRequestBytes == 0 {
		options.MaxRequestBytes = parent.owner.settings.MaxRequestBytes
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = parent.owner.settings.MaxResponseBytes
	}
	return options, nil
}

func parentResolverSchemes(parent *Client) []string {
	if parent == nil || parent.owner == nil {
		return nil
	}
	schemes := make([]string, len(parent.owner.runtime.ResolverBuilders))
	for index, binding := range parent.owner.runtime.ResolverBuilders {
		schemes[index] = binding.Scheme
	}
	return schemes
}

func validateSharedSource(value settings, runtime RuntimeOptions, parent *Client, parentBytes int64) error {
	if parent == nil || parent.owner == nil || parent.access == nil || parentBytes < value.reservation() {
		return failure(ErrInput, "shared-client")
	}
	if value.Lazy || value.Endpoint != parent.owner.settings.Endpoint || value.Plaintext != parent.owner.settings.Plaintext ||
		value.APIKey != "" || value.RootCAPEM != "" || value.CertificatePEM != "" || value.PrivateKeyPEM != "" || value.ServerName != "" ||
		value.MaxRequestBytes > parent.owner.settings.MaxRequestBytes || value.MaxResponseBytes > parent.owner.settings.MaxResponseBytes ||
		runtime.Credentials != nil || runtime.HeadersProvider != nil || runtime.TrafficController != nil || !emptyConnectionOptions(runtime.ConnectionOptions) ||
		len(runtime.ResolverBuilders) != 0 || runtime.ContextDialer != nil || runtime.UserAgent != "" {
		return failure(ErrAuthority, "shared-transport")
	}
	return nil
}
