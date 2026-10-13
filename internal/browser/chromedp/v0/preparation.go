/*
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

package chromedp

import (
	"context"
	"crypto/tls"
	"slices"

	"github.com/frost-leo/fathomry/internal/resource"
)

// NativeOptionsV1 borrows TLS callbacks and private keys for the source lifetime.
// Prepare freezes the TLS containers. It makes no connection and invokes no callback.
type NativeOptionsV1 struct {
	private
	TLS *tls.Config
}

// Budget is the authoritative logical reservation, not browser RSS or wire bytes.
type Budget struct {
	WorkBytes, EvidenceBytes, SourceBytes int64
	Active, Queued, Pages, Streams        int
	Subscriptions                         int
	Bindings, BindingCalls                int
	ObjectGroups, Objects                 int
}

// Prepared owns immutable effective settings and borrowed runtime dependencies.
type Prepared struct {
	private
	data   resource.Prepared[settings]
	budget Budget
	limits resource.Limits
	native NativeOptionsV1
}

// PrepareV1 freezes the exact effective data before policy or construction.
func PrepareV1(options OptionsV1, layers ...resource.Layer) (Prepared, error) {
	version := options.Version
	if version == 0 {
		version = 1
	}
	schema := resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate}
	data, err := resource.Prepare(schema, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return Prepared{}, err
	}
	resolved, err := resource.PrepareData(schema, layers)
	if err != nil {
		return Prepared{}, err
	}
	value, err := resolved.ValueCopy()
	if err != nil {
		return Prepared{}, err
	}
	native := options.Native
	if native.TLS != nil {
		tlsCopy := native.TLS.Clone()
		if tlsCopy.RootCAs != nil {
			tlsCopy.RootCAs = tlsCopy.RootCAs.Clone()
		}
		if tlsCopy.ClientCAs != nil {
			tlsCopy.ClientCAs = tlsCopy.ClientCAs.Clone()
		}
		tlsCopy.NextProtos = slices.Clone(tlsCopy.NextProtos)
		tlsCopy.CipherSuites = slices.Clone(tlsCopy.CipherSuites)
		tlsCopy.CurvePreferences = slices.Clone(tlsCopy.CurvePreferences)
		tlsCopy.EncryptedClientHelloConfigList = slices.Clone(tlsCopy.EncryptedClientHelloConfigList)
		tlsCopy.Certificates = slices.Clone(tlsCopy.Certificates)
		for i := range tlsCopy.Certificates {
			cert := &tlsCopy.Certificates[i]
			cert.SupportedSignatureAlgorithms = slices.Clone(cert.SupportedSignatureAlgorithms)
			cert.Certificate = slices.Clone(cert.Certificate)
			for j := range cert.Certificate {
				cert.Certificate[j] = slices.Clone(cert.Certificate[j])
			}
			cert.OCSPStaple = slices.Clone(cert.OCSPStaple)
			cert.SignedCertificateTimestamps = slices.Clone(cert.SignedCertificateTimestamps)
			for j := range cert.SignedCertificateTimestamps {
				cert.SignedCertificateTimestamps[j] = slices.Clone(cert.SignedCertificateTimestamps[j])
			}
		}
		native.TLS = tlsCopy
	}
	sourceBytes := value.MaxEventBytes + 2*value.MaxCommandBytes + 2*value.MaxResultBytes + 4*65536
	sourceBytes += int64(value.MaxSessions) * int64(value.MaxPages) * (2*value.MaxEventBytes + 2*value.MaxResultBytes + int64(value.MaxEvents)*512 + 65536)
	return Prepared{data: data, limits: value.limits(), native: native, budget: Budget{WorkBytes: value.reservation(), EvidenceBytes: value.evidenceBytes(), SourceBytes: sourceBytes, Active: value.MaxSessions, Queued: value.QueuedCalls, Pages: value.MaxPages, Streams: value.MaxStreams, Subscriptions: value.MaxSubscriptions, Bindings: value.MaxBindings, BindingCalls: value.MaxBindingCalls, ObjectGroups: value.MaxObjectGroups, Objects: value.MaxObjects}}, nil
}
func (prepared Prepared) Budget() Budget          { return prepared.budget }
func (prepared Prepared) Limits() resource.Limits { return prepared.limits }
func (prepared Prepared) Select(lifetime context.Context) (resource.Selection[Source], error) {
	if lifetime == nil || lifetime.Done() == nil {
		return resource.Selection[Source]{}, failure(ErrInput, "lifetime")
	}
	if lifetime.Err() != nil {
		return resource.Selection[Source]{}, failure(ErrState, "lifetime", lifetime.Err(), context.Cause(lifetime))
	}
	if prepared.budget.WorkBytes == 0 {
		return resource.Selection[Source]{}, failure(ErrInput, "preparation")
	}
	return resource.Select(prepared.data, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if err := ctx.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "construct", err, context.Cause(ctx))
		}
		if err := lifetime.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "lifetime", err, context.Cause(lifetime))
		}
		life, cancelCause := context.WithCancelCause(lifetime)
		own := &owner{settings: value, native: prepared.native, life: life, cancel: func() { cancelCause(context.Canceled) }, abort: cancelCause, gate: make(chan struct{}, 1), sessions: make(map[*browserContext]struct{})}
		result := resource.Resource[Source]{Acquired: true, Capability: Source{owner: own}, Release: own.release}
		if value.CheckReady {
			result.Check = own.start
		}
		return result, nil
	}), nil
}
