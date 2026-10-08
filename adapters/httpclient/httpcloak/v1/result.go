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
	"github.com/frost-leo/fathomry/adapters/httpclient/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	http "github.com/sardanioss/http"
)

// Metadata is immutable response evidence without owning request/body/TLS handles.
// Explicit header and URL inspection may disclose sensitive data.
type Metadata struct {
	private
	native native.Metadata
}

func (value Metadata) StatusCode() int          { return value.native.StatusCode() }
func (value Metadata) Protocol() string         { return value.native.Protocol() }
func (value Metadata) URL() string              { return value.native.URL() }
func (value Metadata) HeadersCopy() http.Header { return value.native.HeadersCopy() }
func (value Metadata) ContentLength() int64     { return value.native.ContentLength() }
func (value Metadata) ContentEncoding() string  { return value.native.ContentEncoding() }

// HeaderOrderCopy and HeaderCasingCopy preserve optional native observations;
// nil is unavailable evidence, not an empty observed wire sequence.
func (value Metadata) HeaderOrderCopy() []string  { return value.native.HeaderOrderCopy() }
func (value Metadata) HeaderCasingCopy() []string { return value.native.HeaderCasingCopy() }

// Result preserves separate immutable response, request-input and cleanup facts.
// Complete is not an HTTP-success decision, effect acknowledgement or proof of
// source release. Cancellation never proves an external operation did not occur.
type Result struct {
	private
	native      native.Result
	source      httpclient.Info
	attribution httpclient.Attribution
	attempts    httpclient.Attempts
	present     bool
}

func (value Result) HasData() bool                       { return value.present }
func (value Result) Source() httpclient.Info             { return value.source.Clone() }
func (value Result) Attribution() httpclient.Attribution { return value.attribution }
func (value Result) Attempts() httpclient.Attempts       { return value.attempts }
func (value Result) Metadata() Metadata                  { return Metadata{native: value.native.Metadata()} }
func (value Result) DataCopy() []byte                    { return value.native.DataCopy() }
func (value Result) TrailersCopy() http.Header           { return value.native.TrailersCopy() }
func (value Result) Complete() bool                      { return value.native.Complete() }
func (value Result) InputComplete() bool                 { return value.native.InputComplete() }
func (value Result) ProxyMode() string                   { return value.native.ProxyMode() }
func (value Result) BytesRead() int64                    { return value.native.BytesRead() }

// WireBytesRead counts encoded entity bytes, not HTTP/TLS/QUIC framing.
func (value Result) WireBytesRead() int64    { return value.native.WireBytesRead() }
func (value Result) RequestBytesRead() int64 { return value.native.RequestBytesRead() }

// WritesObserved counts native notifications, not exact attempts or remote effects.
func (value Result) WritesObserved() int { return value.native.WritesObserved() }
func (value Result) Exchanges() int      { return value.native.Exchanges() }
func (value Result) Replays() int        { return value.native.Replays() }

func (value Result) NativeErrorsCopy() []error {
	values := value.native.NativeErrorsCopy()
	for index, value := range values {
		values[index] = translate(value, "native-callback")
	}
	return values
}

func project(value invocation.Result[native.Result], metadata adapters.Info) Result {
	return Result{native: value.Outcome.Value, source: info(value.Source),
		attribution: httpclient.Attribution{Runtime: metadata.Runtime, Operation: metadata.Operation, ID: metadata.ID,
			Sequence: metadata.Sequence, Parent: metadata.Parent, Depth: metadata.Depth, Source: metadata.Source},
		attempts: httpclient.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}, present: value.Outcome.Present}
}

func info(value source.Info) httpclient.Info {
	config := value.Configuration
	result := httpclient.Info{Scope: value.Scope, Provider: config.Identity.Provider, Name: config.Identity.Name,
		Revision: config.Revision, FormatVersion: config.Format}
	for _, layer := range config.Provenance {
		result.Provenance = append(result.Provenance, httpclient.LayerInfo{Kind: uint8(layer.Kind), Fields: append([]string(nil), layer.Fields...)})
	}
	return result
}
