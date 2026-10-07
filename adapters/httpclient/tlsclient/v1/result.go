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
	http "github.com/bogdanfinn/fhttp"

	"github.com/frost-leo/fathomry/adapters/httpclient/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
)

// Metadata is an immutable header observation. Explicit URL/header inspection
// may disclose credentials; it exposes no Request, Body, TLS or transport handle.
type Metadata struct {
	private
	native native.Metadata
}

func (value Metadata) StatusCode() int          { return value.native.StatusCode() }
func (value Metadata) Protocol() string         { return value.native.Protocol() }
func (value Metadata) URL() string              { return value.native.URL() }
func (value Metadata) HeadersCopy() http.Header { return value.native.HeadersCopy() }
func (value Metadata) ContentLength() int64     { return value.native.ContentLength() }
func (value Metadata) Uncompressed() bool       { return value.native.Uncompressed() }

// Result preserves immutable technical facts, not durable data. Complete means
// final native body EOF without primary failure, not business success or absence
// of effects. Hook notices and losing-input errors are separately inspectable.
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
func (value Result) ProxyMode() string                   { return value.native.ProxyMode() }
func (value Result) BytesRead() int64                    { return value.native.BytesRead() }

// RequestBytesRead counts consumed input, including replay and an over-limit
// witness; it does not prove transmission or peer receipt.
func (value Result) RequestBytesRead() int64 { return value.native.RequestBytesRead() }

// Exchanges counts client RoundTrip entries, not every physical wire attempt.
func (value Result) Exchanges() int { return value.native.Exchanges() }

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

// HookErrorsCopy returns bounded translated native hook notices.
func (value Result) HookErrorsCopy() []error {
	return translateErrors(value.native.HookErrorsCopy(), "hook")
}

// InputErrorsCopy includes losing-leg/replay-reader errors, not wire totals.
func (value Result) InputErrorsCopy() []error {
	return translateErrors(value.native.InputErrorsCopy(), "input")
}
func translateErrors(values []error, operation string) []error {
	for index, value := range values {
		values[index] = translate(value, operation)
	}
	return values
}
