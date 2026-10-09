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

package otel

import (
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
)

// Signal names a telemetry data family, not a business disposition.
type Signal string

const (
	Logs    Signal = "logs"
	Traces  Signal = "traces"
	Metrics Signal = "metrics"
)

// Effect identifies an observed export boundary. Acknowledgement is receiver
// acceptance, never proof of indexing, backend durability or evidence custody.
type Effect string

const (
	NotAttempted  Effect = ""
	UnknownEffect Effect = "unknown"
	Acknowledged  Effect = "acknowledged"
	PartialEffect Effect = "partial"
)

// SignalResult is detached and excludes payloads/endpoints. TransportCalls counts
// RoundTrip entries, not exact physical attempts. Errors remain deliberately
// inspectable but their default presentation never formats private native text.
type SignalResult struct {
	private
	Signal                                                      Signal
	Accepted, Submitted, Acknowledged, Rejected, TransportCalls int
	Sampled                                                     bool
	Effect                                                      Effect
	Err                                                         error
}

// Info identifies the original frozen source, separately from public generation.
type Info struct {
	private
	Scope, Provider, Name, Revision string
	FormatVersion                   uint32
	Provenance                      []LayerInfo
}

// LayerInfo contains declared field provenance, never endpoint or credential values.
type LayerInfo struct {
	Kind   uint8
	Fields []string
}

func (value Info) Clone() Info {
	if value.Provenance == nil {
		return value
	}
	layers := make([]LayerInfo, len(value.Provenance))
	for index, layer := range value.Provenance {
		layers[index] = LayerInfo{Kind: layer.Kind, Fields: append([]string(nil), layer.Fields...)}
	}
	value.Provenance = layers
	return value
}

// Attribution freezes identity and the resource generation actually borrowed.
// Lifecycle state belongs to the public receipt, not this immutable observation.
// Zero Source includes direct calls and source construction, not known absence.
type Attribution struct {
	private
	Runtime, Operation, ID string
	Sequence, Parent       uint64
	Depth                  int
	Source                 adapters.Source
}

// Result shares immutable private facts between direct receipt and evidence.
type Result struct {
	private
	signals     []SignalResult
	source      Info
	attribution Attribution
	present     bool
}

// HasData identifies native signal facts, not source-only cleanup metadata.
func (value Result) HasData() bool { return value.present }

// SignalsCopy returns independent slice storage; retained error objects remain
// immutable borrowed causes for deliberate inspection, not automatic formatting.
func (value Result) SignalsCopy() []SignalResult {
	return append([]SignalResult(nil), value.signals...)
}

// Source returns detached identity for the actual original native source.
func (value Result) Source() Info { return value.source.Clone() }

// Attribution returns frozen operation identity, not current receipt lifecycle.
func (value Result) Attribution() Attribution { return value.attribution }

func project(value invocation.Result[native.Result], attribution adapters.Info) Result {
	result := Result{source: info(value.Source), attribution: publicAttribution(attribution), present: value.Outcome.Present}
	for _, signal := range value.Outcome.Value.SignalsCopy() {
		result.signals = append(result.signals, SignalResult{Signal: Signal(signal.Signal), Accepted: signal.Accepted, Submitted: signal.Submitted,
			Acknowledged: signal.Acknowledged, Rejected: signal.Rejected, TransportCalls: signal.TransportCalls, Sampled: signal.Sampled, Effect: Effect(signal.Effect), Err: translate(signal.Err, "signal")})
	}
	return result
}

func publicAttribution(value adapters.Info) Attribution {
	return Attribution{Runtime: value.Runtime, Operation: value.Operation, ID: value.ID,
		Sequence: value.Sequence, Parent: value.Parent, Depth: value.Depth, Source: value.Source}
}

func info(value source.Info) Info {
	config := value.Configuration
	result := Info{Scope: value.Scope, Provider: config.Identity.Provider, Name: config.Identity.Name, Revision: config.Revision, FormatVersion: config.Format}
	for _, layer := range config.Provenance {
		result.Provenance = append(result.Provenance, LayerInfo{Kind: uint8(layer.Kind), Fields: append([]string(nil), layer.Fields...)})
	}
	return result
}
