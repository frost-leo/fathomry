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

package zap

import (
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
)

// SinkState preserves native Zap observations, not backend durability.
type SinkState uint8

const (
	NotAttempted SinkState = iota
	Filtered
	Written
	Synced
	Failed
)

// SinkResult preserves each destination independently, including bytes from
// partial writes. Structured bytes are unknown (zero); errors are safe by default.
type SinkResult struct {
	private
	logging.Output
	State SinkState
	Bytes int
	Err   error
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
type Attribution = logging.Attribution

// Correlation is the native process-local, physical-source-scoped identity
// written into technical record fields. It is not globally unique, durable or
// proof of an emitted/accepted/exported record.
type Correlation struct {
	private
	Call, Parent, Owner string
}

// Result shares immutable private facts between direct receipt and evidence.
type Result struct {
	private
	sinks          []SinkResult
	policyRevision string
	source         Info
	attribution    Attribution
	correlation    Correlation
	present        bool
}

// HasData identifies native sink facts, not source-only cleanup metadata.
func (value Result) HasData() bool { return value.present }

// SinksCopy returns independent per-destination observations.
func (value Result) SinksCopy() []SinkResult { return append([]SinkResult(nil), value.sinks...) }

// PolicyRevision is the logical filtering revision; Source retains physical identity.
func (value Result) PolicyRevision() string { return value.policyRevision }

// Source returns detached identity for the actual original native source.
func (value Result) Source() Info { return value.source.Clone() }

// Attribution returns frozen operation identity, not current receipt lifecycle.
func (value Result) Attribution() Attribution { return value.attribution }

// NativeCorrelation links actual native evidence to local fathomry.call and
// the typed bridge's logging.call. Public WithID/Sequence are distinct namespaces.
// Zero means no native correlation, including source/family-only observations.
func (value Result) NativeCorrelation() Correlation { return value.correlation }

func project(value invocation.Result[native.Result], attribution adapters.Info, revision string, outputs []logging.Output) Result {
	result := Result{source: info(value.Source), attribution: publicAttribution(attribution), present: value.Outcome.Present, policyRevision: revision}
	actual := value.Context.Correlation
	result.correlation = Correlation{Call: actual.Call, Parent: actual.Parent, Owner: actual.Owner}
	for _, sink := range value.Outcome.Value.SinksCopy() {
		output := logging.Output{Name: sink.Name}
		for _, candidate := range outputs {
			if candidate.Name == sink.Name {
				output = candidate
				break
			}
		}
		result.sinks = append(result.sinks, SinkResult{Output: output, State: SinkState(sink.State), Bytes: sink.Bytes, Err: translate(sink.Err, "sink")})
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
