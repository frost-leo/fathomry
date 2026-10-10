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

package telemetry

import "github.com/frost-leo/fathomry/adapters/v1"

// Signal names a telemetry data family, not a business disposition.
type Signal string

const (
	Logs    Signal = "logs"
	Traces  Signal = "traces"
	Metrics Signal = "metrics"
)

// Effect describes observed receiver acceptance, not indexing, durable storage
// or evidence custody. Unknown is distinct from a known unattempted export.
type Effect string

const (
	NotAttempted  Effect = ""
	UnknownEffect Effect = "unknown"
	Acknowledged  Effect = "acknowledged"
	PartialEffect Effect = "partial"
)

// SignalResult is detached export evidence, excluding payloads and endpoints.
// TransportCalls counts observed transport entries, not physical attempts.
// Err remains a borrowed immutable cause for deliberate inspection; ordinary
// formatting and JSON never invoke its presentation or traversal methods.
type SignalResult struct {
	private
	Signal                                                      Signal
	Accepted, Submitted, Acknowledged, Rejected, TransportCalls int
	Sampled                                                     bool
	Effect                                                      Effect
	Err                                                         error
}

// Info identifies the frozen source selection, independently of a public
// generation. Provenance contains field names, never credential or endpoint values.
type Info struct {
	private
	Scope, Provider, Name, Revision string
	FormatVersion                   uint32
	Provenance                      []LayerInfo
}

// LayerInfo records one declared configuration layer's kind and field names.
type LayerInfo struct {
	Kind   uint8
	Fields []string
}

// Clone copies every mutable provenance container. A nil outer slice stays nil;
// an empty Fields slice is normalized to nil.
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

// Attribution records actual operation identity and the borrowed generation.
// Zero Source includes direct calls and construction, not known absence;
// lifecycle completion belongs to the independent receipt.
type Attribution struct {
	private
	Runtime, Operation, ID string
	Sequence, Parent       uint64
	Depth                  int
	Source                 adapters.Source
}

// Fact distinguishes unknown, declared, observed and not-applicable information.
// Its Kind and Value do not certify service readiness or compatibility.
type Fact struct {
	private
	Kind, Value string
}

// Option records one effective non-secret selection, not a compatibility promise.
type Option struct{ Name, Value string }
