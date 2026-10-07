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

package sqlengine

import (
	"slices"

	"github.com/frost-leo/fathomry/adapters/v1"
)

// LayerInfo describes a preparation layer without its settings payload.
// Fields is caller-owned; it contains field names, not credentials or values.
type LayerInfo struct {
	Kind   uint8
	Fields []string
}

// Info identifies a frozen native preparation, not readiness or a resource
// generation. Revision is opaque preparation identity, not a settings hash.
type Info struct {
	private
	Scope, Provider, Name string
	FormatVersion         uint32
	Revision              string
	Provenance            []LayerInfo
}

// Clone detaches both the provenance list and every nested Fields slice.
// The input must not be mutated concurrently; nil and empty slices stay distinct.
func (value Info) Clone() Info {
	value.Provenance = slices.Clone(value.Provenance)
	for index := range value.Provenance {
		value.Provenance[index].Fields = slices.Clone(value.Provenance[index].Fields)
	}
	return value
}

// Attribution captures a public operation and the generation actually borrowed.
// A zero Source also covers direct access or failed acquisition; it is not proof
// that no source was configured. Sequence is local, not a durable resume token.
type Attribution struct {
	private
	Runtime, Operation, ID string
	Sequence, Parent       uint64
	Depth                  int
	Source                 adapters.Source
}

// Attempts counts observed dispatches, not logical calls or acknowledged effects.
// Exact=false with Observed=0 leaves unobserved attempts possible.
type Attempts struct {
	Observed uint64
	Exact    bool
}

// Fact distinguishes unknown (empty Kind), declared, observed, redacted,
// development and not-applicable facts. It does not certify compatibility.
type Fact struct {
	private
	Kind, Value string
}

// Option is an effective non-secret setting, not service-support evidence.
type Option struct{ Name, Value string }

// Profile distinguishes implementation/SDK mode from protocol, native library
// and service observations. Unknown facts remain unknown. Options is caller-owned;
// this neither opens an engine nor proves a catalog, connector or table format.
type Profile struct {
	private
	ImplementationModule, SDKMode                 string
	ServiceMode, ServiceVersion, Protocol, Native Fact
	Options                                       []Option
}

// Clone returns independently mutable option storage.
func (value Profile) Clone() Profile {
	value.Options = slices.Clone(value.Options)
	return value
}
