/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package notification

import (
	"github.com/frost-leo/fathomry/adapters/v1"
	"slices"
)

// LayerInfo contains preparation field names, never values.
type LayerInfo struct {
	Kind   uint8
	Fields []string
}

// Info identifies frozen configuration, not readiness or a resource generation.
// Revision is opaque and must not be treated as a credential hash.
type Info struct {
	private
	Scope, Provider, Name, Revision string
	FormatVersion                   uint32
	Provenance                      []LayerInfo
}

func (value Info) Clone() Info {
	value.Provenance = slices.Clone(value.Provenance)
	for index := range value.Provenance {
		value.Provenance[index].Fields = slices.Clone(value.Provenance[index].Fields)
	}
	return value
}

// Attribution identifies the admitted operation and actual borrowed generation.
// Zero Source covers direct use and failed acquisition, not known absence.
type Attribution struct {
	private
	Runtime, Operation, ID string
	Sequence, Parent       uint64
	Depth                  int
	Source                 adapters.Source
}

// Attempts is a provider-defined observed dispatch count, not a delivery count.
type Attempts struct {
	Observed uint64
	Exact    bool
}

// Observation names distinct confirmed facts. Absence means no such observation,
// not a negative guarantee. No fact implies business completion or human reading.
type Observation string

const (
	RelayAccepted          Observation = "smtp-relay-accepted"
	PlatformAcknowledged   Observation = "platform-acknowledged"
	CallbackAuthenticated  Observation = "callback-authenticated"
	AcknowledgementWritten Observation = "acknowledgement-written"
	LocalProcessSucceeded  Observation = "local-process-succeeded"
	RecipientReadObserved  Observation = "recipient-read-observed"
)
