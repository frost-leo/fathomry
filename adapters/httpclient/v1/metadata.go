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

package httpclient

import (
	"github.com/frost-leo/fathomry/adapters/v1"
	"slices"
)

// LayerInfo contains preparation field names, never their values.
type LayerInfo struct {
	Kind   uint8
	Fields []string
}

// Info identifies frozen preparation, not readiness or a resource generation.
// Revision is opaque, not a credential hash. Clone isolates its slices.
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

// Attribution freezes operation identity and the generation actually borrowed.
// Zero Source also covers direct use and failed acquisition, not known absence.
type Attribution struct {
	private
	Runtime, Operation, ID string
	Sequence, Parent       uint64
	Depth                  int
	Source                 adapters.Source
}

// Attempts reports observations, not guaranteed wire attempts or peer effects.
// Exact=false with Observed=0 permits an unobserved submission.
type Attempts struct {
	Observed uint64
	Exact    bool
}
