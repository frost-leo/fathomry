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

package broker

import (
	"github.com/frost-leo/fathomry/adapters/v1"
)

// Info identifies frozen source settings, not readiness or a public generation.
type Info struct {
	private
	Scope, Provider, Name, Revision string
	FormatVersion                   uint32
}

// Attribution freezes public correlation and the actually borrowed generation.
// Direct-owner calls have a zero Source; it does not mean the client is unowned.
type Attribution struct {
	private
	Runtime, Operation, ID string
	Sequence, Parent       uint64
	Depth                  int
	Source                 adapters.Source
}

// Attempts counts observed native dispatches, not accepted operations or effects.
// Inexact zero does not prove that no native request was made.
type Attempts struct {
	Observed uint64
	Exact    bool
}
