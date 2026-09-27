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

package cli

import "github.com/frost-leo/fathomry/failure/v1"

// completion keeps host classification separate from the semantic return value.
// Slots are observations, not a public result schema or a flattened cause graph.
type completion struct {
	returned         error
	usage            bool
	presentation     error
	output           error
	diagnostics      error
	diagnosticRender error
	cancellation     error
}

func (result completion) err() error {
	slots := []error{result.returned, result.presentation, result.output, result.diagnostics, result.diagnosticRender, result.cancellation}
	seen := make(map[*failure.Error]bool)
	seenOutput := make(map[*outputFailure]bool)
	kept := make([]error, 0, len(slots))
	for _, err := range slots {
		if err == nil {
			continue
		}
		if output, ok := err.(*outputFailure); ok && output != nil && output.core != nil && output.core.Diagnostic().Condition == ErrOutput {
			if seenOutput[output] {
				continue
			}
			seenOutput[output] = true
		}
		// Only exact, valid core pointers can identify repeated observations of the
		// returned host latch. Foreign accessors/equality/hooks are never consulted.
		if core, ok := err.(*failure.Error); ok && core != nil && core.Diagnostic().Condition.Valid() {
			if seen[core] {
				continue
			}
			seen[core] = true
		}
		kept = append(kept, err)
	}
	return combine(kept...)
}

func (result completion) status() int {
	if result.presentation != nil || result.output != nil || result.diagnostics != nil || result.diagnosticRender != nil {
		return 1
	}
	if result.usage && result.returned != nil {
		if result.cancellation != nil {
			return 1
		}
		return 2
	}
	return exitStatus(result.err())
}
