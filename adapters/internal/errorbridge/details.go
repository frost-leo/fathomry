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

package errorbridge

import "github.com/frost-leo/fathomry/failure/v1"

// Details finds only the typed details of the supplied current core, through
// transparent wrappers. It never borrows a different occurrence's details merely
// because that occurrence appears in a cause graph. Traversal is bounded even
// when foreign errors contain cycles; T retains its original clone contract.
func Details[T any](original error, core *failure.Error, limit int) (T, bool) {
	var zero T
	if core == nil {
		return zero, false
	}
	remaining := limit
	var visit func(error) (T, bool)
	visit = func(err error) (T, bool) {
		if err == nil || remaining <= 0 {
			return zero, false
		}
		remaining--
		if candidate, ok := failure.Inspect(err); ok && candidate != core {
			return zero, false
		}
		if detailed, ok := err.(*failure.Detailed[T]); ok && detailed.Failure() == core {
			return detailed.Details()
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range value.Unwrap() {
				if remaining <= 0 {
					break
				}
				if details, present := visit(child); present {
					return details, true
				}
			}
		case interface{ Unwrap() error }:
			return visit(value.Unwrap())
		}
		return zero, false
	}
	return visit(original)
}
