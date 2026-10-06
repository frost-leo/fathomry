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

import "reflect"

// Contains proves identity-based containment within limit visited nodes. It
// intentionally does not call foreign Is/Error methods. A non-comparable target
// or exhausted traversal is not proof of absence; callers retain unmatched
// originals rather than discarding evidence based on an incomplete search.
func Contains(original, target error, limit int) bool {
	if target == nil {
		return original == nil
	}
	if !reflect.ValueOf(target).Comparable() {
		return false
	}
	remaining := limit
	var visit func(error) bool
	visit = func(err error) bool {
		if err == nil || remaining <= 0 {
			return false
		}
		remaining--
		if err == target {
			return true
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range value.Unwrap() {
				if remaining <= 0 {
					break
				}
				if visit(child) {
					return true
				}
			}
		case interface{ Unwrap() error }:
			return visit(value.Unwrap())
		}
		return false
	}
	return visit(original)
}
