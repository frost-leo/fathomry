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

package failure

import (
	"fmt"
	"io"
	"log/slog"
)

// MaxConditionBytes bounds an accepted condition in bytes.
const MaxConditionBytes = 128

// Condition is a comparable, exact, owner-qualified identity and an errors.Is
// target. Use code-owned constants, such as "example.source.absent"; the final
// dot-separated component names a condition in the preceding owner's namespace.
// Valid unfamiliar conditions need no registration. A condition is not an
// occurrence, a resource key, a retry decision or a proof of namespace authority.
// Never repurpose an identity or use dynamic sensitive/high-cardinality labels.
// The zero value is invalid; direct string conversion does not validate it.
type Condition string

// Valid checks 1..128 ASCII bytes and at least two dot-separated components.
// Each component starts with a lowercase letter, followed by lowercase letters,
// digits, underscores or hyphens. There is no normalization or registry lookup.
func (condition Condition) Valid() bool {
	if len(condition) == 0 || len(condition) > MaxConditionBytes {
		return false
	}
	start, qualified := true, false
	for index := 0; index < len(condition); index++ {
		char := condition[index]
		if char == '.' {
			if start {
				return false
			}
			start, qualified = true, true
			continue
		}
		if start {
			if char < 'a' || char > 'z' {
				return false
			}
			start = false
		} else if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return qualified && !start
}

// Error returns the exact valid code or a fixed invalid marker, never rejected
// input. Text equality is not identity with an unrelated error.
func (condition Condition) Error() string {
	if !condition.Valid() {
		return "failure: invalid condition"
	}
	return string(condition)
}

// Format emits only Error's safe baseline when fmt dispatches to it; q quotes
// it. Flags, width and precision do not expand that projection. fmt handles
// type/pointer inspection (%T/%p) and malformed-format diagnostics itself;
// those paths can bypass Format and expose raw invalid Condition text.
func (condition Condition) Format(state fmt.State, verb rune) {
	formatText(state, verb, condition.Error())
}

// LogValue emits the same locale-independent baseline.
func (condition Condition) LogValue() slog.Value {
	return slog.StringValue(condition.Error())
}

func formatText(state fmt.State, verb rune, text string) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", text)
	} else {
		_, _ = io.WriteString(state, text)
	}
}
