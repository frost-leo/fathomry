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

// Package configcheck is an independent example of capability-owned machine
// detail. Its fixture codes and limits are not a framework configuration catalog.
package configcheck

import (
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/failure"
)

const InvalidInput failure.Code = "example.config.invalid_input"
const MaxViolations = 8

type occurrence = failure.Error

// Violation describes a field and a stable rule, never its private input value.
// This fixture accepts the field "port" or "enabled" and rule "type" or "range".
type Violation struct {
	Field string
	Rule  string
}

// Validation owns required structured detail, independently of optional diagnostics.
// Only constructor-validated detail is complete. Zero is an invalid occurrence.
type Validation struct {
	occurrence
	violations []Violation
	complete   bool
}

// NewValidation preserves InvalidInput even if required detail is refused.
// It copies a nonempty collection of at most MaxViolations valid entries, including
// string backing bytes. Input must not be concurrently mutated during this call.
// No native cause is exposed by this capability.
func NewValidation(violations []Violation) Validation {
	result := Validation{occurrence: failure.New(InvalidInput, nil)}
	if len(violations) == 0 || len(violations) > MaxViolations {
		return result
	}
	for _, violation := range violations {
		if (violation.Field != "port" && violation.Field != "enabled") ||
			(violation.Rule != "type" && violation.Rule != "range") {
			return result
		}
	}
	result.violations = make([]Violation, len(violations))
	for index, violation := range violations {
		result.violations[index] = Violation{
			Field: strings.Clone(violation.Field), Rule: strings.Clone(violation.Rule),
		}
	}
	result.complete = true
	return result
}

// Violations returns an independent ordered collection and its completeness.
// False means required detail is unavailable, NOT that validation found no problems;
// callers must refuse any action requiring the complete list. Identity survives.
func (err Validation) Violations() ([]Violation, bool) {
	return slices.Clone(err.violations), err.complete
}
