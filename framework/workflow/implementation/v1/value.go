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

package implementation

import (
	"context"
	"unicode/utf8"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// Invocation limits apply independently to each direction. Values include the
// root, containers, null and each member name. Root wire depth is 1; pointers do
// not add depth. String bytes include names and count each serialized occurrence.
// These bounds do not cap handler allocations or encoder-internal buffering.
const (
	MaxInvocationBytes       = 16 << 20
	MaxInvocationDepth       = 32
	MaxInvocationValues      = 1 << 20
	MaxInvocationStringBytes = 16 << 20
)

type valueBudget struct {
	ctx         context.Context
	phase       string
	values      int
	stringBytes int
}

func (budget *valueBudget) charge(path string) error {
	if err := invocationCanceled(budget.ctx, budget.phase); err != nil {
		return err
	}
	budget.values++
	if budget.values > MaxInvocationValues {
		return budget.limit("values", path)
	}
	return nil
}
func (budget *valueBudget) enter(depth int, path string) error {
	if err := budget.charge(path); err != nil {
		return err
	}
	if depth > MaxInvocationDepth {
		return budget.limit("depth", path)
	}
	return nil
}
func (budget *valueBudget) text(value, path string) error {
	if len(value) > MaxInvocationStringBytes-budget.stringBytes {
		return budget.limit("strings", path)
	}
	budget.stringBytes += len(value)
	if !utf8.ValidString(value) {
		return budget.invalid("utf8", path)
	}
	return nil
}
func (budget *valueBudget) name(value, path string) error {
	if err := budget.charge(path); err != nil {
		return err
	}
	return budget.text(value, path)
}
func (budget *valueBudget) stringValue(value string, schema declaration.DataSchema, path string) error {
	if int64(len(value)) > *schema.MaxLength*utf8.UTFMax {
		return budget.invalid("string_length", path)
	}
	if err := budget.text(value, path); err != nil {
		return err
	}
	length := int64(utf8.RuneCountInString(value))
	if length < *schema.MinLength || length > *schema.MaxLength {
		return budget.invalid("string_length", path)
	}
	if len(schema.Enum) != 0 {
		found := false
		for _, allowed := range schema.Enum {
			if value == allowed {
				found = true
				break
			}
		}
		if !found {
			return budget.invalid("enum", path)
		}
	}
	return nil
}
func (budget *valueBudget) integer(value int64, schema declaration.DataSchema, path string) error {
	if value < *schema.Minimum || value > *schema.Maximum {
		return budget.invalid("integer_range", path)
	}
	return nil
}
func (budget *valueBudget) invalid(reason, path string, causes ...error) error {
	return invocationFailure(budget.phase, reason, path, ErrInvalid, causes...)
}
func (budget *valueBudget) limit(reason, path string) error {
	return invocationFailure(budget.phase, reason, path, ErrLimit)
}
