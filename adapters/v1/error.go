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

package adapters

import (
	"github.com/frost-leo/fathomry/failure/v1"
)

// Details is direct local operation evidence. Zero sequence/parent means
// unspecified; Pending is occurrence-time local responsibility, not rollback.
type Details struct {
	Sequence uint64
	Parent   uint64
	Pending  bool
}

func failureOf(code failure.Code, operation, scope string, details Details, causes ...error) error {
	value, err := failure.NewDetailed(definition(code), failure.Location{Operation: operation, Instance: scope}, details, func(value Details) Details { return value }, causes...)
	if err != nil {
		return err
	}
	return value
}
