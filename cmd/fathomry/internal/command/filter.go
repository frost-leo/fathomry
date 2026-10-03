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

package command

import (
	"strings"

	"github.com/frost-leo/fathomry/failure/v1"
)

// OwnerFilter selects exact namespaces independently; zero selects all owners.
type OwnerFilter struct {
	Module    string
	Component string
}

func (filter OwnerFilter) Validate() error {
	for _, item := range []struct {
		value string
		limit int
	}{{filter.Module, 128}, {filter.Component, 64}} {
		if item.value != "" && (len(item.value) > item.limit || !failure.Identifier(item.value+".filter").Valid()) {
			return Fail(ErrUsage)
		}
	}
	return nil
}

func (filter OwnerFilter) Match(module, component string) bool {
	return (filter.Module == "" || filter.Module == module) &&
		(filter.Component == "" || filter.Component == component)
}

// RequireMatch refuses unknown explicit owners, not an empty keyword result.
func (filter OwnerFilter) RequireMatch(found bool) error {
	if !found && (filter.Module != "" || filter.Component != "") {
		return Fail(ErrNotFound)
	}
	return nil
}

// ContainsQuery matches a pre-lowercased literal query, never a regular expression.
func ContainsQuery(query string, fields ...string) bool {
	for _, field := range fields {
		if strings.Contains(strings.ToLower(field), query) {
			return true
		}
	}
	return false
}
