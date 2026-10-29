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

package framework

import (
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// Catalogs is a caller-owned pair of immutable catalogs. Editing the pair does
// not mutate either catalog or install it in a process default.
type Catalogs struct {
	Errors   *failure.Catalog
	Messages *i18n.Catalog
}

// Prepare validates bounded component metadata using the existing engines.
// Resource filesystems are read explicitly; callers must not mutate inputs or
// their resource files during this call. Failure returns a zero pair.
func Prepare(components ...i18n.Component) (Catalogs, error) {
	messages, err := i18n.Prepare(components...)
	if err != nil {
		return Catalogs{}, err
	}
	// Successful i18n admission has already bounded the combined definition set.
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	errors, err := failure.Prepare(definitions...)
	if err != nil {
		return Catalogs{}, err
	}
	return Catalogs{Errors: errors, Messages: messages}, nil
}
