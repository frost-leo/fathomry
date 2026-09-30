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

package component

import (
	"slices"

	"github.com/frost-leo/fathomry/settings/v1"
)

// Preferences is owned by this example component, not the project outer struct.
type Preferences struct {
	Language  string   `json:"language"`
	Fallbacks []string `json:"fallbacks"`
}

// Clone isolates this component's mutable data.
func Clone(value Preferences) Preferences {
	value.Fallbacks = slices.Clone(value.Fallbacks)
	return value
}

// Read selects one section from an already captured operation view.
func Read(view settings.View) (Preferences, bool, error) {
	return settings.Read(view, "/application/i18n", Clone)
}

// Language obtains the application preference without a locale argument or any
// knowledge of the project's complete type. This fixture is not an i18n renderer.
func Language() (string, bool, error) {
	view, err := settings.Default()
	if err != nil {
		return "", false, err
	}
	preferences, found, err := Read(view)
	return preferences.Language, found, err
}
