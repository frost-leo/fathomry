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

package i18n

import (
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// CoreComponents explicitly gathers the three foundation owners. Locale files
// remain in their owning packages and are discovered by Prepare, not listed here.
// The authored baseline is English; runtime preferences are independently chosen.
// Callers may append component bundles before preparing their complete catalog.
func CoreComponents() []Component {
	return []Component{
		{Module: "fathomry", Name: "failure", BaseLocale: "en", Resources: failure.Resources(), Directory: "resources", Definitions: failure.Definitions()},
		{Module: "fathomry", Name: "settings", BaseLocale: "en", Resources: settings.Resources(), Directory: "resources", Definitions: settings.Definitions()},
		{Module: "fathomry", Name: "i18n", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()},
	}
}
