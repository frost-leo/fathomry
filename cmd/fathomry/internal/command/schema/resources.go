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
package schema

import (
	"embed"
	"io/fs"

	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed resources/*.json
var messages embed.FS

// Resources supplies inert component-owned localization resources.
func Resources() fs.FS { return messages }

// Components declares schema-group presentation without a second error owner.
func Components() []i18n.Component {
	return []i18n.Component{
		{Module: "fathomry", Name: "schema_commands", BaseLocale: "en", Resources: Resources(), Directory: "resources"},
	}
}
