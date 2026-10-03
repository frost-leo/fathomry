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

package project

import (
	"embed"
	"io/fs"

	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed resources/*.json
var messages embed.FS

// Resources exposes the embedded offline message resources.
func Resources() fs.FS { return messages }

// Component declares project-creation errors and translations for composition.
func Component() i18n.Component {
	return i18n.Component{Module: "fathomry", Name: "project_creation", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()}
}
