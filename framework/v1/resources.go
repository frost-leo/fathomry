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
	"embed"
	"io/fs"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

//go:embed resources/*.json
var resources embed.FS

// Resources supplies immutable assembly-owned message resources.
func Resources() fs.FS { return resources }

// CoreComponents explicitly gathers the implemented shared public owners, never
// concrete SDKs. Applications append only their selected component bundles.
func CoreComponents() []i18n.Component {
	return append(i18n.CoreComponents(),
		i18n.Component{Module: "fathomry", Name: "resource", BaseLocale: "en", Resources: resource.Resources(), Directory: "resources", Definitions: resource.Definitions()},
		i18n.Component{Module: "fathomry", Name: "operation", BaseLocale: "en", Resources: adapters.Resources(), Directory: "resources", Definitions: adapters.Definitions()},
		i18n.Component{Module: "fathomry", Name: "assembly", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()},
	)
}
