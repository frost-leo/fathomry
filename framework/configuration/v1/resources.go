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

package configuration

import (
	"embed"
	"io/fs"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/resource/v1"

	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed resources/*.json
var resources embed.FS

// Resources returns explicit component-owned English and zh-CN resources.
func Resources() fs.FS { return resources }

// Component returns detached metadata for explicit catalog preparation.
// It installs no catalog, locale or process default.
func Component() i18n.Component {
	return i18n.Component{Module: "fathomry", Name: "configuration", BaseLocale: "en",
		Resources: Resources(), Directory: "resources", Definitions: Definitions()}
}

// Components returns the configuration-loading profile's explicit metadata,
// excluding i18n.CoreComponents. It opens no configuration sources or resources.
func Components() []i18n.Component {
	return []i18n.Component{
		Component(),
		dependencyComponent(resource.Resources(), resource.Definitions()),
		dependencyComponent(adapters.Resources(), adapters.Definitions()),
		dependencyComponent(configsource.Resources(), configsource.Definitions()),
		dependencyComponent(viper.Resources(), viper.Definitions()),
		dependencyComponent(nacos.Resources(), nacos.Definitions()),
	}
}

func dependencyComponent(resources fs.FS, definitions []failure.Definition) i18n.Component {
	return i18n.Component{Module: definitions[0].Module, Name: definitions[0].Component, BaseLocale: "en",
		Resources: resources, Directory: "resources", Definitions: definitions}
}
