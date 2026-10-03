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
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed resources/*.json
var resources embed.FS

// Resources supplies immutable configuration-owned localization resources.
func Resources() fs.FS { return resources }

// Components supplies all definitions reachable through the supported scenarios.
// It constructs no providers or clients and reads no deployment settings.
func Components() []i18n.Component {
	return append(framework.CoreComponents(),
		i18n.Component{Module: "fathomry", Name: "configuration_data", BaseLocale: "en", Resources: configsource.Resources(), Directory: "resources", Definitions: configsource.Definitions()},
		i18n.Component{Module: "fathomry", Name: "configuration", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()},
		i18n.Component{Module: "fathomry", Name: "configsource_viper", BaseLocale: "en", Resources: viper.Resources(), Directory: "resources", Definitions: viper.Definitions()},
		i18n.Component{Module: "fathomry", Name: "configsource_nacos", BaseLocale: "en", Resources: nacos.Resources(), Directory: "resources", Definitions: nacos.Definitions()},
	)
}
