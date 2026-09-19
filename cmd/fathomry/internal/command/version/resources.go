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

package version

import (
	_ "embed"

	"github.com/frost-leo/fathomry/i18n"
	"github.com/frost-leo/fathomry/version/presentation"
)

//go:embed locales/en.json
var english string

//go:embed locales/zh-Hans.json
var chinese string

// Resources composes version's own wording with the public version presenter.
func Resources() []i18n.Resource {
	resources := presentation.Resources()
	return append(resources,
		i18n.Resource{Name: "fathomry/cli/version/en.json", Data: []byte(english)},
		i18n.Resource{Name: "fathomry/cli/version/zh-Hans.json", Data: []byte(chinese)},
	)
}
