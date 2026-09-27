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

	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed resources/*.json
var resources embed.FS

// Sources returns owner-local resources for explicit host composition.
func Sources() ([]i18n.Source, error) {
	var sources []i18n.Source
	for _, name := range []string{"en", "zh-cn"} {
		data, err := resources.ReadFile("resources/" + name + ".json")
		if err != nil {
			return nil, err
		}
		sources = append(sources, i18n.Source{Name: "project." + name, Data: data})
	}
	return sources, nil
}

var requiredText = [...]string{"new", "module", "source", "created", "notStarted", "partial", "completeDelivery", "sourceUnavailable", "destinationExists"}
