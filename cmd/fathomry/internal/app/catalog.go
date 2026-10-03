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

package app

import (
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/errorcatalog"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/messages"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project"
	"github.com/frost-leo/fathomry/failure/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// catalogs is offline metadata composition, not Framework runtime construction.
func catalogs() (command.Catalogs, error) {
	components := append(configuration.Components(),
		command.Component(),
		errorcatalog.Component(),
		messages.Component(),
		project.Component(),
	)
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	errors, err := failure.Prepare(definitions...)
	if err != nil {
		return command.Catalogs{}, err
	}
	translations, err := i18n.Prepare(components...)
	if err != nil {
		return command.Catalogs{}, err
	}
	return command.Catalogs{Errors: errors, Messages: translations}, nil
}
