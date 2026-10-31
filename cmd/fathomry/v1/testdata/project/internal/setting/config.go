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

package setting

import (
	"context"
	"errors"

	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// Config is the complete schema owned by this independent project.
type Config struct {
	Application Application `json:"application"`
}

// Schema supplies immutable defaults and pure project validation.
func Schema() configuration.Schema[Config] {
	return configuration.Schema[Config]{Version: 1,
		Defaults: Config{Application: Application{Name: "consumer", I18n: i18n.Preferences{Locale: "en"}}},
		Validate: func(ctx context.Context, value Config) error {
			if value.Application.Name != "consumer" {
				return errors.New("project-identity-mismatch")
			}
			return errors.Join(value.Application.I18n.Validate(), ctx.Err())
		}}
}
