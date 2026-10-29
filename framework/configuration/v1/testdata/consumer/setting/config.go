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

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

// Config belongs to this independent project, not the configuration library.
type Config struct {
	Application Application `json:"application"`
	Database    Database    `json:"database"`
	Cache       Cache       `json:"cache"`
}

func Schema() configsource.Schema[Config] {
	return configsource.Schema[Config]{Version: 1, Validate: func(ctx context.Context, value Config) error {
		if value.Application.Workers < 1 {
			return errors.New("workers must be positive")
		}
		if err := value.Database.Validate(); err != nil {
			return err
		}
		if err := value.Cache.Validate(); err != nil {
			return err
		}
		return ctx.Err()
	}}
}
