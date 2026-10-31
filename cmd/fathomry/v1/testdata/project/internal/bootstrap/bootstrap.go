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

package bootstrap

import (
	"context"
	"os"

	"example.org/fathomry-startup-consumer/internal/setting"
	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
)

// Run supplies the project's compiled declarations to Fathomry's shared entry.
func Run(ctx context.Context, args []string, options cli.Options) error {
	return cli.RunProject(ctx, args, cli.Project[setting.Config]{Schema: func(context.Context, string) (configuration.Schema[setting.Config], error) {
		return setting.Schema(), nil
	}, LookupEnv: os.LookupEnv}, options)
}
