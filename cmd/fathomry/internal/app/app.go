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

// Package app connects the official executable to the minimal public CLI.
package app

import (
	"context"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
)

// Run owns one finite CLI invocation without reading process globals.
func Run(ctx context.Context, args []string, options command.Options) error {
	return cli.Run(ctx, args, cli.Options{
		Input: options.Input, Output: options.Output, ErrorOutput: options.ErrorOutput,
		Language: options.Language, CleanupTimeout: options.CleanupTimeout,
	})
}
