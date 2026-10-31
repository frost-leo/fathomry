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

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"example.org/fathomry-startup-consumer/internal/bootstrap"
	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
)

func main() { os.Exit(run()) }
func run() int {
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return cli.ExitCode(bootstrap.Run(ctx, os.Args[1:], cli.Options{
		Input: os.Stdin, Output: os.Stdout, ErrorOutput: os.Stderr, Language: os.Getenv("FATHOMRY_LANG"),
	}))
}
