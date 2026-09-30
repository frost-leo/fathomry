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

// Command fathomry provides official offline error and translation catalogs.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/app"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
)

func main() { os.Exit(run()) }
func run() int {
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signalContext()
	defer stop()
	err := app.Run(ctx, os.Args[1:], command.Options{Input: os.Stdin, Output: os.Stdout,
		ErrorOutput: os.Stderr, Language: os.Getenv("FATHOMRY_LANG")})
	return command.ExitCode(err)
}

func signalContext() (context.Context, func()) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		stop()
		close(done)
	}()
	return ctx, func() { stop(); <-done }
}
