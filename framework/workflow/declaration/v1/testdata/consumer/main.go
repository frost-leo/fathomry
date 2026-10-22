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
	"io"
	"os"
	"os/signal"
	"syscall"

	"example.org/workflow-consumer/health"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := export(ctx, os.Stdout)
	stop()
	if err != nil {
		os.Exit(1)
	}
}

func export(ctx context.Context, output io.Writer) error {
	bundle, err := wf.Compile(ctx, health.Declarations())
	if err != nil {
		return err
	}
	raw, err := bundle.BytesCopy()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	count, err := output.Write(raw)
	if err == nil && count != len(raw) {
		return io.ErrShortWrite
	}
	if err == nil {
		return ctx.Err()
	}
	return err
}
