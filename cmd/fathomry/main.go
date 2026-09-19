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

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/root"
)

func main() {
	status := runProcess(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, root.Run)
	os.Exit(status)
}

func runProcess(args []string, in io.Reader, out, errOut io.Writer,
	run func(context.Context, []string, io.Reader, io.Writer, io.Writer) int,
) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals, restoreInterrupts, stop := notifySignals()
	defer stop()
	done := make(chan struct{})
	stopped := make(chan struct{})
	signalStatus := 0
	go func() {
		defer close(stopped)
		select {
		case received := <-signals:
			signalStatus = exitForSignal(received)
			restoreInterrupts()
			cancel()
		case <-done:
		}
	}()
	status := run(ctx, args, in, out, errOut)
	close(done)
	<-stopped
	if signalStatus != 0 && status != 1 && status != 2 {
		return signalStatus
	}
	return status
}
