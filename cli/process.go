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

package cli

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

// Main owns process I/O, SIGPIPE, INT/TERM and exit, and does not return.
// The first observed INT/TERM requests cancellation; a later observed termination
// signal exits immediately without waiting for I/O or cleanup. Forced exit does not
// prove cleanup. Use Run in applications that already own process policy.
func Main() {
	os.Exit(process(os.Args[1:], Streams{os.Stdin, os.Stdout, os.Stderr}, Run))
}

func process(args []string, streams Streams, execute func(context.Context, []string, Streams) (int, error)) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	termination := make(chan os.Signal, 2)
	pipe := make(chan os.Signal, 1)
	signal.Notify(pipe, syscall.SIGPIPE)
	signal.Notify(termination, os.Interrupt, syscall.SIGTERM)
	var first atomic.Int32
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		for {
			select {
			case observed := <-termination:
				status := int32(130)
				if observed == syscall.SIGTERM {
					status = 143
				}
				if first.CompareAndSwap(0, status) {
					cancel()
				} else {
					os.Exit(int(status))
				}
			case <-stop:
				return
			}
		}
	}()
	defer func() {
		signal.Stop(termination)
		signal.Stop(pipe)
		close(stop)
		<-joined
	}()
	status, _ := execute(ctx, args, streams)
	if status == 130 && first.Load() == 143 {
		return 143
	}
	return status
}
