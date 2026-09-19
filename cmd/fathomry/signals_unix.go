//go:build unix

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
	"os"
	"os/signal"
	"syscall"
)

func notifySignals() (<-chan os.Signal, func(), func()) {
	interrupts := make(chan os.Signal, 2)
	pipes := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM)
	signal.Notify(pipes, syscall.SIGPIPE)
	return interrupts, func() { signal.Stop(interrupts) }, func() {
		signal.Stop(interrupts)
		signal.Stop(pipes)
	}
}

func exitForSignal(received os.Signal) int {
	if received == syscall.SIGTERM {
		return 143
	}
	return 130
}
