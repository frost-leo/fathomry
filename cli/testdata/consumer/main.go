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

	"github.com/frost-leo/fathomry/cli"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "run" {
		status, _ := cli.Run(context.Background(), os.Args[2:], cli.Streams{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr})
		os.Exit(status)
	}
	cli.Main()
}
