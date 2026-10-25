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
	"encoding/json"
	"os"

	health "example.org/binding-consumer/workflows/example/server_health/v1"
	"example.org/binding-consumer/workflows/example/server_health/v1/nodes/sample"
	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
)

func main() {
	ctx := context.Background()
	project := cli.Project{Name: "binding-consumer", Workflows: []cli.Workflow{health.Registration()}}
	handled, err := cli.Handle(ctx, os.Args[1:], project, cli.Options{Input: os.Stdin, Output: os.Stdout, ErrorOutput: os.Stderr})
	if !handled {
		value, problem := sample.CPU(ctx, sample.Input{Target: "local"})
		err = problem
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(value)
		}
	}
	if err != nil {
		os.Exit(cli.ExitCode(err))
	}
}
