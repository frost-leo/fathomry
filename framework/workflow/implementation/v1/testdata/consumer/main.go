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

	health "example.org/binding-consumer/workflows/example/server_health/v1"
	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

func main() {
	ctx := context.Background()
	registered := health.Registration()
	project := cli.Project{Name: "binding-consumer", Workflows: []cli.Workflow{registered}}
	handled, err := cli.Handle(ctx, os.Args[1:], project, cli.Options{Input: os.Stdin, Output: os.Stdout, ErrorOutput: os.Stderr})
	if !handled {
		value, problem := invokeCPU(ctx, registered)
		err = problem
		if err == nil {
			_, err = os.Stdout.Write(value)
		}
	}
	if err != nil {
		os.Exit(cli.ExitCode(err))
	}
}

func invokeCPU(ctx context.Context, registered cli.Workflow) ([]byte, error) {
	bundle, err := declaration.Compile(ctx, registered.Declaration)
	if err != nil {
		return nil, err
	}
	bound, err := implementation.Prepare(ctx, bundle, declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}, registered.Implementations[0].Bindings)
	if err != nil {
		return nil, err
	}
	result, err := bound.Invoke(ctx, "read_cpu", []byte(`{"target":"local"}`))
	return result.Output, err
}
