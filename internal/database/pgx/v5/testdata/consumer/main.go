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

// This maintainer executable verifies build, explicit preparation and local pool
// ownership. It deliberately performs no server query or public framework startup.
package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/frost-leo/fathomry/internal/compatibility"
	database "github.com/frost-leo/fathomry/internal/database/pgx/v5"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func main() {
	options := database.OptionsV1{Name: "consumer", Address: "127.0.0.1", Port: 1, Database: "fixture", User: "fixture",
		Password: "fixture", Plaintext: true, ParserHome: os.Getenv("HOME")}
	selected, err := database.Select(options)
	if err != nil {
		panic("consumer options failed")
	}
	selected = resource.WithLimits(selected, database.LimitsV1(options))
	owner, err := resource.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil {
		panic("consumer construction failed")
	}
	inbox, _ := invocation.NewInbox[database.Result](1, 128<<20)
	client, err := database.Bind(owner, selected, inbox, nil)
	if err != nil || client.Profile().SDKMode != "pgx-puddle-synchronous" {
		panic("consumer binding failed")
	}
	if err := owner.Close(context.Background()); err != nil {
		panic("consumer cleanup failed")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{
		"github.com/jackc/pgx/v5", "github.com/jackc/puddle/v2", "github.com/jackc/pgpassfile",
		"github.com/jackc/pgservicefile", "golang.org/x/sync", "golang.org/x/text"}})
	if err != nil {
		panic("consumer build inspection failed")
	}
	modules := make(map[string]string)
	for _, module := range build.SDKs {
		modules[module.Path.Value] = module.Version.Value
	}
	output := struct {
		Go                   string
		ConstructedAndClosed bool
		QueryExecuted        bool
		Modules              map[string]string
	}{
		Go: build.Go.Value, ConstructedAndClosed: true, Modules: modules}
	if json.NewEncoder(os.Stdout).Encode(output) != nil {
		panic("consumer report failed")
	}
}
