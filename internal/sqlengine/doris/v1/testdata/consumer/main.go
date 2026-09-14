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
	"runtime"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	doris "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
)

func main() {
	options := doris.OptionsV1{Name: "consumer", HTTPOrigins: []string{"http://127.0.0.1:1"}, Database: "gh42", User: "synthetic", Plaintext: true}
	selected, err := doris.Select(options)
	if err != nil {
		os.Exit(1)
	}
	selected = resource.WithLimits(selected, doris.LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil {
		os.Exit(1)
	}
	inbox, err := invocation.NewInbox[doris.Result](1, 1<<30)
	if err != nil {
		os.Exit(1)
	}
	_, err = doris.Bind(assembly, selected, inbox, nil)
	if err != nil {
		os.Exit(1)
	}
	if err = assembly.Close(context.Background()); err != nil {
		os.Exit(1)
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/go-sql-driver/mysql", "filippo.io/edwards25519"}})
	if err != nil {
		os.Exit(1)
	}
	modules := map[string]string{}
	for _, module := range build.SDKs {
		modules[module.Path.Value] = module.Version.Value
	}
	if json.NewEncoder(os.Stdout).Encode(struct {
		Go                   string
		ConstructedAndClosed bool
		Modules              map[string]string
	}{runtime.Version(), true, modules}) != nil {
		os.Exit(1)
	}
}
