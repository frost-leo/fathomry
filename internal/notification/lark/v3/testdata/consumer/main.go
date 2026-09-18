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
	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	"github.com/frost-leo/fathomry/internal/resource"
)

func main() {
	options := lark.OptionsV1{Name: "consumer", Profile: "application", AppID: "fixture-app", AppSecret: "fixture-secret", BaseURL: "https://no-network.invalid"}
	selected, err := lark.Select(options)
	if err != nil {
		os.Exit(1)
	}
	selected = resource.WithLimits(selected, lark.LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil {
		os.Exit(2)
	}
	inbox, err := invocation.NewInbox[lark.Result](1, lark.EvidenceBytesV1(options))
	if err != nil {
		os.Exit(3)
	}
	client, err := lark.Bind(assembly, selected, inbox, nil)
	if err != nil {
		os.Exit(4)
	}
	if err = assembly.Close(context.Background()); err != nil {
		os.Exit(5)
	}
	facts, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/larksuite/oapi-sdk-go/v3"}})
	if err != nil || len(facts.SDKs) != 1 {
		os.Exit(6)
	}
	_ = json.NewEncoder(os.Stdout).Encode(struct {
		Go, SDK, Mode string
		Closed        bool
	}{runtime.Version(), facts.SDKs[0].Version.Value, client.Profile().SDKMode, true})
}
