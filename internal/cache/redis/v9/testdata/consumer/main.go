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
	"runtime/debug"

	redis "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func main() {
	redis.DisableNativeLogging()
	options := redis.OptionsV1{Name: "consumer", Mode: "standalone", Addrs: []string{"127.0.0.1:1"}, Plaintext: true}
	selected, err := redis.Select(options)
	if err != nil {
		panic("selection failed")
	}
	selected = resource.WithLimits(selected, redis.LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil {
		panic("construction failed")
	}
	inbox, _ := invocation.NewInbox[redis.Result](1, 1<<40)
	if _, err := redis.Bind(assembly, selected, inbox, nil); err != nil {
		panic("binding failed")
	}
	if err := assembly.Close(context.Background()); err != nil {
		panic("release failed")
	}
	var sdk string
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, module := range build.Deps {
			if module.Path == "github.com/redis/go-redis/v9" && module.Replace == nil {
				sdk = module.Version
			}
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(struct {
		Go                   string
		ConstructedAndClosed bool
		QueryExecuted        bool
		SDK                  string
	}{runtime.Version(), true, false, sdk})
}
