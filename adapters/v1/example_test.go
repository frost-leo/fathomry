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

package adapters_test

import (
	"context"
	"fmt"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func ExampleEndpoint_Run() {
	ctx := context.Background()
	runtime, err := adapters.New(ctx, adapters.Options{})
	if err != nil {
		panic(err)
	}
	defer runtime.Close(ctx)
	inbox, err := adapters.NewInbox[int](adapters.EvidenceOptions{})
	if err != nil {
		panic(err)
	}
	endpoint, err := adapters.Bind(runtime, adapters.Declaration[int]{
		Copy: func(value int) int { return value }, Evidence: inbox,
	})
	if err != nil {
		panic(err)
	}
	receipt, err := endpoint.Run(ctx, adapters.Request{Operation: "example.read", WorkBytes: 64, EvidenceBytes: 64},
		func(call *adapters.Call[int]) {
			if err := call.Resolve(adapters.Outcome[int]{Value: 7, Present: true}); err != nil {
				panic(err)
			}
		})
	if err != nil {
		panic(err)
	}
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		panic(err)
	}
	if err := result.Err(); err != nil {
		panic(err)
	}
	value, present := result.ValueCopy()
	fmt.Println(value, present, result.Info().Released)
	err = inbox.DeliverOne(ctx, func(_ context.Context, evidence adapters.Snapshot[int]) error {
		value, _ := evidence.ValueCopy()
		fmt.Println("received:", value)
		return nil
	})
	if err != nil {
		panic(err)
	}
	status, err := inbox.Inspect()
	if err != nil {
		panic(err)
	}
	fmt.Println("outstanding:", status.Outstanding)
	// Output:
	// 7 true true
	// received: 7
	// outstanding: 0
}
