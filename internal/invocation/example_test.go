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

package invocation_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func ExampleCall_Resolve() {
	type settings struct{}
	type evidence struct{ Rows, UnknownRows int }

	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1},
		resource.Input{Identity: resource.Identity{Provider: "example.local", Name: "output"}, Format: 1})
	if err != nil {
		panic(err)
	}
	// This local fixture demonstrates contracts, not SDK or service support.
	selected := resource.WithLimits(resource.Select(prepared, func(context.Context, settings) (resource.Resource[struct{}], error) {
		return resource.Resource[struct{}]{Acquired: true, Capability: struct{}{},
			Release: func(context.Context) resource.ReleaseResult {
				return resource.ReleaseResult{Quiescent: true, Released: true}
			}}, nil
	}), resource.Limits{Active: 1, Bytes: 1024, MaxLeases: 1})
	ctx := context.Background()
	assembly, err := resource.Assemble(ctx, ctx, "application", selected)
	if err != nil {
		panic(err)
	}
	defer assembly.Close(ctx)
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		panic(err)
	}
	inbox, err := invocation.NewInbox[evidence](1, 128)
	if err != nil {
		panic(err)
	}
	call, err := invocation.Begin(ctx, access, invocation.Request{
		Name: "write", Shape: invocation.Stream, Bytes: 1024, EvidenceBytes: 128,
		Correlation: fault.Correlation{Call: "call-1"},
		Admission:   invocation.Budget{Limit: time.Second},
	}, inbox, nil)
	if err != nil {
		panic(err)
	}

	call.Resolve(invocation.Outcome[evidence]{Present: true, Value: evidence{Rows: 2, UnknownRows: 1},
		Primary: errors.New("partial technical failure")})
	early, err := call.Receipt().Wait(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("partial rows:", early.Outcome.Value.Rows, "unknown:", early.Outcome.Value.UnknownRows, "final:", early.Final)

	cleanup := errors.New("cleanup warning after confirmed local release")
	call.Finish(cleanup)
	call.Release()
	final, err := call.Receipt().WaitReleased(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("cleanup failure:", errors.Is(final.Outcome.Cleanup, cleanup), "released:", final.Released)

	delivery, err := inbox.Next(ctx)
	if err != nil {
		panic(err)
	}
	recorded, err := delivery.Receipt().WaitReleased(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("independent evidence:", recorded.Outcome.Value == final.Outcome.Value)
	// The framework must handle required evidence before relinquishing this slot.
	// Release itself does not write or acknowledge any durable storage.
	if err := delivery.Release(); err != nil {
		panic(err)
	}
	// Output:
	// partial rows: 2 unknown: 1 final: false
	// cleanup failure: true released: true
	// independent evidence: true
}
