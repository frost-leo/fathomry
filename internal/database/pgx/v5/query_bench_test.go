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

package pgx

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
)

// Direct shared-core invocation keeps validation, admission, budgets, immutable result,
// native pooling/cleanup and independent evidence as the integration. Removing
// these obligations would compare different guarantees, not wrapper overhead.
func directCoreQuery(database *Database, id, sql string, args []any) (*invocation.Receipt[Result], error) {
	if err := validStatement(sql, args); err != nil {
		return nil, err
	}
	ctx := context.Background()
	call, err := database.beginCall(ctx, correlation(id), "query", invocation.Finite, nil)
	if err != nil {
		return nil, err
	}
	budget, cancel, err := (invocation.Budget{Limit: database.owner.settings.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	defer cancel()
	handle, err := database.owner.take(budget)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	if budget.Err() != nil {
		call.Complete(invocation.Outcome[Result]{Primary: failure(ErrQuery, "query", budget.Err(), context.Cause(budget)), Cleanup: database.owner.give(handle)})
		return call.Receipt(), nil
	}
	_, _ = call.Attempt()
	result, primary, cleanup := consume(budget, handle.Value().native, database.owner.settings, sql, args, true)
	cleanup = failureOrNil(ErrCleanup, "consume", cleanup, database.owner.give(handle))
	call.Complete(invocation.Outcome[Result]{Value: result, Present: true, Primary: primary, Cleanup: cleanup})
	return call.Receipt(), nil
}
func BenchmarkBoundedQuery(b *testing.B) {
	for _, size := range []int{64, 4096, 256 << 10} {
		for _, mode := range []string{"direct-shared-core", "integration"} {
			b.Run(fmt.Sprintf("%d/%s", size, mode), func(b *testing.B) {
				peer := newProtocolPeer(b, false)
				fixture := bindFixture(b, peer.options(), 1)
				payload := strings.Repeat("x", size)
				run := func() error {
					var receipt *invocation.Receipt[Result]
					var err error
					if mode == "direct-shared-core" {
						receipt, err = directCoreQuery(fixture.database, "benchmark", "SELECT $1::text", []any{payload})
					} else {
						receipt, err = fixture.database.Query(context.Background(), correlation("benchmark"), "SELECT $1::text", payload)
					}
					if err != nil {
						return err
					}
					result, _ := receipt.Result()
					if result.Err() != nil {
						return result.Err()
					}
					row, err := result.Outcome.Value.First()
					if err != nil {
						return err
					}
					if string(row.ValuesCopy()[0]) != payload {
						b.Fatal("useful benchmark result changed")
					}
					record, err := fixture.inbox.Next(context.Background())
					if err != nil {
						return err
					}
					observed, _ := record.Receipt().Result()
					if !observed.Final || !observed.Released || !observed.Outcome.Value.Complete() {
						b.Fatal("benchmark evidence guarantee changed")
					}
					return record.Release()
				}
				if err := run(); err != nil {
					b.Fatal(err)
				}
				var samples [128]time.Duration
				count := 0
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index := count
					count++
					start := time.Now()
					if err := run(); err != nil {
						b.Fatal(err)
					}
					if index < len(samples) {
						samples[index] = time.Since(start)
					}
				}
				b.StopTimer()
				used := min(count, len(samples))
				slices.Sort(samples[:used])
				if used > 0 {
					b.ReportMetric(float64(samples[(used-1)*50/100]), "sample-p50-ns")
					b.ReportMetric(float64(samples[(used-1)*95/100]), "sample-p95-ns")
				}
				if fixture.inbox.Usage() != (invocation.InboxUsage{}) {
					b.Fatal("benchmark leaked evidence")
				}
			})
		}
	}
}
