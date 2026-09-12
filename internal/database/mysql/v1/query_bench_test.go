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

package mysql

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
)

// Both modes use the same transaction, parameterized workload, transport bounds,
// immutable results and independently released evidence. Only preparation reuse
// changes. A local protocol peer measures neither MySQL throughput nor durability.
func BenchmarkPreparationReuse(b *testing.B) {
	for _, size := range []int{64, 4096, 256 << 10} {
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/reuse=%t", size, reuse), func(b *testing.B) {
				peer := newPeer(b, false, false)
				fixture := bindFixture(b, peer.options(), 3)
				ctx := context.Background()
				tx, _, err := fixture.db.Begin(ctx, correlation("benchmark-tx"), TxOptionsV1{})
				if err != nil || tx == nil {
					b.Fatal("benchmark transaction failed")
				}
				root, err := fixture.inbox.Next(ctx)
				if err != nil {
					b.Fatal(err)
				}
				var stmt *Statement
				var prepared *invocation.DeliveryRecord[Result]
				if reuse {
					stmt, _, err = tx.Prepare(ctx, correlation("benchmark-prepare"), "SELECT ?")
					if err != nil || stmt == nil {
						b.Fatal("benchmark preparation failed")
					}
					prepared, err = fixture.inbox.Next(ctx)
					if err != nil {
						b.Fatal(err)
					}
				}
				payload := strings.Repeat("x", size)
				run := func() {
					var receipt *invocation.Receipt[Result]
					var err error
					if reuse {
						receipt, err = stmt.Query(ctx, correlation("benchmark-query"), payload)
					} else {
						receipt, err = tx.Query(ctx, correlation("benchmark-query"), "SELECT ?", payload)
					}
					result := observe(b, receipt, err)
					row, rowErr := result.Outcome.Value.First()
					if result.Err() != nil || rowErr != nil || string(row.ValuesCopy()[0]) != payload || !result.Nested {
						b.Fatal("useful benchmark result or ownership guarantee changed")
					}
					record, err := fixture.inbox.Next(ctx)
					if err != nil {
						b.Fatal(err)
					}
					evidence, ready := record.Receipt().Result()
					if !ready || !evidence.Final || !evidence.Released || !evidence.Outcome.Value.Complete() {
						b.Fatal("benchmark independent evidence changed")
					}
					if err = record.Release(); err != nil {
						b.Fatal(err)
					}
				}
				run()
				var samples [128]time.Duration
				count := 0
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					start := time.Now()
					run()
					if count < len(samples) {
						samples[count] = time.Since(start)
					}
					count++
				}
				b.StopTimer()
				used := min(count, len(samples))
				slices.Sort(samples[:used])
				if used > 0 {
					b.ReportMetric(float64(samples[(used-1)*50/100]), "sample-p50-ns")
					b.ReportMetric(float64(samples[(used-1)*95/100]), "sample-p95-ns")
				}
				receipt, err := tx.Rollback(ctx)
				if result := observe(b, receipt, err); result.Err() != nil {
					b.Fatal(result.Err())
				}
				if prepared != nil {
					if err = prepared.Release(); err != nil {
						b.Fatal(err)
					}
				}
				if err = root.Release(); err != nil {
					b.Fatal(err)
				}
				if fixture.inbox.Usage() != (invocation.InboxUsage{}) {
					b.Fatal("benchmark leaked evidence")
				}
			})
		}
	}
}
