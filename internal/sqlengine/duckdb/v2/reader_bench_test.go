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

package duckdb

import (
	"context"
	"strings"
	"testing"
)

// Both paths complete the same finite ordered scalar workload and explicitly
// receive/release all independent evidence. Larger-capacity proof is a test,
// not a comparison against a finite route that cannot complete the workload.
func BenchmarkReaderComparison(b *testing.B) {
	const query = "SELECT i,repeat('x',128) FROM range(4096) r(i) ORDER BY i"
	expected := strings.Repeat("x", 128)
	for _, incremental := range []bool{false, true} {
		name := "snapshot"
		if incremental {
			name = "incremental"
		}
		b.Run(name, func(b *testing.B) {
			fixture := openFixture(b, OptionsV1{ReaderChunkRows: 256})
			check := func(rows [][]any, count *int64) {
				for _, row := range rows {
					if row[0] != *count || row[1] != expected {
						b.Fatal("comparison exact-value oracle failed")
					}
					*count++
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var count int64
				if !incremental {
					receipt, err := fixture.database.Run(context.Background(), context.Background(), fixture.id(), Request{Mode: Query, SQL: query})
					if err != nil {
						b.Fatal(err)
					}
					result, err := receipt.WaitReleased(context.Background())
					if err != nil || result.Err() != nil {
						b.Fatal("snapshot comparison failed", err, result.Err())
					}
					progress := result.Outcome.Value.Snapshot()
					if !progress.Steps[0].Complete || !progress.ConnectionClosed {
						b.Fatal("snapshot comparison incomplete")
					}
					check(progress.Steps[0].Rows, &count)
					record, err := fixture.inbox.Next(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if err := record.Release(); err != nil {
						b.Fatal(err)
					}
				} else {
					reader, receipt, err := fixture.database.Read(context.Background(), context.Background(), fixture.id(), Request{Mode: Query, SQL: query})
					if err != nil {
						b.Fatal(err)
					}
					root, err := fixture.inbox.Next(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					for {
						chunk, err := reader.Next(context.Background(), fixture.id())
						if err != nil {
							b.Fatal(err)
						}
						result, err := chunk.WaitReleased(context.Background())
						if err != nil || result.Err() != nil {
							b.Fatal("reader comparison failed", err, result.Err())
						}
						progress := result.Outcome.Value.Snapshot()
						check(progress.Steps[0].Rows, &count)
						record, err := fixture.inbox.Next(context.Background())
						if err != nil {
							b.Fatal(err)
						}
						if err := record.Release(); err != nil {
							b.Fatal(err)
						}
						if progress.Steps[0].Complete {
							break
						}
					}
					result, err := receipt.WaitReleased(context.Background())
					if err != nil || result.Err() != nil || !result.Outcome.Value.Snapshot().ConnectionClosed {
						b.Fatal("reader cleanup comparison failed")
					}
					if err := root.Release(); err != nil {
						b.Fatal(err)
					}
				}
				if count != 4096 || fixture.inbox.Usage().Outstanding != 0 {
					b.Fatal("comparison workload/evidence incomplete")
				}
			}
		})
	}
}
