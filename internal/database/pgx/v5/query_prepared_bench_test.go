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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/invocation"
)

// Both modes use the same retained transaction, text workload, copied results and
// independently released evidence. Establishment/finalization are verified outside
// steady-state timing. This local protocol peer does not measure server planning
// or real PostgreSQL throughput.
func BenchmarkTransactionalPreparation(b *testing.B) {
	for _, size := range []int{64, 4096, 256 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			ctx := context.Background()
			peer := newProtocolPeer(b, false)
			fixture := bindFixture(b, peer.options(), 3)
			tx := beginTransaction(b, fixture, "benchmark-root")
			var root *invocation.DeliveryRecord[Result]
			b.Cleanup(func() {
				receipt, err := tx.Rollback(ctx)
				result := operationResult(b, receipt, err)
				if result.Err() != nil || result.Outcome.Value.TransactionOutcome() != RollbackAcknowledged {
					b.Error("benchmark transaction cleanup failed", result.Err())
				}
				if root != nil {
					if err = root.Release(); err != nil {
						b.Error(err)
					}
				}
				if fixture.inbox.Usage() != (invocation.InboxUsage{}) || fixture.database.Stats().AcquiredResources() != 0 {
					b.Error("benchmark retained ownership or evidence")
				}
			})
			var err error
			root, err = fixture.inbox.Next(ctx)
			if err != nil {
				b.Fatal(err)
			}
			connection := tx.handle.Value()
			payload := strings.Repeat("x", size)
			for _, mode := range []string{"one-shot", "retained-prepared"} {
				b.Run(mode, func(b *testing.B) {
					var statement *Statement
					if mode == "retained-prepared" {
						statement, _, err = tx.Prepare(ctx, correlation("benchmark-prepare"), "SELECT $1::text")
						if err != nil || statement == nil {
							b.Fatal("benchmark preparation failed", err)
						}
						var prepared *invocation.DeliveryRecord[Result]
						b.Cleanup(func() {
							receipt, err := statement.Close(ctx)
							if result := operationResult(b, receipt, err); result.Err() != nil {
								b.Error("benchmark statement cleanup failed", result.Err())
							}
							if prepared != nil {
								if err = prepared.Release(); err != nil {
									b.Error(err)
								}
							}
						})
						prepared, err = fixture.inbox.Next(ctx)
						if err != nil {
							b.Fatal(err)
						}
					}
					check := func(result invocation.Result[Result], ready bool) error {
						if !ready || !result.Final || !result.Released || !result.Nested || !result.Outcome.Present || !result.Outcome.Value.Complete() {
							return errors.New("benchmark result/evidence contract changed")
						}
						if result.Err() != nil {
							return result.Err()
						}
						rows := result.Outcome.Value.RowsCopy()
						columns := result.Outcome.Value.ColumnsCopy()
						if len(rows) != 1 || len(columns) != 1 || columns[0].OID != 25 || columns[0].Name != "value" || result.Outcome.Value.RowsRead() != 1 {
							return errors.New("benchmark row/metadata contract changed")
						}
						values := rows[0].ValuesCopy()
						if len(values) != 1 || string(values[0]) != payload {
							return errors.New("benchmark useful payload changed")
						}
						return nil
					}
					run := func() error {
						var receipt *invocation.Receipt[Result]
						var err error
						if statement == nil {
							receipt, err = tx.Query(ctx, correlation("benchmark-query"), "SELECT $1::text", payload)
						} else {
							receipt, err = statement.Query(ctx, correlation("benchmark-query"), payload)
						}
						if err != nil {
							return err
						}
						if err = check(receipt.Result()); err != nil {
							return err
						}
						record, err := fixture.inbox.Next(ctx)
						if err != nil {
							return err
						}
						return errors.Join(check(record.Receipt().Result()), record.Release())
					}
					if err := run(); err != nil {
						b.Fatal(err)
					}
					b.SetBytes(int64(size))
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if err := run(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if tx.handle.Value() != connection || peer.connects.Load() != 1 || fixture.database.Stats().AcquiredResources() != 1 {
						b.Fatal("benchmark changed its retained connection")
					}
				})
			}
		})
	}
}
