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

package franz

import (
	"bytes"
	"context"
	"slices"
	"strconv"
	"testing"
	"time"
)

// Both variants keep the same data, ACK/identity/evidence guarantees and bounds.
// kfake timings are local combination measurements, not production throughput.
func BenchmarkProduceBatch(b *testing.B) {
	for _, compression := range []string{"none", "gzip"} {
		b.Run(compression, func(b *testing.B) {
			cluster := localCluster(b)
			options := clusterOptions(cluster)
			options.Compression = compression
			fixture := bindFixture(b, options, 1)
			messages := make([]Message, 64)
			for index := range messages {
				messages[index] = Message{Topic: "records", Value: bytes.Repeat([]byte("data"), 256)}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			latencies := make([]time.Duration, b.N)
			b.SetBytes(64 * 1024)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				started := time.Now()
				receipt, err := fixture.client.Produce(ctx, correlation("batch-"+strconv.Itoa(iteration)), messages)
				if err != nil {
					b.Fatal(err)
				}
				result, err := receipt.WaitReleased(ctx)
				if err != nil || result.Err() != nil {
					b.Fatal("benchmark delivery failed")
				}
				writes := result.Outcome.Value.WritesCopy()
				if len(writes) != 64 || writes[0].State != WriteAcknowledged || !writes[0].PositionKnown || !writes[0].IdentityChecked {
					b.Fatal("benchmark guarantee changed")
				}
				delivery, err := fixture.inbox.Next(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if err := delivery.Release(); err != nil {
					b.Fatal(err)
				}
				latencies[iteration] = time.Since(started)
			}
			b.StopTimer()
			if len(latencies) > 0 {
				slices.Sort(latencies)
				b.ReportMetric(float64(latencies[(len(latencies)-1)*95/100].Nanoseconds()), "p95-ns")
				b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100].Nanoseconds()), "p99-ns")
			}
		})
	}
}
