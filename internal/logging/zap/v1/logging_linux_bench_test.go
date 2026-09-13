//go:build linux

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

package zap

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"go.uber.org/zap/zapcore"
)

// The gzip ablation retains the same input, native JSON, admission, evidence,
// rotation size and backup count. It does not compare void logging with receipts.
func BenchmarkBoundedFileLogging(b *testing.B) {
	for _, size := range []int{256, 4096} {
		for _, compress := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes-%d/gzip-%t", size, compress), func(b *testing.B) {
				options := fileOptions(b, compress)
				options.MaxEntryBytes = 8192
				options.Outputs[0].MaxFileBytes = 64 << 10
				options.Outputs[0].MaxBackups = 2
				fixture := bindFixture(b, options, nil, 1)
				message := strings.Repeat("x", size)
				ctx := context.Background()
				var samples [1024]time.Duration
				count := 0
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					start := time.Now()
					receipt, err := fixture.logger.Log(ctx, fault.Correlation{Call: "benchmark"}, zapcore.InfoLevel, message)
					if err != nil {
						b.Fatal(err)
					}
					result, ok := receipt.Result()
					if !ok || result.Err() != nil || !result.Released || result.Outcome.Value.SinksCopy()[0].State != Written {
						b.Fatal("benchmark result contract failed")
					}
					delivery, err := fixture.inbox.Next(ctx)
					if err != nil || delivery.Release() != nil {
						b.Fatal("benchmark evidence handoff failed")
					}
					samples[count%len(samples)] = time.Since(start)
					count++
				}
				used := min(count, len(samples))
				slices.Sort(samples[:used])
				if used > 0 {
					b.ReportMetric(float64(samples[(used-1)*50/100]), "sample-p50-ns")
					b.ReportMetric(float64(samples[(used-1)*95/100]), "sample-p95-ns")
					b.ReportMetric(float64(samples[(used-1)*99/100]), "sample-p99-ns")
				}
				if fixture.inbox.Usage() != (invocation.InboxUsage{}) {
					b.Fatal("benchmark leaked evidence")
				}
			})
		}
	}
}
