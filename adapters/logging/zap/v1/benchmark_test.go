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

package zap_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"go.uber.org/zap/zapcore"
)

// The comparison retains native JSON, file policy, admission, independent
// evidence and acknowledgement. Filtered calls also produce accepted evidence.
func BenchmarkPublicFileLogging(b *testing.B) {
	for _, filtered := range []bool{false, true} {
		for _, compress := range []bool{false, true} {
			b.Run(fmt.Sprintf("filtered-%t/gzip-%t", filtered, compress), func(b *testing.B) {
				directory := b.TempDir()
				if err := os.Chmod(directory, 0700); err != nil {
					b.Fatal(err)
				}
				level := "info"
				if filtered {
					level = "warn"
				}
				settings := zap.Settings{Name: "benchmark", Version: 1, MaxEntryBytes: ptr(8192), Outputs: []zap.Output{{Name: "file", Kind: "file", Directory: directory, Level: &level, MaxFileBytes: ptr(int64(64 << 10)), MaxBackups: ptr(2), Compress: &compress}}}
				policy, err := zap.Recommend(settings)
				if err != nil {
					b.Fatal(err)
				}
				runtime, err := adapters.New(context.Background(), policy.Runtime)
				if err != nil {
					b.Fatal(err)
				}
				inbox, err := adapters.NewInbox[zap.Result](policy.Evidence)
				if err != nil {
					b.Fatal(err)
				}
				owner, err := zap.Open(context.Background(), settings, zap.Dependencies{Runtime: runtime, Evidence: inbox})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := owner.Close(context.Background()); err != nil {
						b.Error(err)
					}
					if err := runtime.Close(context.Background()); err != nil {
						b.Error(err)
					}
				})
				message := strings.Repeat("x", 256)
				var samples [1024]time.Duration
				count := 0
				b.SetBytes(256)
				b.ReportAllocs()
				for b.Loop() {
					start := time.Now()
					receipt, err := owner.Client().Log(context.Background(), zapcore.InfoLevel, message)
					if err != nil {
						b.Fatal(err)
					}
					snapshot, err := receipt.WaitReleased(context.Background())
					if err != nil || snapshot.Err() != nil {
						b.Fatal(err, snapshot.Err())
					}
					value, ok := snapshot.ValueCopy()
					if !ok {
						b.Fatal("missing output")
					}
					expected := zap.Written
					if filtered {
						expected = zap.Filtered
					}
					if value.SinksCopy()[0].State != expected {
						b.Fatal("incorrect measured effect")
					}
					delivery, err := inbox.NextReleased(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if err := delivery.Ack(); err != nil {
						b.Fatal(err)
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
				b.StopTimer()
				start := time.Now()
				if err := owner.Close(context.Background()); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(time.Since(start)), "shutdown-ns")
				if err := inbox.Seal(); err != nil {
					b.Fatal(err)
				}
				for index := 0; index < 2; index++ {
					delivery, err := inbox.NextReleased(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if err := delivery.Ack(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
