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

package zerolog_test

import (
	"context"
	"fmt"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"
)

// Keep two borrowed byte outputs, admission, outcomes and independent evidence
// constant. Filtering is the only changed mechanism within each comparison.
func BenchmarkPublicControlledTwoSinks(b *testing.B) {
	for _, filtered := range []bool{false, true} {
		b.Run(fmt.Sprintf("filtered-%t", filtered), func(b *testing.B) {
			level := zerolog.Info
			if filtered {
				level = zerolog.Error
			}
			settings := zerolog.Settings{Name: "benchmark", Version: 1, MinLevel: &level, Sinks: []zerolog.Sink{{Name: "first", Kind: "writer"}, {Name: "second", Kind: "writer"}}}
			policy, err := zerolog.Recommend(settings)
			if err != nil {
				b.Fatal(err)
			}
			runtime, err := adapters.New(context.Background(), policy.Runtime)
			if err != nil {
				b.Fatal(err)
			}
			inbox, err := adapters.NewInbox[zerolog.Result](policy.Evidence)
			if err != nil {
				b.Fatal(err)
			}
			owner, err := zerolog.Open(context.Background(), settings, zerolog.Dependencies{Runtime: runtime, Evidence: inbox, Writers: map[string]io.Writer{"first": io.Discard, "second": io.Discard}})
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
			var samples [1024]time.Duration
			count := 0
			b.ReportAllocs()
			for b.Loop() {
				start := time.Now()
				receipt, err := owner.Client().Log(context.Background(), zerolog.Info, "message", slog.Int("rows", 12), slog.String("component", "node"))
				if err != nil {
					b.Fatal(err)
				}
				snapshot, err := receipt.WaitReleased(context.Background())
				if err != nil || snapshot.Err() != nil {
					b.Fatal(err, snapshot.Err())
				}
				value, ok := snapshot.ValueCopy()
				if !ok || len(value.SinksCopy()) != 2 {
					b.Fatal("missing output facts")
				}
				for _, sink := range value.SinksCopy() {
					if sink.Filtered != filtered || sink.Accepted == filtered || sink.Attempted == filtered {
						b.Fatal("wrong measured effect")
					}
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
			_ = inbox.Seal()
			for {
				delivery, err := inbox.NextReleased(context.Background())
				if err == io.EOF {
					break
				}
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
