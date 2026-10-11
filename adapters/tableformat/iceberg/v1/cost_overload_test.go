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

package iceberg_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

// BenchmarkSaturatedDataFamily holds one identical 65-row stream at its first
// borrowed batch. Rejected work must not invoke the callback or acquire another
// result. The held stream is then resumed, verified exactly and receipted.
func BenchmarkSaturatedDataFamily(b *testing.B) {
	for _, variant := range []string{"internal", "public", "public", "internal"} {
		b.Run(variant, func(b *testing.B) {
			server := dataBenchmarkPeer(b)
			defer server.Close()
			settings := publicSettings(server.URL)
			settings.MaxActive = 1
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			done := make(chan error, 1)
			var retained int64
			rows := benchmarkRows{}
			first := true
			visit := func(batch arrow.RecordBatch) (bool, error) {
				if first {
					first = false
					retained = native.RecordBytes(batch)
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return false, ctx.Err()
					}
				}
				return rows.visit(batch)
			}
			var reject func() error
			if variant == "internal" {
				prepared, err := native.PrepareManaged(native.ManagedSettings{Name: settings.Name, CatalogURI: settings.CatalogURI, CatalogPrefix: settings.CatalogPrefix, Warehouse: settings.Warehouse, Namespace: settings.Namespace, Location: settings.Location, Plaintext: settings.Plaintext, StorageEndpoint: settings.StorageEndpoint, StorageRegion: settings.StorageRegion, StorageAccessKey: settings.StorageAccessKey, StorageSecretKey: settings.StorageSecretKey, Permissions: native.Grants{Read: true}, MaxActive: 1}, native.ManagedNativeOptions{})
				if err != nil {
					b.Fatal(err)
				}
				selection, err := prepared.Select()
				if err != nil {
					b.Fatal(err)
				}
				selection = source.WithLimits(selection, prepared.Limits())
				assembly, err := source.Assemble(ctx, ctx, "overload", selection)
				if err != nil {
					b.Fatal(err)
				}
				defer assembly.Close(context.Background())
				inbox, err := invocation.NewInbox[native.ManagedResult](2, 2*prepared.Budget().EvidenceBytes)
				if err != nil {
					b.Fatal(err)
				}
				client, err := native.BindManaged(assembly, selection, inbox)
				if err != nil {
					b.Fatal(err)
				}
				receipt, err := client.Use(ctx, fault.Correlation{Call: "held"}, func(s *native.ManagedSession) error {
					tbl, err := s.LoadTable([]string{"isolated", "historical"})
					if err != nil {
						return err
					}
					scan, err := tbl.Scan(native.ManagedScanOptions{BatchSize: 8})
					if err != nil {
						return err
					}
					summary, err := scan.Records(visit)
					if err != nil {
						return err
					}
					return rows.complete(summary.Complete)
				})
				if err != nil {
					b.Fatal(err)
				}
				go func() {
					result, err := receipt.WaitReleased(ctx)
					if err == nil {
						err = result.Err()
					}
					if err == nil {
						delivery, claimErr := inbox.Next(ctx)
						err = claimErr
						if err == nil {
							err = delivery.Release()
						}
					}
					done <- err
				}()
				reject = func() error {
					called := false
					receipt, err := client.Use(ctx, fault.Correlation{Call: "saturated"}, func(*native.ManagedSession) error { called = true; return nil })
					if err == nil || receipt != nil || called {
						return fmt.Errorf("internal saturation admitted work")
					}
					return nil
				}
			} else {
				owner, inbox, _ := openPublic(b, settings)
				go func() {
					receipt, err := owner.Client().Use(ctx, func(s *iceberg.Session) error {
						tbl, err := s.LoadTable([]string{"isolated", "historical"})
						if err != nil {
							return err
						}
						scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 8})
						if err != nil {
							return err
						}
						summary, err := scan.Records(visit)
						if err != nil {
							return err
						}
						return rows.complete(summary.Complete)
					})
					if err == nil {
						result, waitErr := receipt.WaitReleased(ctx)
						err = waitErr
						if err == nil {
							err = result.Err()
						}
						if err == nil {
							delivery, claimErr := inbox.NextReleased(ctx)
							err = claimErr
							if err == nil {
								err = delivery.Ack()
							}
						}
					}
					done <- err
				}()
				reject = func() error {
					called := false
					var receipt *adapters.Receipt[iceberg.Result]
					receipt, err := owner.Client().Use(ctx, func(*iceberg.Session) error { called = true; return nil })
					if err == nil || receipt != nil || called {
						return fmt.Errorf("public saturation admitted work")
					}
					return nil
				}
			}
			select {
			case <-entered:
			case err := <-done:
				b.Fatal("held stream exited", err)
			case <-ctx.Done():
				b.Fatal(ctx.Err())
			}
			elapsed := make([]time.Duration, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for index := range b.N {
				started := time.Now()
				if err := reject(); err != nil {
					b.Fatal(err)
				}
				elapsed[index] = time.Since(started)
			}
			b.StopTimer()
			close(release)
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
			if len(elapsed) > 0 {
				b.ReportMetric(float64(elapsed[(len(elapsed)-1)*95/100].Nanoseconds()), "p95-ns")
			}
			b.ReportMetric(float64(retained), "held-backing-B")
		})
	}
}
