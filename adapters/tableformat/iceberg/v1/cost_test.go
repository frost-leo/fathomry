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
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

// This comparison measures one small Catalog read with identical source limits
// and mandatory evidence reception. It is not a scan/write or RSS benchmark.
func BenchmarkCatalogFamilyBridge(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/catalog/v1/config" {
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"namespace":["isolated"],"properties":{"value":"exact"}}`))
	}))
	defer server.Close()
	settings := publicSettings(server.URL)
	for _, variant := range []string{"internal", "public"} {
		b.Run(variant, func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			var call func() error
			if variant == "internal" {
				prepared, err := native.PrepareManaged(native.ManagedSettings{Name: settings.Name, CatalogURI: settings.CatalogURI, CatalogPrefix: settings.CatalogPrefix, Warehouse: settings.Warehouse, Namespace: settings.Namespace, Location: settings.Location, Plaintext: settings.Plaintext, StorageEndpoint: settings.StorageEndpoint, StorageRegion: settings.StorageRegion, StorageAccessKey: settings.StorageAccessKey, StorageSecretKey: settings.StorageSecretKey, Permissions: native.Grants{Read: true}}, native.ManagedNativeOptions{})
				if err != nil {
					b.Fatal(err)
				}
				selection, err := prepared.Select()
				if err != nil {
					b.Fatal(err)
				}
				selection = source.WithLimits(selection, prepared.Limits())
				assembly, err := source.Assemble(ctx, ctx, "benchmark", selection)
				if err != nil {
					b.Fatal(err)
				}
				defer assembly.Close(context.Background())
				inbox, _ := invocation.NewInbox[native.ManagedResult](1, prepared.Budget().EvidenceBytes)
				client, err := native.BindManaged(assembly, selection, inbox)
				if err != nil {
					b.Fatal(err)
				}
				call = func() error {
					receipt, err := client.Use(ctx, fault.Correlation{Call: "benchmark"}, func(s *native.ManagedSession) error {
						props, err := s.LoadNamespaceProperties([]string{"isolated"})
						if err == nil && props["value"] != "exact" {
							b.Error("values changed")
						}
						return err
					})
					if err != nil {
						return err
					}
					result, err := receipt.WaitReleased(ctx)
					if err != nil {
						return err
					}
					delivery, err := inbox.Next(ctx)
					if err != nil {
						return err
					}
					if err = delivery.Release(); err != nil {
						return err
					}
					return result.Err()
				}
			} else {
				policy, err := iceberg.Recommend(settings)
				if err != nil {
					b.Fatal(err)
				}
				runtime, err := adapters.New(ctx, policy.Runtime)
				if err != nil {
					b.Fatal(err)
				}
				defer runtime.Close(context.Background())
				inbox, _ := adapters.NewInbox[iceberg.Result](policy.Evidence)
				owner, err := iceberg.Open(ctx, settings, iceberg.Dependencies{Runtime: runtime, Evidence: inbox})
				if err != nil {
					b.Fatal(err)
				}
				defer owner.Close(context.Background())
				call = func() error {
					receipt, err := owner.Client().Use(ctx, func(s *iceberg.Session) error {
						props, err := s.LoadNamespaceProperties([]string{"isolated"})
						if err == nil && props["value"] != "exact" {
							b.Error("values changed")
						}
						return err
					})
					if err != nil {
						return err
					}
					result, err := receipt.WaitReleased(ctx)
					if err != nil {
						return err
					}
					delivery, err := inbox.NextReleased(ctx)
					if err != nil {
						return err
					}
					if err = delivery.Ack(); err != nil {
						return err
					}
					return result.Err()
				}
			}
			elapsed := make([]time.Duration, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				if err := call(); err != nil {
					b.Fatal(err)
				}
				elapsed[i] = time.Since(started)
			}
			b.StopTimer()
			sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
			if len(elapsed) > 0 {
				b.ReportMetric(float64(elapsed[(len(elapsed)-1)/2].Nanoseconds()), "p50-ns")
				b.ReportMetric(float64(elapsed[(len(elapsed)-1)*95/100].Nanoseconds()), "p95-ns")
			}
		})
	}
}
