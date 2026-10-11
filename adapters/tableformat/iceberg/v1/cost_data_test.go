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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

func dataBenchmarkPeer(b testing.TB) *httptest.Server {
	b.Helper()
	directory := "../../../../internal/tableformat/iceberg/v0/testdata/format2-v060"
	objects := map[string][]byte{}
	if err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "README.md" {
			return nil
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		objects["owned/historical/"+filepath.ToSlash(relative)] = data
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	var index struct {
		Location string `json:"metadata-location"`
	}
	if err := json.Unmarshal(objects["owned/historical/fixture.json"], &index); err != nil {
		b.Fatal(err)
	}
	metadata := objects[strings.TrimPrefix(index.Location, "s3://fixture/")]
	response, err := json.Marshal(map[string]any{"metadata-location": index.Location, "metadata": json.RawMessage(metadata)})
	if err != nil {
		b.Fatal(err)
	}
	var mutex sync.Mutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/catalog/v1/config" {
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse"}}`))
			return
		}
		if r.URL.Path == "/catalog/v1/warehouse/namespaces/isolated/tables/historical" {
			_, _ = w.Write(response)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/fixture/owned/historical/") {
			w.WriteHeader(404)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/fixture/")
		mutex.Lock()
		defer mutex.Unlock()
		if r.Method == http.MethodDelete {
			delete(objects, key)
			w.WriteHeader(204)
			return
		}
		if r.Method == http.MethodPut {
			if _, present := objects[key]; present || r.Header.Get("If-None-Match") != "*" {
				w.WriteHeader(412)
				return
			}
			data, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
			if err != nil {
				w.WriteHeader(400)
				return
			}
			objects[key] = data
			digest := sha256.Sum256(data)
			w.Header().Set("ETag", strconv.Quote(hex.EncodeToString(digest[:])))
			return
		}
		data, present := objects[key]
		if !present {
			w.WriteHeader(404)
			return
		}
		digest := sha256.Sum256(data)
		etag := strconv.Quote(hex.EncodeToString(digest[:]))
		if expected := r.Header.Get("If-Match"); expected != "" && expected != etag {
			w.WriteHeader(412)
			return
		}
		w.Header().Set("ETag", etag)
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			return
		}
		if header := r.Header.Get("Range"); header != "" {
			var first, last int
			if _, err := fmt.Sscanf(header, "bytes=%d-%d", &first, &last); err != nil || first < 0 || last < first || last >= len(data) {
				w.WriteHeader(416)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(last-first+1))
			w.WriteHeader(206)
			_, _ = w.Write(data[first : last+1])
			return
		}
		_, _ = w.Write(data)
	}))
}

type benchmarkRows struct {
	seen  [65]bool
	count int
}

func (rows *benchmarkRows) visit(record arrow.RecordBatch) (bool, error) {
	ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
	for index := range ids.Len() {
		id := ids.Value(index)
		if id < 0 || id >= 65 || rows.seen[id] || values.IsNull(index) != (id%7 == 0) || !values.IsNull(index) && values.Value(index) != fmt.Sprintf("historical-%d", id) {
			return false, fmt.Errorf("benchmark exact row/null mismatch")
		}
		rows.seen[id] = true
		rows.count++
	}
	return true, nil
}
func (rows *benchmarkRows) table(value arrow.Table) error {
	reader := array.NewTableReader(value, 8)
	defer reader.Release()
	for reader.Next() {
		if _, err := rows.visit(reader.RecordBatch()); err != nil {
			return err
		}
	}
	return reader.Err()
}
func (rows *benchmarkRows) complete(complete bool) error {
	if rows.count != 65 || !complete {
		return fmt.Errorf("benchmark selected result incomplete")
	}
	return nil
}
func benchmarkTasks(files []sdk.DataFile) ([]table.FileScanTask, []string) {
	tasks := make([]table.FileScanTask, len(files))
	paths := make([]string, len(files))
	for index, file := range files {
		tasks[index] = table.FileScanTask{File: file, Length: file.FileSizeBytes(), Residual: sdk.AlwaysTrue{}}
		paths[index] = file.FilePath()
	}
	return tasks, paths
}
func nativeDataBenchmark(s *native.ManagedSession, mode string, input arrow.RecordBatch) (int64, error) {
	tbl, err := s.LoadTable([]string{"isolated", "historical"})
	if err != nil {
		return 0, err
	}
	scan, err := tbl.Scan(native.ManagedScanOptions{BatchSize: 8, Planning: table.ScanPlanningLocal})
	if err != nil {
		return 0, err
	}
	var rows benchmarkRows
	if mode == "retained-table" {
		result, summary, err := scan.ToArrowTable()
		if result != nil {
			defer result.Close()
		}
		if err != nil {
			return 0, err
		}
		if err = result.Visit(rows.table); err != nil {
			return 0, err
		}
		return summary.Bytes, rows.complete(summary.Complete)
	}
	if mode == "write-verify-cleanup" {
		files, err := tbl.WriteRecords(input.Schema(), func(yield func(arrow.RecordBatch, error) bool) { input.Retain(); yield(input, nil) })
		if err != nil {
			return 0, err
		}
		tasks, paths := benchmarkTasks(files)
		summary, err := scan.ReadTasks(tasks, rows.visit)
		if err != nil {
			return 0, err
		}
		if err = rows.complete(summary.Complete); err != nil {
			return 0, err
		}
		deleted, err := s.DeleteFiles(paths)
		if err == nil && len(deleted) != len(paths) {
			err = fmt.Errorf("benchmark file cleanup incomplete")
		}
		return summary.Bytes, err
	}
	summary, err := scan.Records(rows.visit)
	if err != nil {
		return 0, err
	}
	return summary.Bytes, rows.complete(summary.Complete)
}
func publicDataBenchmark(s *iceberg.Session, mode string, input arrow.RecordBatch) (int64, error) {
	tbl, err := s.LoadTable([]string{"isolated", "historical"})
	if err != nil {
		return 0, err
	}
	scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 8, Planning: table.ScanPlanningLocal})
	if err != nil {
		return 0, err
	}
	var rows benchmarkRows
	if mode == "retained-table" {
		result, summary, err := scan.ToArrowTable()
		if result != nil {
			defer result.Close()
		}
		if err != nil {
			return 0, err
		}
		if err = result.Visit(rows.table); err != nil {
			return 0, err
		}
		return summary.Bytes, rows.complete(summary.Complete)
	}
	if mode == "write-verify-cleanup" {
		files, err := tbl.WriteRecords(input.Schema(), func(yield func(arrow.RecordBatch, error) bool) { input.Retain(); yield(input, nil) })
		if err != nil {
			return 0, err
		}
		tasks, paths := benchmarkTasks(files)
		summary, err := scan.ReadTasks(tasks, rows.visit)
		if err != nil {
			return 0, err
		}
		if err = rows.complete(summary.Complete); err != nil {
			return 0, err
		}
		deleted, err := s.DeleteFiles(paths)
		if err == nil && len(deleted) != len(paths) {
			err = fmt.Errorf("benchmark file cleanup incomplete")
		}
		return summary.Bytes, err
	}
	summary, err := scan.Records(rows.visit)
	if err != nil {
		return 0, err
	}
	return summary.Bytes, rows.complete(summary.Complete)
}

// BenchmarkDataFamilyBridge holds source budgets, native data and exact result
// validation fixed. Every iteration consumes independent evidence. Retained
// tables stay owned through verification; writes are read back and removed.
// This is a local REST/S3 peer comparison, not a process-RSS or service benchmark.
func BenchmarkDataFamilyBridge(b *testing.B) {
	for _, mode := range []string{"stream", "retained-table", "write-verify-cleanup"} {
		for _, variant := range []string{"internal", "public", "public", "internal"} {
			b.Run(mode+"/"+variant, func(b *testing.B) {
				server := dataBenchmarkPeer(b)
				defer server.Close()
				settings := publicSettings(server.URL)
				settings.MaxActive = 1
				settings.Permissions = iceberg.Grants{Read: true, Write: true, Import: true, DeleteFiles: true}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
				builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
				for id := 0; id < 65; id++ {
					builder.Field(0).(*array.Int64Builder).Append(int64(id))
					if id%7 == 0 {
						builder.Field(1).(*array.StringBuilder).AppendNull()
					} else {
						builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("historical-%d", id))
					}
				}
				input := builder.NewRecordBatch()
				builder.Release()
				defer input.Release()
				var call func() (int64, error)
				if variant == "internal" {
					prepared, err := native.PrepareManaged(native.ManagedSettings{Name: settings.Name, CatalogURI: settings.CatalogURI, CatalogPrefix: settings.CatalogPrefix, Warehouse: settings.Warehouse, Namespace: settings.Namespace, Location: settings.Location, Plaintext: settings.Plaintext, StorageEndpoint: settings.StorageEndpoint, StorageRegion: settings.StorageRegion, StorageAccessKey: settings.StorageAccessKey, StorageSecretKey: settings.StorageSecretKey, Permissions: native.Grants(settings.Permissions), MaxActive: settings.MaxActive}, native.ManagedNativeOptions{})
					if err != nil {
						b.Fatal(err)
					}
					selected, err := prepared.Select()
					if err != nil {
						b.Fatal(err)
					}
					selected = source.WithLimits(selected, prepared.Limits())
					assembly, err := source.Assemble(ctx, ctx, "data-benchmark", selected)
					if err != nil {
						b.Fatal(err)
					}
					defer assembly.Close(context.Background())
					inbox, err := invocation.NewInbox[native.ManagedResult](2, 2*prepared.Budget().EvidenceBytes)
					if err != nil {
						b.Fatal(err)
					}
					client, err := native.BindManaged(assembly, selected, inbox)
					if err != nil {
						b.Fatal(err)
					}
					call = func() (int64, error) {
						var bytes int64
						receipt, err := client.Use(ctx, fault.Correlation{Call: "benchmark"}, func(s *native.ManagedSession) error {
							var err error
							bytes, err = nativeDataBenchmark(s, mode, input)
							return err
						})
						if err != nil {
							return 0, err
						}
						result, err := receipt.WaitReleased(ctx)
						if err != nil {
							return 0, err
						}
						delivery, err := inbox.Next(ctx)
						if err != nil {
							return 0, err
						}
						if err = delivery.Release(); err != nil {
							return 0, err
						}
						return bytes, result.Err()
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
					inbox, err := adapters.NewInbox[iceberg.Result](policy.Evidence)
					if err != nil {
						b.Fatal(err)
					}
					owner, err := iceberg.Open(ctx, settings, iceberg.Dependencies{Runtime: runtime, Evidence: inbox})
					if err != nil {
						b.Fatal(err)
					}
					defer owner.Close(context.Background())
					call = func() (int64, error) {
						var bytes int64
						receipt, err := owner.Client().Use(ctx, func(s *iceberg.Session) error {
							var err error
							bytes, err = publicDataBenchmark(s, mode, input)
							return err
						})
						if err != nil {
							return 0, err
						}
						result, err := receipt.WaitReleased(ctx)
						if err != nil {
							return 0, err
						}
						delivery, err := inbox.NextReleased(ctx)
						if err != nil {
							return 0, err
						}
						if err = delivery.Ack(); err != nil {
							return 0, err
						}
						return bytes, result.Err()
					}
				}
				elapsed := make([]time.Duration, b.N)
				var logicalBytes int64
				b.ReportAllocs()
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					started := time.Now()
					bytes, err := call()
					if err != nil {
						b.Fatal(err)
					}
					logicalBytes += bytes
					elapsed[index] = time.Since(started)
				}
				b.StopTimer()
				sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
				if len(elapsed) > 0 {
					b.ReportMetric(float64(elapsed[(len(elapsed)-1)/2].Nanoseconds()), "p50-ns")
					b.ReportMetric(float64(elapsed[(len(elapsed)-1)*95/100].Nanoseconds()), "p95-ns")
					b.ReportMetric(float64(logicalBytes)/float64(b.N), "logical-output-B/op")
				}
			})
		}
	}
}
