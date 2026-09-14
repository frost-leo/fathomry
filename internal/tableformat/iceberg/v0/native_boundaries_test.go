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

package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table"
)

func TestNativeArrowOwnershipOnEarlyExitCancellationAndReadError(t *testing.T) {
	for _, mode := range []string{"full", "early", "canceled", "missing-file"} {
		t.Run(mode, func(t *testing.T) {
			peer, options := newPeer(t)
			f := bindFixture(t, options, 8)
			setupTable(t, f)
			appendBatch(t, f, 0, 4097, "rows")
			allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
			ctx, cancel := context.WithCancel(deadline(t))
			defer cancel()
			state := newExchange(f.client.owner, nil)
			ctx = compute.WithAllocator(withExchange(ctx, state), allocator)
			tbl, err := f.client.load(ctx, state, "data")
			if err != nil {
				t.Fatal(err)
			}
			tasks, err := tbl.Scan(table.WitMaxConcurrency(1)).PlanFiles(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "missing-file" {
				peer.mu.Lock()
				delete(peer.objects, strings.TrimPrefix(tasks[0].File.FilePath(), "s3://fixture/"))
				peer.mu.Unlock()
			}
			if mode == "canceled" {
				cancel()
			}
			schema, records, err := tbl.Scan(table.WitMaxConcurrency(1)).ReadTasks(ctx, tasks)
			if err == nil {
				limit := 4098
				if mode == "early" {
					limit = 3
				}
				data := &resultData{}
				err = collectRecords(data, schema, records, limit, optionsOrDefaultBatchBytes(options))
				if mode == "full" {
					if err != nil {
						t.Fatal(err)
					}
					inspectRows(t, Result{data: data}, expectedRows(0, 4097, "rows"))
				}
				if mode == "early" && (err != nil || data.rows != 3 || data.complete) {
					t.Fatal("early exit contract failed", err)
				}
			}
			if (mode == "canceled" || mode == "missing-file") && err == nil {
				t.Fatal("read failure disappeared")
			}
			_, _ = state.finish()
			allocator.AssertSize(t, 0)
		})
	}
}
func optionsOrDefaultBatchBytes(options OptionsV1) int { return defaults(options).MaxBatchBytes }

func TestPartialRecordsRemainDecodableAfterFailure(t *testing.T) {
	first := makeBatch(t, 0, 17, "first")
	second := makeBatch(t, 20, 17, "second")
	cause := errors.New("controlled-late-read-failure")
	for _, limited := range []bool{false, true} {
		t.Run(map[bool]string{false: "iterator-error", true: "byte-limit"}[limited], func(t *testing.T) {
			records := func(yield func(arrow.RecordBatch, error) bool) {
				first.Retain()
				if !yield(first, nil) {
					return
				}
				if limited {
					second.Retain()
					yield(second, nil)
				} else {
					yield(nil, cause)
				}
			}
			var reference bytes.Buffer
			writer := ipc.NewWriter(&reference, ipc.WithSchema(first.Schema()))
			if err := writer.Write(first); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			maximum := 1 << 20
			if limited {
				maximum = reference.Len()
			}
			data := &resultData{}
			err := collectRecords(data, first.Schema(), records, 100, maximum)
			if err == nil || data.complete {
				t.Fatal("partial scan certified complete")
			}
			if !limited && !errors.Is(err, cause) {
				t.Fatal("original iterator cause lost")
			}
			inspectRows(t, Result{data: data}, expectedRows(0, 17, "first"))
		})
	}
}

func TestVendedCredentialsAndEndpointAreNotUsed(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 10)
	setupTable(t, f)
	var storageRequests atomic.Int64
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if strings.HasPrefix(request.URL.Path, "/fixture/") {
			storageRequests.Add(1)
			if !strings.Contains(request.Header.Get("Authorization"), "Credential=test-access/") ||
				strings.Contains(request.Header.Get("Authorization"), "untrusted") ||
				request.Header.Get("X-Amz-Security-Token") != "" {
				t.Error("selected storage identity changed")
			}
		}
		if request.Method == "GET" && strings.HasSuffix(request.URL.Path, "/tables/data") {
			peer.mu.Lock()
			payload := peer.tableResponse("data", peer.tables["data"])
			peer.mu.Unlock()
			payload["config"] = map[string]string{"s3.endpoint": "http://127.0.0.1:1", "s3.access-key-id": "untrusted", "s3.secret-access-key": "untrusted"}
			payload["storage-credentials"] = []any{map[string]any{"prefix": options.Location, "config": payload["config"]}}
			writeJSON(writer, payload)
			return true
		}
		if strings.HasSuffix(request.URL.Path, "/credentials") {
			t.Error("credential refresher ran")
		}
		return false
	}
	peer.mu.Unlock()
	appendBatch(t, f, 0, 33, "static")
	inspectRows(t, readAll(t, f), expectedRows(0, 33, "static"))
	if storageRequests.Load() == 0 {
		t.Fatal("no independent storage requests observed")
	}
}
func TestMetadataObjectMustMatchCatalog(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 8)
	setupTable(t, f)
	peer.mu.Lock()
	var metadata map[string]any
	_ = json.Unmarshal(peer.tables["data"], &metadata)
	metadata["table-uuid"] = "00000000-0000-4000-8000-000000000001"
	changed, _ := json.Marshal(metadata)
	peer.objects[strings.TrimPrefix(peer.locations["data"], "s3://fixture/")] = changed
	peer.mu.Unlock()
	receipt, err := f.client.Read(deadline(t), correlation("mismatched-store"), "data", BatchRead{})
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Reloaded() {
		t.Fatal("mismatched Catalog/object store certified")
	}
}
