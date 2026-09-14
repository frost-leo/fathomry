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
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table"
)

func TestNullableMutationsPreserveNonmatchingNulls(t *testing.T) {
	for _, mode := range []WriteMode{Delete, Overwrite} {
		t.Run(string(mode), func(t *testing.T) {
			_, options := newPeer(t)
			f := bindFixture(t, options, 10)
			setupTable(t, f)
			appendBatch(t, f, 0, 21, "rows")
			request := BatchWrite{Mode: mode, Predicates: []Predicate{{Column: "value", Operator: "eq", Value: "rows-1"}}}
			if mode == Overwrite {
				request.Batches = []arrow.RecordBatch{makeBatch(t, 1, 1, "replacement")}
			}
			receipt, err := f.client.Write(deadline(t), correlation("nullable"), "data", request)
			requireOK(t, settle(t, receipt, err))
			expected := expectedRows(0, 21, "rows")
			if mode == Delete {
				delete(expected, 1)
			} else {
				expected[1] = expectedRows(1, 1, "replacement")[1]
			}
			inspectRows(t, readAll(t, f), expected)
		})
	}
}
func TestSparseReadAndRewriteReleaseArrowReferences(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 8)
	setupTable(t, f)
	appendBatch(t, f, 0, 4097, "rows")
	for _, rewrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "rewrite"}[rewrite], func(t *testing.T) {
			allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
			state := newExchange(f.client.owner, nil)
			ctx := compute.WithAllocator(withExchange(deadline(t), state), allocator)
			tbl, err := f.client.load(ctx, state, "data")
			if err != nil {
				t.Fatal(err)
			}
			scan := tbl.Scan(table.WitMaxConcurrency(1))
			schema, err := scan.Projection()
			if err != nil {
				t.Fatal(err)
			}
			operation, value := "eq", "1"
			if rewrite {
				operation, value = "gt", "1"
			}
			expression, err := predicates(schema, []Predicate{{Column: "id", Operator: operation, Value: value}})
			if err != nil {
				t.Fatal(err)
			}
			tasks, err := scan.PlanFiles(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if rewrite {
				record, err := rewrittenRows(ctx, defaults(options), tbl, tasks, expression)
				if err != nil {
					t.Fatal(err)
				}
				if record.NumRows() != 2 {
					record.Release()
					t.Fatal("sparse rewrite lost rows")
				}
				record.Release()
			} else {
				wrap, err := prepareRecords(ctx, schema, schema, expression, false)
				if err != nil {
					t.Fatal(err)
				}
				arrowSchema, records, err := scan.ReadTasks(ctx, tasks)
				if err != nil {
					t.Fatal(err)
				}
				data := &resultData{}
				if err = collectRecords(data, arrowSchema, wrap(records), 65536, defaults(options).MaxBatchBytes); err != nil {
					t.Fatal(err)
				}
				inspectRows(t, Result{data: data}, expectedRows(1, 1, "rows"))
			}
			_, _ = state.finish()
			allocator.AssertSize(t, 0)
		})
	}
}
func TestFrozenWritesContainOneOwnedRecord(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 6)
	setupTable(t, f)
	raw, rows, err := freezeBatches(defaults(options), []arrow.RecordBatch{makeBatch(t, 0, 17, "first"), makeBatch(t, 17, 23, "second")})
	if err != nil || rows != 40 {
		t.Fatal("coalescing failed", err)
	}
	for range 20 {
		allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
		reader, err := ipc.NewReader(bytes.NewReader(raw), ipc.WithAllocator(allocator))
		if err != nil {
			t.Fatal(err)
		}
		if !reader.Next() || reader.RecordBatch().NumRows() != 40 {
			reader.Release()
			t.Fatal("input was not coalesced")
		}
		state := newExchange(f.client.owner, nil)
		ctx := compute.WithAllocator(withExchange(deadline(t), state), allocator)
		tbl, err := f.client.load(ctx, state, "data")
		if err != nil {
			reader.Release()
			t.Fatal(err)
		}
		work, cancel := context.WithCancel(ctx)
		cancel()
		if _, err = writeRecord(work, defaults(options), tbl, reader.RecordBatch()); err == nil {
			reader.Release()
			t.Fatal("canceled write accepted")
		}
		if reader.Next() {
			reader.Release()
			t.Fatal("multiple records escaped coalescing")
		}
		reader.Release()
		_, _ = state.finish()
		allocator.AssertSize(t, 0)
	}
}
func TestFileIOReservesBeforePayloadTransfer(t *testing.T) {
	peer, options := newPeer(t)
	options.MaxObjectBytes = 1024
	options.MaxIOBytes = 1024
	f := bindFixture(t, options, 4)
	peer.mu.Lock()
	peer.objects["owned/first"] = bytes.Repeat([]byte("a"), 700)
	peer.objects["owned/second"] = bytes.Repeat([]byte("b"), 700)
	served := 0
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "GET" && strings.HasPrefix(request.URL.Path, "/fixture/") {
			peer.mu.Lock()
			served += 700
			peer.mu.Unlock()
		}
		return false
	}
	peer.mu.Unlock()
	state := newExchange(f.client.owner, nil)
	files := &fileIO{ctx: deadline(t), state: state}
	file, err := files.Open(options.Location + "first")
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err = files.Open(options.Location + "second"); !errors.Is(err, ErrLimit) {
		t.Fatal("aggregate byte limit ignored", err)
	}
	peer.mu.Lock()
	actual := served
	peer.mu.Unlock()
	if actual != 700 || state.ioBytes != 700 {
		t.Fatalf("payload bytes=%d reserved=%d", actual, state.ioBytes)
	}
}
