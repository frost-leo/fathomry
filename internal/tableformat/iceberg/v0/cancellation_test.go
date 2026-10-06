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
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func TestRecordConsumptionCancellationAndCleanup(t *testing.T) {
	for _, consumer := range []string{"read", "rewrite"} {
		for _, mode := range []string{
			"empty", "full", "filtered-empty", "limited", "canceled-empty", "deadline-empty",
			"canceled-partial", "canceled-drain", "canceled-limit", "canceled-native-error",
		} {
			t.Run(consumer+"/"+mode, func(t *testing.T) {
				allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer allocator.AssertSize(t, 0)
				schema := native.NewSchema(0, native.NestedField{ID: 1, Name: "id", Type: native.PrimitiveTypes.Int64})
				arrowSchema, err := table.SchemaToArrowSchema(schema, nil, false, false)
				if err != nil {
					t.Fatal(err)
				}
				newRecord := func() arrow.RecordBatch {
					builder := array.NewRecordBuilder(allocator, arrowSchema)
					defer builder.Release()
					builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
					return builder.NewRecordBatch()
				}
				record, queued := newRecord(), newRecord()
				defer record.Release()
				entered, drained := false, false
				defer func() {
					if !drained {
						queued.Release()
					}
				}()
				cause := errors.New("controlled consumer cancellation")
				nativeError := errors.New("independent iterator failure")
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				if mode == "deadline-empty" {
					expired, stop := context.WithDeadlineCause(ctx, time.Now().Add(-time.Second), cause)
					defer stop()
					ctx = expired
				}
				ctx = compute.WithAllocator(ctx, allocator)
				if mode == "canceled-empty" {
					cancel(cause)
				}
				records := func(yield func(arrow.RecordBatch, error) bool) {
					entered = true
					defer func() {
						queued.Release()
						drained = true
						if mode == "canceled-drain" || mode == "canceled-limit" {
							cancel(cause)
						}
					}()
					if mode == "empty" || mode == "canceled-empty" || mode == "deadline-empty" || mode == "canceled-drain" {
						return
					}
					record.Retain()
					if !yield(record, nil) {
						return
					}
					if mode == "canceled-partial" || mode == "canceled-native-error" {
						cancel(cause)
					}
					if mode == "canceled-native-error" {
						yield(nil, nativeError)
					}
				}
				var expression native.BooleanExpression = native.AlwaysTrue{}
				if mode == "filtered-empty" {
					expression = native.AlwaysFalse{}
				}
				wrap, err := prepareRecords(ctx, schema, schema, expression, false)
				if err != nil {
					t.Fatal(err)
				}
				limit := 8
				if mode == "limited" || mode == "canceled-limit" {
					limit = 1
				}
				var data resultData
				var combined arrow.RecordBatch
				if consumer == "read" {
					err = collectRecords(ctx, &data, arrowSchema, wrap(records), limit, 1<<20)
				} else {
					settings := defaults(OptionsV1{})
					settings.MaxRows = limit
					combined, err = coalesceRecords(ctx, settings, arrowSchema, wrap(records))
					if combined != nil {
						defer combined.Release()
					}
				}
				if !entered || !drained {
					t.Fatal("cancellation bypassed iterator entry or cleanup")
				}
				canceled := ctx.Err()
				if canceled != nil {
					if !errors.Is(err, canceled) || !errors.Is(err, cause) {
						t.Fatal("caller cancellation or its cause disappeared", err)
					}
					if consumer == "read" && data.complete {
						t.Fatal("canceled record consumption certified exhaustion")
					}
					if consumer == "rewrite" && combined != nil {
						t.Fatal("canceled rewrite returned usable partial input")
					}
				} else if consumer == "rewrite" && limit == 1 {
					if !errors.Is(err, ErrLimit) || combined != nil {
						t.Fatal("rewrite limit contract changed", err)
					}
				} else if err != nil {
					t.Fatal("live empty/full/limited consumption failed", err)
				}
				if mode == "canceled-native-error" && !errors.Is(err, nativeError) {
					t.Fatal("cancellation replaced the native cause")
				}
				if consumer == "rewrite" && limit == 1 && !errors.Is(err, ErrLimit) {
					t.Fatal("cancellation replaced the rewrite limit cause")
				}
				if consumer == "read" {
					expectedRows := int64(3)
					if mode == "empty" || mode == "filtered-empty" || mode == "canceled-empty" || mode == "deadline-empty" || mode == "canceled-drain" {
						expectedRows = 0
					} else if limit == 1 {
						expectedRows = 1
					}
					if data.rows != expectedRows || data.complete != (canceled == nil && limit != 1) {
						t.Fatal("read prefix or completion changed", data.rows, data.complete)
					}
					reader, readErr := ipc.NewReader(bytes.NewReader(data.ipc), ipc.WithAllocator(allocator))
					if readErr != nil {
						t.Fatal("returned IPC prefix is not decodable", readErr)
					}
					var decoded int64
					for reader.Next() {
						decoded += reader.RecordBatch().NumRows()
					}
					readErr = reader.Err()
					reader.Release()
					if readErr != nil || decoded != expectedRows {
						t.Fatal("returned IPC prefix lost rows", readErr, decoded)
					}
				} else if canceled == nil && limit != 1 {
					if mode == "empty" || mode == "filtered-empty" {
						if combined != nil {
							t.Fatal("empty rewrite created a record")
						}
					} else if combined == nil || combined.NumRows() != 3 {
						t.Fatal("complete rewrite lost rows")
					}
				}
			})
		}
	}
}

type cancelOnReleaseRecord struct {
	arrow.RecordBatch
	onRelease func()
}

func (record *cancelOnReleaseRecord) Release() {
	record.RecordBatch.Release()
	record.onRelease()
}

func TestCoalesceCancellationDuringInputReleaseDiscardsOutput(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(allocator, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	ctx = compute.WithAllocator(ctx, allocator)
	cause := errors.New("canceled during owned input release")
	releases := 0
	wrapped := &cancelOnReleaseRecord{RecordBatch: record, onRelease: func() {
		releases++
		cancel(cause)
	}}
	records := func(yield func(arrow.RecordBatch, error) bool) {
		record.Retain()
		yield(wrapped, nil)
	}
	combined, err := coalesceRecords(ctx, defaults(OptionsV1{}), schema, records)
	if combined != nil {
		defer combined.Release()
		t.Error("cancellation during cleanup exposed an assembled rewrite")
	}
	if releases != 1 || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatal("input cleanup or its cancellation cause was lost", releases, err)
	}
}

func TestRecordByteLimitAndCancellationRetainBothCauses(t *testing.T) {
	first := makeBatch(t, 0, 17, "first")
	second := makeBatch(t, 20, 17, "second")
	for _, consumer := range []string{"read", "rewrite"} {
		t.Run(consumer, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("cancellation during byte-limit cleanup")
			records := func(yield func(arrow.RecordBatch, error) bool) {
				defer cancel(cause)
				first.Retain()
				if !yield(first, nil) {
					return
				}
				second.Retain()
				yield(second, nil)
			}
			var err error
			if consumer == "read" {
				var reference bytes.Buffer
				writer := ipc.NewWriter(&reference, ipc.WithSchema(first.Schema()))
				if err = writer.Write(first); err != nil {
					t.Fatal(err)
				}
				if err = writer.Close(); err != nil {
					t.Fatal(err)
				}
				data := &resultData{}
				err = collectRecords(ctx, data, first.Schema(), records, 100, reference.Len())
				if data.complete {
					t.Fatal("failed byte-limited read certified exhaustion")
				}
				inspectRows(t, Result{data: data}, expectedRows(0, 17, "first"))
			} else {
				settings := defaults(OptionsV1{})
				settings.MaxBatchBytes = 1
				var combined arrow.RecordBatch
				combined, err = coalesceRecords(ctx, settings, first.Schema(), records)
				if combined != nil {
					combined.Release()
					t.Fatal("failed byte-limited rewrite returned partial input")
				}
			}
			if !errors.Is(err, ErrLimit) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
				t.Fatal("byte-limit or cancellation cause disappeared", err)
			}
		})
	}
}
