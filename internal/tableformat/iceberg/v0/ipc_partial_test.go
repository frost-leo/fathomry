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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func encodeIPCReference(t *testing.T, schema *arrow.Schema, records ...arrow.RecordBatch) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			closeErr := writer.Close()
			t.Fatal(errors.Join(err, closeErr))
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestIPCPartialPrefixAtEveryByteLimit(t *testing.T) {
	first := makeBatch(t, 0, 17, "first")
	second := makeBatch(t, 20, 17, "second")
	prefix := encodeIPCReference(t, first.Schema(), first)
	complete := encodeIPCReference(t, first.Schema(), first, second)
	records := func(yield func(arrow.RecordBatch, error) bool) {
		for _, record := range []arrow.RecordBatch{first, second} {
			record.Retain()
			if !yield(record, nil) {
				return
			}
		}
	}
	for maximum := 0; maximum <= len(complete); maximum++ {
		data := &resultData{}
		err := collectRecords(context.Background(), data, first.Schema(), records, 100, maximum)
		if len(data.ipc) > maximum {
			t.Fatalf("maximum %d: published %d bytes", maximum, len(data.ipc))
		}
		if maximum == len(complete) {
			if err != nil || !data.complete || data.rows != 34 || !bytes.Equal(data.ipc, complete) {
				t.Fatalf("exact full-stream budget: rows=%d complete=%t err=%v", data.rows, data.complete, err)
			}
			continue
		}
		if !errors.Is(err, ErrLimit) || data.complete {
			t.Fatalf("maximum %d: lost limit failure or falsely exhausted scan: %v", maximum, err)
		}
		if maximum < len(prefix) {
			if data.rows != 0 || len(data.ipc) != 0 {
				t.Fatalf("maximum %d: incomplete first record published", maximum)
			}
			continue
		}
		if data.rows != 17 || !bytes.Equal(data.ipc, prefix) {
			t.Fatalf("maximum %d: complete scalar prefix changed", maximum)
		}
	}
	inspectRows(t, Result{data: &resultData{ipc: prefix, rows: 17}}, expectedRows(0, 17, "first"))
	t.Logf("checked every byte budget from 0 through %d; first complete prefix is %d bytes", len(complete), len(prefix))
}

func TestIPCPartialPrefixAfterSchemaMismatch(t *testing.T) {
	first := makeBatch(t, 0, 17, "first")
	fields := first.Schema().Fields()
	fields[0].Name = "different"
	second := array.NewRecordBatch(arrow.NewSchema(fields, nil), first.Columns(), first.NumRows())
	defer second.Release()
	records := func(yield func(arrow.RecordBatch, error) bool) {
		first.Retain()
		if !yield(first, nil) {
			return
		}
		second.Retain()
		yield(second, nil)
	}
	data := &resultData{}
	err := collectRecords(context.Background(), data, first.Schema(), records, 100, 1<<20)
	if err == nil || errors.Is(err, ErrLimit) || data.complete {
		t.Fatal("schema mismatch lost or partial scan certified complete", err)
	}
	if !bytes.Equal(data.ipc, encodeIPCReference(t, first.Schema(), first)) {
		t.Fatal("schema mismatch corrupted accepted prefix")
	}
	inspectRows(t, Result{data: data}, expectedRows(0, 17, "first"))
}

func TestIPCIteratorAndIndependentCloseFailureRetainCauses(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	for _, test := range []struct {
		name    string
		maximum int
		limited bool
	}{
		{name: "close-failure", maximum: 8, limited: true},
		{name: "successful-close", maximum: 1 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			readErr := errors.New("iterator failure before first record")
			cancelErr := errors.New("cancellation during iterator cleanup")
			records := func(yield func(arrow.RecordBatch, error) bool) {
				defer cancel(cancelErr)
				yield(nil, readErr)
			}
			data := &resultData{}
			err := collectRecords(ctx, data, schema, records, 100, test.maximum)
			if !errors.Is(err, readErr) || !errors.Is(err, context.Canceled) || !errors.Is(err, cancelErr) ||
				errors.Is(err, ErrLimit) != test.limited {
				t.Fatal("iterator, independent Close, or cancellation cause lost", err)
			}
			if data.complete || data.rows != 0 {
				t.Fatal("failed iterator certified complete")
			}
			if test.limited {
				if len(data.ipc) != 0 {
					t.Fatal("failed schema Close published an incomplete stream")
				}
			} else if !bytes.Equal(data.ipc, encodeIPCReference(t, schema)) {
				t.Fatal("successful Close did not preserve the empty stream")
			}
		})
	}
}
