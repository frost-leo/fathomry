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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type managedNilRecord struct{ arrow.RecordBatch }

func TestManagedRecordBytesCountsSharedSlicedDictionaryBacking(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	dictionaryType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	builder := array.NewDictionaryBuilder(allocator, dictionaryType).(*array.BinaryDictionaryBuilder)
	for index := range 1024 {
		if index%7 == 0 {
			builder.AppendNull()
		} else if err := builder.AppendString(strings.Repeat("dictionary-backing", index%31+1)); err != nil {
			t.Fatal(err)
		}
	}
	dictionary := builder.NewDictionaryArray()
	builder.Release()
	sliced := array.NewSlice(dictionary, 1, 2)
	dictionary.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "first", Type: dictionaryType, Nullable: true},
		{Name: "shared", Type: dictionaryType, Nullable: true},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{sliced, sliced}, 1)
	sliced.Release()
	defer record.Release()
	// Only this record now owns allocator-backed buffers. A one-row slice must
	// still pay for the entire dictionary and index parents, without doubling
	// the cost when the same buffers appear in another column.
	bytes := RecordBytes(record)
	if bytes != int64(allocator.CurrentAlloc()) || bytes <= 1024 {
		t.Fatalf("retained backing accounting: got=%d allocated=%d", bytes, allocator.CurrentAlloc())
	}
	scope := &ManagedSession{source: &managedSource{prepared: ManagedPreparation{settings: ManagedSettings{MaxRows: 2, MaxBatchBytes: bytes - 1, MaxIOBytes: bytes * 2}}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	visited := false
	summary, err := scope.consume(ctx, cancel, func(yield func(arrow.RecordBatch, error) bool) {
		record.Retain()
		yield(record, nil)
	}, func(arrow.RecordBatch) (bool, error) {
		visited = true
		return true, nil
	})
	if !errors.Is(err, ErrLimit) || visited || summary.Complete || summary.Rows != 0 {
		t.Fatal("shared dictionary escaped pre-visitor byte limit")
	}
}

func TestManagedTypedNilRecordKeepsOriginalCause(t *testing.T) {
	var record *managedNilRecord
	if RecordBytes(record) != 0 {
		t.Fatal("typed nil counted data")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scope := &ManagedSession{}
	original := errors.New("native iterator failed")
	_, err := scope.consume(ctx, cancel, func(yield func(arrow.RecordBatch, error) bool) { yield(record, original) }, func(arrow.RecordBatch) (bool, error) {
		t.Error("typed nil reached visitor")
		return true, nil
	})
	if !errors.Is(err, original) {
		t.Fatal("iterator failure replaced by nil-record cleanup")
	}
}
