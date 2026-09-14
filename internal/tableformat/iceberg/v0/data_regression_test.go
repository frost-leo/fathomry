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
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func TestRegressionCompactionPreservesReadableFileRowBounds(t *testing.T) {
	_, options := newPeer(t)
	options.MaxRows = 20
	f := bindFixture(t, options, 16)
	setupTable(t, f)
	appendBatch(t, f, 0, 15, "first")
	appendBatch(t, f, 15, 15, "second")
	receipt, err := f.client.Read(deadline(t), correlation("before-compact"), "data", BatchRead{Limit: 20})
	before := requireOK(t, settle(t, receipt, err))
	if before.Rows() != 20 || before.Complete() {
		t.Fatal("invalid before-compaction control")
	}
	receipt, err = f.client.Compact(deadline(t), correlation("compact"), "data")
	compacted := settle(t, receipt, err)
	if !errors.Is(compacted.Err(), ErrLimit) || compacted.Outcome.Value.Effect() != NotSubmitted || len(compacted.Outcome.Value.FilesCopy()) != 0 {
		t.Fatal("over-budget compaction was not rejected before file writes")
	}
	receipt, err = f.client.Read(deadline(t), correlation("after-compact"), "data", BatchRead{Limit: 20})
	after := settle(t, receipt, err)
	if after.Err() != nil {
		t.Fatalf("successfully compacted table is no longer readable: %s", causes(after.Err()))
	}
}

func TestRegressionSchemaEvolutionPreventsUnsupportedCommit(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 8)
	fields := make([]native.NestedField, 128)
	for index := range fields {
		fields[index] = native.NestedField{ID: index + 1, Name: fmt.Sprintf("field_%d", index), Type: native.PrimitiveTypes.Int64}
	}
	receipt, err := f.client.CreateTable(deadline(t), correlation("create"), "wide", native.NewSchema(0, fields...))
	requireOK(t, settle(t, receipt, err))
	receipt, err = f.client.EvolveSchema(deadline(t), correlation("evolve"), "wide", []SchemaChange{{Operation: "add", Name: "one_more", Type: "long"}})
	result := settle(t, receipt, err)
	peer.mu.Lock()
	metadata, parseErr := table.ParseMetadataBytes(peer.tables["wide"])
	commits := peer.commits
	peer.mu.Unlock()
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	t.Logf("evolve effect=%v error=%s commits=%d current fields=%d", result.Outcome.Value.Effect(), causes(result.Err()), commits, len(metadata.CurrentSchema().Fields()))
	if result.Err() == nil || result.Outcome.Value.Effect() != NotSubmitted || commits != 0 || len(metadata.CurrentSchema().Fields()) != 128 {
		t.Fatalf("unsupported schema was committed rather than rejected before catalog mutation")
	}
}

func TestRegressionHistoricalPredicateUsesHistoricalFieldIdentity(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 16)
	setupTable(t, f)
	written := appendBatch(t, f, 0, 10, "original")
	receipt, err := f.client.EvolveSchema(deadline(t), correlation("drop"), "data", []SchemaChange{{Operation: "drop", Name: "value"}})
	requireOK(t, settle(t, receipt, err))
	receipt, err = f.client.EvolveSchema(deadline(t), correlation("readd"), "data", []SchemaChange{{Operation: "add", Name: "value", Type: "string"}})
	requireOK(t, settle(t, receipt, err))
	receipt, err = f.client.Read(deadline(t), correlation("historical-full"), "data", BatchRead{SnapshotID: written.CommittedSnapshotID()})
	full := requireOK(t, settle(t, receipt, err))
	inspectRows(t, full, expectedRows(0, 10, "original"))
	receipt, err = f.client.Read(deadline(t), correlation("historical-filtered"), "data", BatchRead{SnapshotID: written.CommittedSnapshotID(), Predicates: []Predicate{{Column: "value", Operator: "eq", Value: "original-1"}}})
	filtered := requireOK(t, settle(t, receipt, err))
	reader, err := ipc.NewReader(bytes.NewReader(filtered.IPCCopy()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	t.Logf("full rows=%d filtered rows=%d complete=%v historical schema=%s", full.Rows(), filtered.Rows(), filtered.Complete(), reader.Schema())
	if filtered.Rows() != 1 {
		t.Fatalf("historical filter should find original row but returned %d", filtered.Rows())
	}
}

func TestRegressionNonNullableInputRejectsNullValues(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 12)
	setupTable(t, f)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendNull()
	builder.Field(1).(*array.StringBuilder).Append("null-id")
	batch := builder.NewRecordBatch()
	defer batch.Release()
	if batch.Column(0).NullN() != 1 || batch.Schema().Field(0).Nullable {
		t.Fatal("bad null control")
	}
	receipt, err := f.client.Write(deadline(t), correlation("null-id"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{batch}})
	result := settle(t, receipt, err)
	if errors.Is(result.Err(), ErrInput) && result.Outcome.Value.Effect() == NotSubmitted {
		return
	}
	read := readAll(t, f)
	reader, err := ipc.NewReader(bytes.NewReader(read.IPCCopy()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("missing read result")
	}
	ids := reader.RecordBatch().Column(0).(*array.Int64)
	t.Fatalf("required null was committed as a non-null value: effect=%v rows=%d read-id=%d null=%v", result.Outcome.Value.Effect(), read.Rows(), ids.Value(0), ids.IsNull(0))
}

func TestRegressionStaleCommitReplyCannotCertifyWrite(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 12)
	setupTable(t, f)
	written := appendBatch(t, f, 0, 10, "base")
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method != http.MethodPost || !strings.HasSuffix(request.URL.Path, "/tables/data") {
			return false
		}
		peer.mu.Lock()
		defer peer.mu.Unlock()
		writeJSON(writer, peer.tableResponse("data", peer.tables["data"]))
		return true
	}
	peer.mu.Unlock()
	receipt, err := f.client.Write(deadline(t), correlation("stale-reply"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{makeBatch(t, 10, 10, "not-applied")}})
	result := settle(t, receipt, err)
	peer.mu.Lock()
	peer.hook = nil
	peer.mu.Unlock()
	if result.Err() == nil {
		read := readAll(t, f)
		t.Fatalf("stale metadata reply certified absent write: effect=%v complete=%v input-rows=%d actual-rows=%d committed-id=%d previous-id=%d", result.Outcome.Value.Effect(), result.Outcome.Value.Complete(), result.Outcome.Value.Rows(), read.Rows(), result.Outcome.Value.CommittedSnapshotID(), written.CommittedSnapshotID())
	}
}

func TestRegressionPositiveControls(t *testing.T) {
	t.Run("compaction-at-row-bound", func(t *testing.T) {
		_, options := newPeer(t)
		options.MaxRows = 20
		f := bindFixture(t, options, 12)
		setupTable(t, f)
		appendBatch(t, f, 0, 10, "first")
		appendBatch(t, f, 10, 10, "second")
		receipt, err := f.client.Compact(deadline(t), correlation("compact"), "data")
		requireOK(t, settle(t, receipt, err))
		read := readAll(t, f)
		if read.Rows() != 20 {
			t.Fatalf("rows=%d", read.Rows())
		}
	})
	t.Run("schema-at-field-bound", func(t *testing.T) {
		_, options := newPeer(t)
		f := bindFixture(t, options, 8)
		fields := make([]native.NestedField, 127)
		for index := range fields {
			fields[index] = native.NestedField{ID: index + 1, Name: fmt.Sprintf("field_%d", index), Type: native.PrimitiveTypes.Int64}
		}
		receipt, err := f.client.CreateTable(deadline(t), correlation("create"), "wide", native.NewSchema(0, fields...))
		requireOK(t, settle(t, receipt, err))
		receipt, err = f.client.EvolveSchema(deadline(t), correlation("evolve"), "wide", []SchemaChange{{Operation: "add", Name: "one_more", Type: "long"}})
		requireOK(t, settle(t, receipt, err))
	})
	t.Run("historical-filter-with-unchanged-schema", func(t *testing.T) {
		_, options := newPeer(t)
		f := bindFixture(t, options, 12)
		setupTable(t, f)
		written := appendBatch(t, f, 0, 10, "original")
		appendBatch(t, f, 10, 10, "later")
		receipt, err := f.client.Read(deadline(t), correlation("historical"), "data", BatchRead{SnapshotID: written.CommittedSnapshotID(), Predicates: []Predicate{{Column: "value", Operator: "eq", Value: "original-1"}}})
		filtered := requireOK(t, settle(t, receipt, err))
		inspectRows(t, filtered, expectedRows(1, 1, "original"))
	})
}

func TestRegressionStagedTableMetadataValidationIsLocal(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 8)
	fields := make([]native.NestedField, 128)
	for index := range fields {
		fields[index] = native.NestedField{ID: index + 1, Name: fmt.Sprintf("field_%d", index), Type: native.PrimitiveTypes.Int64}
	}
	receipt, err := f.client.CreateTable(deadline(t), correlation("create"), "wide", native.NewSchema(0, fields...))
	requireOK(t, settle(t, receipt, err))
	peer.mu.Lock()
	metadata, parseErr := table.ParseMetadataBytes(peer.tables["wide"])
	location := peer.locations["wide"]
	requests := peer.requests
	peer.mu.Unlock()
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	base := table.New(table.Identifier{"isolated", "wide"}, metadata, location, nil, nil)
	transaction := base.NewTransaction()
	if err := transaction.UpdateSchema(true, false).AddColumn([]string{"one_more"}, native.PrimitiveTypes.Int64, "", false, nil).Commit(); err != nil {
		t.Fatal(err)
	}
	staged, err := transaction.StagedTable()
	if err != nil {
		t.Fatal(err)
	}
	if validateMetadata(defaults(options), staged.Metadata()) == nil {
		t.Fatal("staged metadata did not expose unsupported schema")
	}
	peer.mu.Lock()
	after := peer.requests
	peer.mu.Unlock()
	if requests != after || len(base.Schema().Fields()) != 128 || len(staged.Schema().Fields()) != 129 {
		t.Fatalf("nonlocal or mutating staging: requests before=%d after=%d base=%d staged=%d", requests, after, len(base.Schema().Fields()), len(staged.Schema().Fields()))
	}
}
