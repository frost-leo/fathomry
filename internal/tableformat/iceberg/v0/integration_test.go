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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	native "github.com/apache/iceberg-go"
	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func testSchema() *native.Schema {
	return native.NewSchema(0,
		native.NestedField{ID: 1, Name: "id", Type: native.PrimitiveTypes.Int64, Required: true},
		native.NestedField{ID: 2, Name: "value", Type: native.PrimitiveTypes.String})
}
func makeBatch(t testing.TB, start, count int, label string) arrow.RecordBatch {
	t.Helper()
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { allocator.AssertSize(t, 0) })
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	for row := start; row < start+count; row++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(row))
		if row%7 == 0 {
			builder.Field(1).(*array.StringBuilder).AppendNull()
		} else {
			builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("%s-%d", label, row))
		}
	}
	record := builder.NewRecordBatch()
	t.Cleanup(record.Release)
	return record
}
func setupTable(t *testing.T, f fixture) {
	t.Helper()
	receipt, err := f.client.CreateNamespace(deadline(t), correlation("namespace"))
	requireOK(t, settle(t, receipt, err))
	receipt, err = f.client.CreateTable(deadline(t), correlation("create"), "data", testSchema())
	requireOK(t, settle(t, receipt, err))
}
func appendBatch(t *testing.T, f fixture, start, count int, label string) Result {
	t.Helper()
	receipt, err := f.client.Write(deadline(t), correlation("append"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{makeBatch(t, start, count, label)}})
	return requireOK(t, settle(t, receipt, err))
}

type cell struct {
	value string
	valid bool
}

func expectedRows(start, count int, label string) map[int64]cell {
	rows := map[int64]cell{}
	for row := start; row < start+count; row++ {
		if row%7 == 0 {
			rows[int64(row)] = cell{}
		} else {
			rows[int64(row)] = cell{fmt.Sprintf("%s-%d", label, row), true}
		}
	}
	return rows
}
func inspectRows(t testing.TB, result Result, expected map[int64]cell) {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(result.IPCCopy()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	seen := map[int64]bool{}
	for reader.Next() {
		record := reader.RecordBatch()
		ids, ok := record.Column(0).(*array.Int64)
		if !ok {
			t.Fatal("id column type changed")
		}
		values, ok := record.Column(1).(*array.String)
		if !ok {
			t.Fatal("value column type changed")
		}
		for index := 0; index < ids.Len(); index++ {
			id := ids.Value(index)
			want, exists := expected[id]
			if !exists || seen[id] || ids.IsNull(index) || values.IsValid(index) != want.valid ||
				want.valid && values.Value(index) != want.value {
				t.Fatalf("row mismatch for id %d", id)
			}
			seen[id] = true
		}
	}
	if reader.Err() != nil {
		t.Fatal(reader.Err())
	}
	if len(seen) != len(expected) || result.Rows() != int64(len(expected)) {
		t.Fatalf("rows: actual=%d expected=%d", len(seen), len(expected))
	}
}
func readAll(t *testing.T, f fixture) Result {
	t.Helper()
	receipt, err := f.client.Read(deadline(t), correlation("read"), "data", BatchRead{})
	result := requireOK(t, settle(t, receipt, err))
	if !result.Complete() {
		t.Fatal("unexpected truncated scan")
	}
	return result
}
func TestBoundedTableIntegration(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 48)
	setupTable(t, f)
	appended := appendBatch(t, f, 0, 4097, "original")
	if !appended.Staged() || appended.Effect() != Acknowledged || !appended.Reloaded() || appended.SnapshotID() == 0 || len(appended.FilesCopy()) == 0 {
		t.Fatal("write phases not evidenced")
	}
	original := expectedRows(0, 4097, "original")
	inspectRows(t, readAll(t, f), original)
	originalSnapshot := appended.SnapshotID()
	receipt, err := f.client.Read(deadline(t), correlation("limited"), "data", BatchRead{Limit: 3})
	limited := requireOK(t, settle(t, receipt, err))
	if limited.Complete() || limited.Rows() != 3 {
		t.Fatal("row limit treated as exhaustion")
	}
	receipt, err = f.client.Read(deadline(t), correlation("filter"), "data", BatchRead{Predicates: []Predicate{{Column: "id", Operator: "ge", Value: "4090"}}})
	inspectRows(t, requireOK(t, settle(t, receipt, err)), expectedRows(4090, 7, "original"))
	receipt, err = f.client.Write(deadline(t), correlation("overwrite"), "data", BatchWrite{Mode: Overwrite,
		Predicates: []Predicate{{Column: "id", Operator: "lt", Value: "8"}}, Batches: []arrow.RecordBatch{makeBatch(t, 0, 8, "replacement")}})
	requireOK(t, settle(t, receipt, err))
	for id, value := range expectedRows(0, 8, "replacement") {
		original[id] = value
	}
	inspectRows(t, readAll(t, f), original)
	receipt, err = f.client.Write(deadline(t), correlation("delete"), "data", BatchWrite{Mode: Delete, Predicates: []Predicate{{Column: "id", Operator: "ge", Value: "4090"}}})
	requireOK(t, settle(t, receipt, err))
	for id := int64(4090); id < 4097; id++ {
		delete(original, id)
	}
	inspectRows(t, readAll(t, f), original)
	receipt, err = f.client.Read(deadline(t), correlation("historical"), "data", BatchRead{SnapshotID: originalSnapshot})
	inspectRows(t, requireOK(t, settle(t, receipt, err)), expectedRows(0, 4097, "original"))
	appendBatch(t, f, 5000, 23, "second")
	for id, value := range expectedRows(5000, 23, "second") {
		original[id] = value
	}
	receipt, err = f.client.Compact(deadline(t), correlation("compact"), "data")
	requireOK(t, settle(t, receipt, err))
	inspectRows(t, readAll(t, f), original)
	receipt, err = f.client.EvolveSchema(deadline(t), correlation("schema"), "data", []SchemaChange{{Operation: "add", Name: "extra", Type: "double"}, {Operation: "rename", Name: "value", NewName: "renamed"}})
	requireOK(t, settle(t, receipt, err))
	read := readAll(t, f)
	inspectRows(t, read, original)
	copied := read.MetadataCopy()
	for i := range copied {
		copied[i] = 0
	}
	if bytes.Equal(copied, read.MetadataCopy()) {
		t.Fatal("metadata aliases result")
	}
	receipt, err = f.client.ListTables(deadline(t), correlation("list"))
	listed := requireOK(t, settle(t, receipt, err))
	if !listed.Complete() || len(listed.NamesCopy()) != 1 || listed.NamesCopy()[0] != "data" {
		t.Fatal("listing mismatch")
	}
	peer.mu.Lock()
	commits, loads, objects := peer.commits, peer.loads, len(peer.objects)
	peer.mu.Unlock()
	if commits < 6 || loads <= commits || objects == 0 {
		t.Fatal("fixture saw no commit/reload/file distinction")
	}
	for f.inbox.Usage().Outstanding > 0 {
		delivery, err := f.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		result, err := delivery.Receipt().WaitReleased(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if result.Context.Correlation.Call == "append" {
			conformance.Result(t, result, conformance.Expected[Result]{Context: fault.Context{Provider: ProviderID, Source: "lake", Scope: "test", Operation: "write", Correlation: correlation("append")}, Source: f.client.access.Info(), Limits: f.client.access.Limits(),
				Shape: invocation.Async, Present: true, Final: true, Released: true, Attempts: invocation.Attempts{Exact: false},
				Value: func(t testing.TB, value Result) {
					if value.Effect() != Acknowledged || !value.Reloaded() {
						t.Error("independent evidence lost")
					}
				}})
		}
		if err = delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/apache/iceberg-go", "github.com/apache/arrow-go/v18", "github.com/aws/aws-sdk-go-v2/service/s3"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, f.client.access, f.client.Profile(), []compatibility.Requirement{{Guarantee: "table-commit", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}}}, nil)
	if err != nil || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("source profile invented service compatibility")
	}
}
func TestPartitionedAppendAndOwnership(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 16)
	setupTable(t, f)
	receipt, err := f.client.EvolvePartitions(deadline(t), correlation("partition"), "data", []PartitionChange{{Operation: "bucket", Column: "id", Name: "id_bucket", Buckets: 4}})
	requireOK(t, settle(t, receipt, err))
	batch := makeBatch(t, 0, 127, "bucket")
	receipt, err = f.client.Write(deadline(t), correlation("copied"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{batch}})
	batch.Column(0).(*array.Int64).Int64Values()[0] = -100
	requireOK(t, settle(t, receipt, err))
	inspectRows(t, readAll(t, f), expectedRows(0, 127, "bucket"))
	receipt, err = f.client.EvolvePartitions(deadline(t), correlation("unpartition"), "data", []PartitionChange{{Operation: "drop", Name: "id_bucket"}})
	requireOK(t, settle(t, receipt, err))
	receipt, err = f.client.EvolvePartitions(deadline(t), correlation("new-partition"), "data", []PartitionChange{{Operation: "identity", Column: "id", Name: "id_identity"}})
	requireOK(t, settle(t, receipt, err))
}
func TestCanceledAdmissionAndEvidenceSaturation(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 1)
	ctx, cancel := context.WithCancel(deadline(t))
	cancel()
	peer.mu.Lock()
	before := peer.requests
	peer.mu.Unlock()
	if receipt, err := f.client.ListTables(ctx, correlation("canceled")); receipt != nil || err == nil {
		t.Fatal("canceled admission accepted")
	}
	peer.mu.Lock()
	after := peer.requests
	peer.mu.Unlock()
	if before != after {
		t.Fatal("canceled call performed I/O")
	}
	receipt, err := f.client.ListTables(deadline(t), correlation("first"))
	requireOK(t, settle(t, receipt, err))
	peer.mu.Lock()
	before = peer.requests
	peer.mu.Unlock()
	if receipt, err = f.client.ListTables(deadline(t), correlation("saturated")); receipt != nil || err == nil {
		t.Fatal("full evidence path accepted")
	}
	peer.mu.Lock()
	after = peer.requests
	peer.mu.Unlock()
	if before != after {
		t.Fatal("evidence saturation performed I/O")
	}
}
