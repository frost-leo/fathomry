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
	"iter"
	"reflect"
	"slices"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
)

// ManagedScanOptions describes all native scan selections without giving an
// option callback a raw Scan. Filter and Reporter are borrowed cooperative code.
type ManagedScanOptions struct {
	Fields          []string
	Filter          native.BooleanExpression
	SnapshotID      int64
	AsOfMillis      *int64
	Ref             string
	CaseInsensitive bool
	Limit           int64
	BatchSize       int
	Concurrency     int
	Planning        table.ScanPlanningMode
	Properties      native.Properties
	Reporter        metrics.Reporter
	RowLineage      bool
}
type ManagedScan struct {
	private
	scope *ManagedSession
	scan  *table.Scan
	limit int64
}
type ScanSummary struct {
	Rows, Bytes int64
	Complete    bool
}

func (f *ManagedSession) scanOptions(o ManagedScanOptions) ([]table.ScanOption, error) {
	s := f.source.prepared.settings
	if len(o.Fields) > s.MaxListEntries || o.SnapshotID < 0 || o.Limit < 0 || o.Limit > s.MaxRows || o.BatchSize < 0 || int64(o.BatchSize) > s.MaxRows || o.Concurrency < 0 || o.Concurrency > s.Workers || typedNil(o.Reporter) || typedNil(o.Filter) {
		return nil, failure(ErrInput, "scan-options")
	}
	if o.SnapshotID != 0 && o.AsOfMillis != nil || o.Ref != "" && (o.SnapshotID != 0 || o.AsOfMillis != nil) {
		return nil, failure(ErrInput, "snapshot-selection")
	}
	props, err := f.boundedProperties(o.Properties)
	if err != nil {
		return nil, err
	}
	opts := []table.ScanOption{table.WithCaseSensitive(!o.CaseInsensitive), table.WithOptions(props)}
	if len(o.Fields) > 0 {
		opts = append(opts, table.WithSelectedFields(slices.Clone(o.Fields)...))
	}
	if o.Filter != nil {
		opts = append(opts, table.WithRowFilter(o.Filter))
	}
	if o.SnapshotID != 0 {
		opts = append(opts, table.WithSnapshotID(o.SnapshotID))
	}
	if o.AsOfMillis != nil {
		opts = append(opts, table.WithSnapshotAsOf(*o.AsOfMillis))
	}
	if o.Limit > 0 {
		opts = append(opts, table.WithLimit(o.Limit))
	}
	if o.Planning != "" {
		opts = append(opts, table.WithScanPlanningMode(o.Planning))
	}
	if o.Reporter != nil {
		opts = append(opts, table.WithReporter(&managedReporter{source: f.source, reporter: o.Reporter}))
	}
	if o.RowLineage {
		opts = append(opts, table.WithRowLineage())
	}
	workers := o.Concurrency
	if workers == 0 {
		workers = s.Workers
	}
	batch := o.BatchSize
	if batch == 0 {
		batch = int(min(s.MaxRows, 1024))
	}
	opts = append(opts, table.WithMaxConcurrency(workers), table.WithArrowBatchSize(batch))
	return opts, nil
}
func (t *ManagedTable) Scan(o ManagedScanOptions) (*ManagedScan, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "scan", grantRead, func(context.Context) (*ManagedScan, error) {
		opts, err := t.scope.scanOptions(o)
		if err != nil {
			return nil, err
		}
		scan := t.table.Scan(opts...)
		if o.Ref != "" {
			scan, err = scan.UseRef(o.Ref)
			if err != nil {
				return nil, err
			}
		}
		if err = t.scope.handle(); err != nil {
			t.scope.noteCleanup(scan.Close())
			return nil, err
		}
		t.scope.closers = append(t.scope.closers, scan)
		limit := o.Limit
		if limit == 0 {
			limit = table.ScanNoLimit
		}
		return &ManagedScan{scope: t.scope, scan: scan, limit: limit}, nil
	})
}
func (t *ManagedTransaction) Scan(o ManagedScanOptions) (*ManagedScan, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "staged-scan", grantRead, func(context.Context) (*ManagedScan, error) {
		opts, err := t.scope.scanOptions(o)
		if err != nil {
			return nil, err
		}
		scan, err := t.transaction.Scan(opts...)
		if err != nil {
			return nil, err
		}
		if o.Ref != "" {
			scan, err = scan.UseRef(o.Ref)
			if err != nil {
				return nil, err
			}
		}
		if err = t.scope.handle(); err != nil {
			t.scope.noteCleanup(scan.Close())
			return nil, err
		}
		t.scope.closers = append(t.scope.closers, scan)
		limit := o.Limit
		if limit == 0 {
			limit = table.ScanNoLimit
		}
		return &ManagedScan{scope: t.scope, scan: scan, limit: limit}, nil
	})
}
func (s *ManagedScan) valid() error {
	if s == nil || s.scope == nil || s.scan == nil {
		return failure(ErrInput, "scan")
	}
	return nil
}
func (s *ManagedScan) Projection() (*native.Schema, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	return managedCall(s.scope, "projection", grantRead, func(context.Context) (*native.Schema, error) {
		schema, err := s.scan.Projection()
		if err != nil {
			return nil, err
		}
		var copy native.Schema
		err = s.scope.cloneData(schema, &copy)
		return &copy, err
	})
}
func (s *ManagedScan) PlanFiles() ([]table.FileScanTask, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	return managedCall(s.scope, "plan-files", grantRead, func(ctx context.Context) ([]table.FileScanTask, error) {
		tasks, err := s.scan.PlanFiles(ctx)
		if err != nil {
			return nil, err
		}
		if err = s.scope.validateScanTasks(tasks); err != nil {
			return nil, err
		}
		return tasks, nil
	})
}
func (f *ManagedSession) validateScanTasks(tasks []table.FileScanTask) error {
	if len(tasks) > f.source.prepared.settings.MaxListEntries {
		return failure(ErrLimit, "scan-tasks")
	}
	for _, task := range tasks {
		files := append([]native.DataFile{task.File}, task.DeleteFiles...)
		files = append(files, task.EqualityDeleteFiles...)
		files = append(files, task.DeletionVectorFiles...)
		if len(files) > f.source.prepared.settings.MaxListEntries {
			return failure(ErrLimit, "delete-files")
		}
		for _, file := range files {
			if file == nil {
				return failure(ErrProtocol, "scan-file")
			}
			if err := f.location(file.FilePath()); err != nil {
				return err
			}
			if file.FileSizeBytes() < 0 || file.FileSizeBytes() > f.source.prepared.settings.MaxObjectBytes {
				return failure(ErrLimit, "scan-file")
			}
		}
	}
	return nil
}

// RecordBytes counts retained backing buffers, including dictionary/child data
// and parents of sliced buffers. Shared Buffer objects are counted once.
func RecordBytes(record arrow.RecordBatch) int64 {
	if record == nil || typedNil(record) {
		return 0
	}
	seen := map[*memory.Buffer]bool{}
	visited := map[arrow.ArrayData]bool{}
	var total int64
	var visit func(arrow.ArrayData)
	visit = func(data arrow.ArrayData) {
		if data == nil || reflect.ValueOf(data).Kind() == reflect.Pointer && reflect.ValueOf(data).IsNil() || visited[data] {
			return
		}
		visited[data] = true
		for _, buffer := range data.Buffers() {
			if buffer == nil {
				continue
			}
			for buffer.Parent() != nil {
				buffer = buffer.Parent()
			}
			if !seen[buffer] {
				seen[buffer] = true
				total += int64(buffer.Cap())
			}
		}
		for _, child := range data.Children() {
			visit(child)
		}
		visit(data.Dictionary())
	}
	for _, column := range record.Columns() {
		visit(column.Data())
	}
	return total
}

func (f *ManagedSession) consume(ctx context.Context, cancel context.CancelFunc, records iter.Seq2[arrow.RecordBatch, error], visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	var result ScanSummary
	if visit == nil {
		return result, failure(ErrInput, "record-visitor")
	}
	for record, err := range records {
		if err != nil {
			if record != nil && !typedNil(record) {
				record.Release()
			}
			cancel()
			return result, err
		}
		if record == nil || typedNil(record) {
			cancel()
			return result, failure(ErrProtocol, "nil-record")
		}
		if ctx.Err() != nil {
			record.Release()
			cancel()
			return result, errors.Join(ctx.Err(), context.Cause(ctx))
		}
		count := RecordBytes(record)
		if record.NumRows() < 0 || record.NumRows() > f.source.prepared.settings.MaxRows-result.Rows || count > f.source.prepared.settings.MaxBatchBytes || count > f.source.prepared.settings.MaxIOBytes-result.Bytes {
			record.Release()
			cancel()
			return result, failure(ErrLimit, "record")
		}
		result.Rows += record.NumRows()
		result.Bytes += count
		f.mu.Lock()
		f.data.rows += record.NumRows()
		f.data.bytes += count
		f.mu.Unlock()
		var next bool
		func() {
			returned := false
			defer func() {
				record.Release()
				if !returned {
					cancel()
				}
			}()
			next, err = visit(record)
			returned = true
		}()
		if err != nil {
			cancel()
			return result, err
		}
		if !next {
			cancel()
			return result, nil
		}
	}
	if ctx.Err() != nil {
		return result, errors.Join(ctx.Err(), context.Cause(ctx))
	}
	result.Complete = true
	return result, nil
}

// Records lends each record only during visit; Retain transfers an additional
// caller-owned reference. Early stop cancels before native iterator join.
func (s *ManagedScan) Records(visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := s.valid(); err != nil {
		return ScanSummary{}, err
	}
	return managedCall(s.scope, "records", grantRead, func(ctx context.Context) (ScanSummary, error) {
		if visit == nil {
			return ScanSummary{}, failure(ErrInput, "visitor")
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		_, records, err := s.scan.ToArrowRecords(child)
		if err != nil {
			return ScanSummary{}, err
		}
		return s.scope.consume(child, cancel, records, visit)
	})
}
func (s *ManagedScan) ReadTasks(tasks []table.FileScanTask, visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := s.valid(); err != nil {
		return ScanSummary{}, err
	}
	return managedCall(s.scope, "read-tasks", grantRead, func(ctx context.Context) (ScanSummary, error) {
		if visit == nil {
			return ScanSummary{}, failure(ErrInput, "visitor")
		}
		if !s.scope.allowed(grantImport) {
			return ScanSummary{}, failure(ErrAuthority, "task-adoption")
		}
		if err := s.scope.validateScanTasks(tasks); err != nil {
			return ScanSummary{}, err
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		_, records, err := s.scan.ReadTasks(child, slices.Clone(tasks))
		if err != nil {
			return ScanSummary{}, err
		}
		return s.scope.consume(child, cancel, records, visit)
	})
}
func (s *ManagedScan) ToArrowTable() (*ManagedArrowTable, ScanSummary, error) {
	if err := s.valid(); err != nil {
		return nil, ScanSummary{}, err
	}
	type output struct {
		table   *ManagedArrowTable
		summary ScanSummary
	}
	out, err := managedCall(s.scope, "arrow-table", grantRead, func(ctx context.Context) (output, error) {
		if err := s.scope.handle(); err != nil {
			return output{}, err
		}
		s.scope.mu.Lock()
		available := s.scope.source.prepared.settings.MaxBatchBytes - s.scope.outputBytes
		s.scope.mu.Unlock()
		if available <= 0 {
			return output{}, failure(ErrLimit, "outstanding-output")
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		schema, records, err := s.scan.ToArrowRecords(child)
		if err != nil {
			return output{}, err
		}
		var kept []arrow.RecordBatch
		defer func() {
			for _, record := range kept {
				record.Release()
			}
		}()
		var retained int64
		summary, err := s.scope.consume(child, cancel, records, func(record arrow.RecordBatch) (bool, error) {
			count := RecordBytes(record)
			if count > available-retained {
				return false, failure(ErrLimit, "table-bytes")
			}
			retained += count
			record.Retain()
			kept = append(kept, record)
			return true, nil
		})
		if len(kept) == 0 && err != nil {
			summary.Rows = 0
			summary.Bytes = 0
			return output{summary: summary}, err
		}
		summary.Rows = 0
		summary.Bytes = retained
		for _, record := range kept {
			summary.Rows += record.NumRows()
		}
		owned := &ManagedArrowTable{scope: s.scope, table: array.NewTableFromRecords(schema, kept), bytes: retained}
		s.scope.mu.Lock()
		s.scope.outputBytes += retained
		s.scope.mu.Unlock()
		s.scope.closers = append(s.scope.closers, owned)
		return output{table: owned, summary: summary}, err
	})
	return out.table, out.summary, err
}

// ManagedArrowTable owns materialized buffers within one admitted family. Visit
// borrows the native table; the callback must not retain it or its columns.
// Close is repeatable and releases its output reservation without fresh admission.
type ManagedArrowTable struct {
	private
	scope *ManagedSession
	mu    sync.Mutex
	table arrow.Table
	bytes int64
}

func (t *ManagedArrowTable) Visit(visit func(arrow.Table) error) error {
	if t == nil || t.scope == nil || visit == nil {
		return failure(ErrInput, "arrow-output")
	}
	return managedDo(t.scope, "arrow-output", grantRead, func(context.Context) error {
		if !t.mu.TryLock() {
			return failure(ErrAuthority, "output-busy")
		}
		defer t.mu.Unlock()
		if t.table == nil {
			return failure(ErrAuthority, "output-closed")
		}
		return visit(t.table)
	})
}
func (t *ManagedArrowTable) Close() error {
	if t == nil || t.scope == nil {
		return failure(ErrInput, "arrow-output")
	}
	if !t.mu.TryLock() {
		return failure(ErrAuthority, "output-busy")
	}
	defer t.mu.Unlock()
	if t.table != nil {
		t.table.Release()
		t.table = nil
		t.scope.mu.Lock()
		t.scope.outputBytes -= t.bytes
		t.scope.mu.Unlock()
	}
	return nil
}
func (s *ManagedScan) Close() error {
	if err := s.valid(); err != nil {
		return err
	}
	if !s.scope.gate.TryLock() {
		return failure(ErrAuthority, "scan-busy")
	}
	defer s.scope.gate.Unlock()
	err := s.scan.Close()
	s.scope.mu.Lock()
	if err != nil && !s.scope.closed {
		s.scope.cleanup = append(s.scope.cleanup, err)
	}
	s.scope.mu.Unlock()
	return failureIf(err)
}

// IncrementalRange uses zero From for the beginning of the retained ancestry
// and zero To for the ScanOptions selection (current snapshot by default).
type IncrementalRange struct {
	From, To  int64
	Inclusive bool
}

func (t *ManagedTable) IncrementalAppend(o ManagedScanOptions, r IncrementalRange) ([]table.FileScanTask, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "incremental-append", grantRead, func(ctx context.Context) ([]table.FileScanTask, error) {
		return t.incrementalAppend(ctx, o, r)
	})
}

func (t *ManagedTable) incrementalAppend(ctx context.Context, o ManagedScanOptions, r IncrementalRange) ([]table.FileScanTask, error) {
	opts, err := t.scope.scanOptions(o)
	if err != nil {
		return nil, err
	}
	scan := t.table.NewIncrementalAppendScan(opts...)
	if o.Ref != "" {
		scan, err = scan.UseRef(o.Ref)
		if err != nil {
			return nil, err
		}
	}
	if r.From != 0 {
		if r.Inclusive {
			scan = scan.FromSnapshotInclusive(r.From)
		} else {
			scan = scan.FromSnapshotExclusive(r.From)
		}
	}
	if r.To != 0 {
		scan = scan.ToSnapshot(r.To)
	}
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	return tasks, t.scope.validateScanTasks(tasks)
}
func (t *ManagedTable) IncrementalChangelog(o ManagedScanOptions, r IncrementalRange) ([]table.ChangelogScanTask, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "incremental-changelog", grantRead, func(ctx context.Context) ([]table.ChangelogScanTask, error) {
		return t.incrementalChangelog(ctx, o, r)
	})
}

func (t *ManagedTable) incrementalChangelog(ctx context.Context, o ManagedScanOptions, r IncrementalRange) ([]table.ChangelogScanTask, error) {
	opts, err := t.scope.scanOptions(o)
	if err != nil {
		return nil, err
	}
	scan := t.table.NewIncrementalChangelogScan(opts...)
	if o.Ref != "" {
		scan, err = scan.UseRef(o.Ref)
		if err != nil {
			return nil, err
		}
	}
	if r.From != 0 {
		if r.Inclusive {
			scan = scan.FromSnapshotInclusive(r.From)
		} else {
			scan = scan.FromSnapshotExclusive(r.From)
		}
	}
	if r.To != 0 {
		scan = scan.ToSnapshot(r.To)
	}
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	if len(tasks) > t.scope.source.prepared.settings.MaxListEntries {
		return nil, failure(ErrLimit, "changelog-tasks")
	}
	for _, task := range tasks {
		if err = t.scope.validateScanTasks([]table.FileScanTask{task.ScanTask()}); err != nil {
			return nil, err
		}
	}
	return tasks, nil
}

func (t *ManagedTable) incrementalReader(o ManagedScanOptions, r IncrementalRange) (*table.Scan, error) {
	if r.To != 0 {
		o.SnapshotID, o.AsOfMillis, o.Ref = r.To, nil, ""
	}
	options, err := t.scope.scanOptions(o)
	if err != nil {
		return nil, err
	}
	scan := t.table.Scan(options...)
	if o.Ref != "" {
		scan, err = scan.UseRef(o.Ref)
	}
	return scan, err
}

// IncrementalAppendRecords consumes its own plan without raw-task import
// authority. Projection and row limits apply to the selected append tasks.
func (t *ManagedTable) IncrementalAppendRecords(o ManagedScanOptions, r IncrementalRange, visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := t.valid(); err != nil {
		return ScanSummary{}, err
	}
	return managedCall(t.scope, "incremental-append-records", grantRead, func(ctx context.Context) (ScanSummary, error) {
		if visit == nil {
			return ScanSummary{}, failure(ErrInput, "visitor")
		}
		tasks, err := t.incrementalAppend(ctx, o, r)
		if err != nil {
			return ScanSummary{}, err
		}
		scan, err := t.incrementalReader(o, r)
		if err != nil {
			return ScanSummary{}, err
		}
		defer func() { t.scope.noteCleanup(scan.Close()) }()
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		_, records, err := scan.ReadTasks(child, tasks)
		if err != nil {
			return ScanSummary{}, err
		}
		return t.scope.consume(child, cancel, records, visit)
	})
}

type ManagedChange struct {
	Operation  table.ChangelogOperation
	Ordinal    int
	SnapshotID int64
}

// IncrementalChangelogRecords streams native task order with each batch's
// change identity. It does not invent row-level changelogs for unsupported native
// delete manifests. Limit applies to the whole requested changelog, not each task.
func (t *ManagedTable) IncrementalChangelogRecords(o ManagedScanOptions, r IncrementalRange, visit func(ManagedChange, arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := t.valid(); err != nil {
		return ScanSummary{}, err
	}
	return managedCall(t.scope, "incremental-changelog-records", grantRead, func(ctx context.Context) (ScanSummary, error) {
		if visit == nil {
			return ScanSummary{}, failure(ErrInput, "visitor")
		}
		tasks, err := t.incrementalChangelog(ctx, o, r)
		if err != nil {
			return ScanSummary{}, err
		}
		scan, err := t.incrementalReader(o, r)
		if err != nil {
			return ScanSummary{}, err
		}
		defer func() { t.scope.noteCleanup(scan.Close()) }()
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		var change ManagedChange
		records := func(yield func(arrow.RecordBatch, error) bool) {
			var rows int64
			for _, task := range tasks {
				if o.Limit > 0 && rows >= o.Limit {
					return
				}
				selected := scan
				if o.Limit > 0 {
					selected = scan.UseRowLimit(o.Limit - rows)
				}
				change = ManagedChange{Operation: task.Operation(), Ordinal: task.ChangeOrdinal(), SnapshotID: task.CommitSnapshotID()}
				if !func() bool {
					if selected != scan {
						defer func() { t.scope.noteCleanup(selected.Close()) }()
					}
					_, batches, err := selected.ReadTasks(child, []table.FileScanTask{task.ScanTask()})
					if err != nil {
						yield(nil, err)
						return false
					}
					for record, err := range batches {
						if record != nil && !typedNil(record) {
							rows += record.NumRows()
						}
						if !yield(record, err) {
							return false
						}
					}
					return true
				}() {
					return
				}
			}
		}
		return t.scope.consume(child, cancel, records, func(record arrow.RecordBatch) (bool, error) { return visit(change, record) })
	})
}

// Inspect streams the chosen native metadata table and owns its RecordReader.
func (t *ManagedTable) Inspect(name string, allocator memory.Allocator, visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := t.valid(); err != nil {
		return ScanSummary{}, err
	}
	return managedCall(t.scope, "inspect", grantRead, func(ctx context.Context) (ScanSummary, error) {
		if typedNil(allocator) || visit == nil {
			return ScanSummary{}, failure(ErrInput, "inspection-input")
		}
		inspector := t.table.Inspect(table.WithInspectAllocator(allocator))
		methods := map[string]func(context.Context) (array.RecordReader, error){
			"history": inspector.History, "snapshots": inspector.Snapshots, "manifests": inspector.Manifests, "refs": inspector.Refs, "metadata-log": inspector.MetadataLogEntries,
			"entries": inspector.Entries, "all-entries": inspector.AllEntries, "files": inspector.Files, "data-files": inspector.DataFiles, "delete-files": inspector.DeleteFiles,
			"all-files": inspector.AllFiles, "all-data-files": inspector.AllDataFiles, "all-delete-files": inspector.AllDeleteFiles, "all-manifests": inspector.AllManifests,
			"partitions": inspector.Partitions, "position-deletes": inspector.PositionDeletes,
		}
		method, ok := methods[name]
		if !ok {
			return ScanSummary{}, failure(ErrInput, "inspection")
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		reader, err := method(child)
		if err != nil {
			return ScanSummary{}, err
		}
		defer reader.Release()
		return t.scope.consume(child, cancel, array.IterFromReader(reader), visit)
	})
}
