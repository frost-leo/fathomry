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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

type ScanOptions struct {
	Fields          []string
	Filter          sdk.BooleanExpression
	SnapshotID      int64
	AsOfMillis      *int64
	Ref             string
	CaseInsensitive bool
	Limit           int64
	BatchSize       int
	Concurrency     int
	Planning        table.ScanPlanningMode
	Properties      sdk.Properties
	Reporter        metrics.Reporter
	RowLineage      bool
}

type Scan struct {
	private
	native *native.ManagedScan
}
type ScanSummary struct {
	Rows, Bytes int64
	Complete    bool
}

// IncrementalRange uses zero From for the beginning of retained ancestry.
// A nonzero To overrides the ScanOptions ending snapshot/ref/time selection.
// Zero To uses that selection, or the current snapshot by default.
type IncrementalRange struct {
	From, To  int64
	Inclusive bool
}

func (s *Scan) valid() error {
	if s == nil || s.native == nil {
		return fail(ErrState, "scan")
	}
	return nil
}
func (t *Table) Scan(options ScanOptions) (*Scan, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.Scan(native.ManagedScanOptions(options))
	if v == nil {
		return nil, translate(err, "scan")
	}
	return &Scan{native: v}, translate(err, "scan")
}
func (t *Transaction) Scan(options ScanOptions) (*Scan, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.Scan(native.ManagedScanOptions(options))
	if v == nil {
		return nil, translate(err, "scan")
	}
	return &Scan{native: v}, translate(err, "scan")
}

func (s *Scan) Projection() (*sdk.Schema, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.Projection()
	return v, translate(err, "projection")
}
func (s *Scan) PlanFiles() ([]table.FileScanTask, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.PlanFiles()
	return v, translate(err, "plan-files")
}
func (s *Scan) Records(visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := s.valid(); err != nil {
		return ScanSummary{}, err
	}
	v, err := s.native.Records(visit)
	return ScanSummary(v), translate(err, "records")
}
func (s *Scan) ReadTasks(tasks []table.FileScanTask, visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := s.valid(); err != nil {
		return ScanSummary{}, err
	}
	v, err := s.native.ReadTasks(tasks, visit)
	return ScanSummary(v), translate(err, "read-tasks")
}
func (s *Scan) ToArrowTable() (*ArrowTable, ScanSummary, error) {
	if err := s.valid(); err != nil {
		return nil, ScanSummary{}, err
	}
	v, summary, err := s.native.ToArrowTable()
	if v == nil {
		return nil, ScanSummary(summary), translate(err, "arrow-table")
	}
	return &ArrowTable{native: v}, ScanSummary(summary), translate(err, "arrow-table")
}

// ArrowTable is a scoped owned materialization. Visit borrows native data and
// Close releases its reservation. Callback return also closes ignored outputs.
type ArrowTable struct {
	private
	native *native.ManagedArrowTable
}

func (t *ArrowTable) Visit(visit func(arrow.Table) error) error {
	if t == nil || t.native == nil {
		return fail(ErrState, "arrow-output")
	}
	return translate(t.native.Visit(visit), "arrow-output")
}
func (t *ArrowTable) Close() error {
	if t == nil || t.native == nil {
		return fail(ErrState, "arrow-output")
	}
	return translate(t.native.Close(), "arrow-output-close")
}
func (s *Scan) Close() error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.Close(), "close-scan")
}
func (t *Table) Inspect(name string, allocator memory.Allocator, visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := t.valid(); err != nil {
		return ScanSummary{}, err
	}
	v, err := t.native.Inspect(name, allocator, visit)
	return ScanSummary(v), translate(err, "inspect")
}
func (t *Table) IncrementalAppend(options ScanOptions, span IncrementalRange) ([]table.FileScanTask, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.IncrementalAppend(native.ManagedScanOptions(options), native.IncrementalRange(span))
	return v, translate(err, "incremental-append")
}
func (t *Table) IncrementalChangelog(options ScanOptions, span IncrementalRange) ([]table.ChangelogScanTask, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.IncrementalChangelog(native.ManagedScanOptions(options), native.IncrementalRange(span))
	return v, translate(err, "incremental-changelog")
}

// IncrementalAppendRecords reads an origin-bound append plan with read permission;
// unlike adopting detached tasks, it does not require import permission.
func (t *Table) IncrementalAppendRecords(options ScanOptions, span IncrementalRange, visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := t.valid(); err != nil {
		return ScanSummary{}, err
	}
	value, err := t.native.IncrementalAppendRecords(native.ManagedScanOptions(options), native.IncrementalRange(span), visit)
	return ScanSummary(value), translate(err, "incremental-append-records")
}

// Change identifies a batch in the native incremental changelog order.
type Change struct {
	Operation  table.ChangelogOperation
	Ordinal    int
	SnapshotID int64
}

// IncrementalChangelogRecords lends batches in native change order. The row limit
// covers the whole changelog. Native unsupported delete-manifest refusal survives.
func (t *Table) IncrementalChangelogRecords(options ScanOptions, span IncrementalRange, visit func(Change, arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if err := t.valid(); err != nil {
		return ScanSummary{}, err
	}
	if visit == nil {
		return ScanSummary{}, fail(ErrInput, "visitor")
	}
	value, err := t.native.IncrementalChangelogRecords(native.ManagedScanOptions(options), native.IncrementalRange(span), func(change native.ManagedChange, record arrow.RecordBatch) (bool, error) {
		return visit(Change(change), record)
	})
	return ScanSummary(value), translate(err, "incremental-changelog-records")
}

func RecordBytes(record arrow.RecordBatch) int64 { return native.RecordBytes(record) }
