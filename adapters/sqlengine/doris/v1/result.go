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

package doris

import (
	"slices"

	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
)

// Result is immutable public evidence, copied before native custody is released.
// Explicit getters are sensitive; no runtime value is a durable JSON contract.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	source                                      sqlengine.Info
	attribution                                 sqlengine.Attribution
	attempts                                    sqlengine.Attempts
	present, complete, dispatched, acknowledged bool
	affected                                    int64
	affectedKnown                               bool
	serverGreeting                              string
	columns                                     []Column
	rows                                        []Row
	load                                        LoadEvidence
	loadPresent                                 bool
	rowsRead, bytesRead, responseBytes          int
}

// Column preserves positional metadata, including duplicate names and presence.
type Column struct {
	private
	Name, DatabaseType      string
	Nullable, NullableKnown bool
	Precision, Scale        int64
	PrecisionKnown          bool
}
type cell struct {
	text string
	null bool
}

// Row holds immutable exact native encodings, not mutable driver storage.
type Row struct {
	private
	cells []cell
}

// ValuesCopy distinguishes SQL NULL (nil) from empty bytes (non-nil).
func (row Row) ValuesCopy() [][]byte {
	values := make([][]byte, len(row.cells))
	for index, value := range row.cells {
		if !value.null {
			values[index] = []byte(value.text)
		}
	}
	return values
}

// LoadState describes retained load/label observations, not retry policy.
type LoadState uint8

const (
	LoadUnknown LoadState = iota
	LoadNotDispatched
	LoadPending
	LoadCommitted
	LoadVisible
	LoadRejected
	LoadAborted
)

// LoadEvidence preserves effect, identity, row quality and duplicate facts
// independently. A visible inspected label has no payload/row witness.
type LoadEvidence struct {
	private
	Database, Table, Label                              string
	PayloadSHA256                                       [32]byte
	State                                               LoadState
	HTTPStatus                                          int
	TransactionID                                       int64
	TransactionKnown                                    bool
	RowsKnown                                           bool
	TotalRows, LoadedRows, FilteredRows, UnselectedRows int64
	Duplicate                                           bool
	ExistingJobStatus                                   string
}

func (r Result) HasData() bool         { return r.data != nil && r.data.present }
func (r Result) Complete() bool        { return r.data != nil && r.data.complete }
func (r Result) Dispatched() bool      { return r.data != nil && r.data.dispatched }
func (r Result) SQLAcknowledged() bool { return r.data != nil && r.data.acknowledged }
func (r Result) RowsAffected() (int64, bool) {
	if r.data == nil {
		return 0, false
	}
	return r.data.affected, r.data.affectedKnown
}
func (r Result) ServerGreeting() string {
	if r.data == nil {
		return ""
	}
	return r.data.serverGreeting
}
func (r Result) Source() sqlengine.Info {
	if r.data == nil {
		return sqlengine.Info{}
	}
	return r.data.source.Clone()
}
func (r Result) Attribution() sqlengine.Attribution {
	if r.data == nil {
		return sqlengine.Attribution{}
	}
	return r.data.attribution
}
func (r Result) Attempts() sqlengine.Attempts {
	if r.data == nil {
		return sqlengine.Attempts{}
	}
	return r.data.attempts
}
func (r Result) ColumnsCopy() []Column {
	if r.data == nil {
		return nil
	}
	return slices.Clone(r.data.columns)
}
func (r Result) RowsCopy() []Row {
	if r.data == nil {
		return nil
	}
	return slices.Clone(r.data.rows)
}
func (r Result) Load() (LoadEvidence, bool) {
	if r.data == nil {
		return LoadEvidence{}, false
	}
	return r.data.load, r.data.loadPresent
}

// RowsRead/BytesRead include bounded cursor lookahead and are cumulative.
// BytesRead includes one metadata copy; ResponseBytes counts whole-wire framing.
// Legacy finite and HTTP results report zero for these cursor-only counters.
func (r Result) RowsRead() int {
	if r.data == nil {
		return 0
	}
	return r.data.rowsRead
}
func (r Result) BytesRead() int {
	if r.data == nil {
		return 0
	}
	return r.data.bytesRead
}
func (r Result) ResponseBytes() int {
	if r.data == nil {
		return 0
	}
	return r.data.responseBytes
}

func info(value source.Info) sqlengine.Info {
	result := sqlengine.Info{Scope: value.Scope, Provider: value.Configuration.Identity.Provider, Name: value.Configuration.Identity.Name,
		FormatVersion: value.Configuration.Format, Revision: value.Configuration.Revision}
	for _, layer := range value.Configuration.Provenance {
		result.Provenance = append(result.Provenance, sqlengine.LayerInfo{Kind: uint8(layer.Kind), Fields: slices.Clone(layer.Fields)})
	}
	return result
}
func project(value invocation.Result[native.Result], metadata adapters.Info) Result {
	facts := value.Outcome.Value
	data := &resultData{source: info(value.Source), attribution: sqlengine.Attribution{Runtime: metadata.Runtime, Operation: metadata.Operation,
		ID: metadata.ID, Sequence: metadata.Sequence, Parent: metadata.Parent, Depth: metadata.Depth, Source: metadata.Source},
		attempts: sqlengine.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}, present: value.Outcome.Present,
		complete: facts.Complete(), dispatched: facts.Dispatched(), acknowledged: facts.SQLAcknowledged(), serverGreeting: facts.ServerGreeting(),
		rowsRead: facts.RowsRead(), bytesRead: facts.BytesRead(), responseBytes: facts.ResponseBytes()}
	data.affected, data.affectedKnown = facts.RowsAffected()
	for _, column := range facts.ColumnsCopy() {
		data.columns = append(data.columns, Column{Name: column.Name, DatabaseType: column.DatabaseType,
			Nullable: column.Nullable, NullableKnown: column.NullableKnown, Precision: column.Precision, Scale: column.Scale, PrecisionKnown: column.PrecisionKnown})
	}
	if rows := facts.RowsCopy(); rows != nil {
		data.rows = make([]Row, len(rows))
		for index, row := range rows {
			values := row.ValuesCopy()
			data.rows[index].cells = make([]cell, len(values))
			for column, value := range values {
				data.rows[index].cells[column] = cell{text: string(value), null: value == nil}
			}
		}
	}
	if load, ok := facts.Load(); ok {
		data.loadPresent = true
		data.load = LoadEvidence{Database: load.Database, Table: load.Table, Label: load.Label, PayloadSHA256: load.PayloadSHA256,
			State: LoadState(load.State), HTTPStatus: load.HTTPStatus, TransactionID: load.TransactionID, TransactionKnown: load.TransactionKnown,
			RowsKnown: load.RowsKnown, TotalRows: load.TotalRows, LoadedRows: load.LoadedRows, FilteredRows: load.FilteredRows,
			UnselectedRows: load.UnselectedRows, Duplicate: load.Duplicate, ExistingJobStatus: load.ExistingJobStatus}
	}
	return Result{data: data}
}
