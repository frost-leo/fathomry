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
	"context"
	"database/sql/driver"
	"io"
	"strconv"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

const (
	MaxSQLBytes = 64 << 10
	MaxColumns  = 64
)

// Column is copied metadata; names and types deliberately expose query data.
// Precision/scale and nullability have separate presence flags.
type Column struct {
	private
	Name           string
	DatabaseType   string
	Nullable       bool
	NullableKnown  bool
	Precision      int64
	Scale          int64
	PrecisionKnown bool
}
type cell struct {
	text string
	null bool
}

// Row retains immutable encodings. ValuesCopy distinguishes NULL (nil) from empty.
type Row struct {
	private
	cells []cell
}

func (r Row) ValuesCopy() [][]byte {
	out := make([][]byte, len(r.cells))
	for index, value := range r.cells {
		if !value.null {
			out[index] = []byte(value.text)
		}
	}
	return out
}

// Result is frozen evidence shared with the receipt and independent inbox.
// SQLAcknowledged is a protocol acknowledgement, not durable or visible rows.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	columns       []Column
	rows          []Row
	complete      bool
	dispatched    bool
	acknowledged  bool
	affected      int64
	affectedKnown bool
	serverVersion string
	load          LoadEvidence
	loadPresent   bool
}

func (r Result) ColumnsCopy() []Column {
	if r.data == nil {
		return nil
	}
	return append([]Column(nil), r.data.columns...)
}
func (r Result) RowsCopy() []Row {
	if r.data == nil {
		return nil
	}
	return append([]Row{}, r.data.rows...)
}
func (r Result) Complete() bool { return r.data != nil && r.data.complete }

// Dispatched means client entry was attempted; it does not prove server receipt.
func (r Result) Dispatched() bool      { return r.data != nil && r.data.dispatched }
func (r Result) SQLAcknowledged() bool { return r.data != nil && r.data.acknowledged }
func (r Result) RowsAffected() (int64, bool) {
	if r.data == nil {
		return 0, false
	}
	return r.data.affected, r.data.affectedKnown
}

// ServerGreeting is an observed protocol string, not an attested Doris FE/BE build.
func (r Result) ServerGreeting() string {
	if r.data == nil {
		return ""
	}
	return r.data.serverVersion
}
func (r Result) Load() (LoadEvidence, bool) {
	if r.data == nil {
		return LoadEvidence{}, false
	}
	return r.data.load, r.data.loadPresent
}

// Query executes one authorized text command and fully consumes a bounded result.
// No parameters, statement preparation, multi-statements or session state persist.
func (c *Client) Query(ctx context.Context, id fault.Correlation, query string) (*invocation.Receipt[Result], error) {
	return c.sql(ctx, id, query, true)
}

// Exec dispatches authorized native SQL once. Acknowledgement does not classify
// that SQL's DDL/DML/transaction/catalog side effects or authorize a retry.
func (c *Client) Exec(ctx context.Context, id fault.Correlation, query string) (*invocation.Receipt[Result], error) {
	return c.sql(ctx, id, query, false)
}
func (c *Client) sql(ctx context.Context, id fault.Correlation, query string, read bool) (*invocation.Receipt[Result], error) {
	if c == nil || c.owner == nil || !validText(query, MaxSQLBytes, false) {
		return nil, failure(ErrInput, "sql")
	}
	if c.owner.settings.SQLAddress == "" {
		return nil, failure(ErrUnsupported, "sql")
	}
	name := "exec"
	if read {
		name = "query"
	}
	call, work, cancel, err := c.begin(ctx, id, name)
	if err != nil {
		return nil, err
	}
	if work == nil {
		return call.Receipt(), nil
	}
	defer cancel()
	s := c.owner.settings
	data := &resultData{}
	primary, cleanup := c.executeSQL(work, call, query, read, data, s)
	call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanup})
	return call.Receipt(), nil
}
func consume(rows driver.Rows, s settings, data *resultData) error {
	names := rows.Columns()
	if len(names) > MaxColumns {
		return failure(ErrLimit, "columns")
	}
	total := 0
	for index, name := range names {
		nullable, known := rows.(driver.RowsColumnTypeNullable).ColumnTypeNullable(index)
		precision, scale, precisionKnown := rows.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(index)
		kind := rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(index)
		total += len(name) + len(kind)
		if total > s.MaxResultBytes {
			return failure(ErrLimit, "metadata")
		}
		data.columns = append(data.columns, Column{Name: name, DatabaseType: kind, Nullable: nullable, NullableKnown: known, Precision: precision, Scale: scale, PrecisionKnown: precisionKnown})
	}
	values := make([]driver.Value, len(names))
	for {
		err := rows.Next(values)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(data.rows) >= s.MaxRows {
			return failure(ErrLimit, "rows")
		}
		row := Row{cells: make([]cell, len(values))}
		for index, value := range values {
			var text string
			switch value := value.(type) {
			case nil:
				row.cells[index].null = true
				continue
			case []byte:
				if len(value) > s.MaxResultBytes-total {
					return failure(ErrLimit, "result-bytes")
				}
				text = string(value)
			case int64:
				text = strconv.FormatInt(value, 10)
			case uint64:
				text = strconv.FormatUint(value, 10)
			case float32:
				text = strconv.FormatFloat(float64(value), 'g', -1, 32)
			case float64:
				text = strconv.FormatFloat(value, 'g', -1, 64)
			default:
				return failure(ErrUnsupported, "value")
			}
			total += len(text)
			if total > s.MaxResultBytes {
				return failure(ErrLimit, "result-bytes")
			}
			row.cells[index].text = text
		}
		data.rows = append(data.rows, row)
	}
}
