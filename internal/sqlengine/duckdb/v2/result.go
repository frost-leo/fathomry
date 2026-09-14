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

package duckdb

import "slices"

// Column retains the exact native name/type; duplicate names remain positional.
type Column struct {
	Name string
	Type string
}

// Step records one attempted request. Prepared and Executions are observations,
// not per-row durability. Submitted means native execution might have effects;
// zero Executions after submission is not proof of no effect. RowsChanged is a
// native aggregate for acknowledged executions, not the failing execution,
// matched rows or a business acknowledgement.
type Step struct {
	private
	Mode             Mode
	Prepared         bool
	Submitted        bool
	Executions       int
	RowsChanged      int64
	RowsChangedKnown bool
	// Appender stages: accepted into native/Go buffering, successfully flushed,
	// and destroyed after a close that may itself flush.
	AcceptedRows           int
	FlushAttempted         bool
	Flushed                bool
	FlushedRows            int
	AppenderClosed         bool
	AppenderCloseSucceeded bool
	Columns                []Column
	Rows                   [][]any
	// Complete requires observed native EOF, not reaching a configured bound.
	Complete bool
	Limited  bool
}

// Progress distinguishes transaction and cleanup evidence from statement effects.
// Committed is a positive native acknowledgement; a failed CommitAttempted may
// have an unknown effect. RolledBack confirms only this local native transaction.
// Earlier step progress is preserved even when the transaction rolls back.
// ConnectionClosed is local ownership completion, not crash-recovery evidence.
type Progress struct {
	private
	Steps             []Step
	Transaction       bool
	Began             bool
	CommitAttempted   bool
	Committed         bool
	RollbackAttempted bool
	RolledBack        bool
	ConnectionClosed  bool
}

// Result owns immutable outcome data shared by receipt and independent inbox.
// A zero Result has no progress. It is not a durable DTO.
type Result struct {
	private
	progress *Progress
}

// Snapshot returns fresh container/scalar storage, safe to mutate independently.
// Concurrent snapshot calls are safe. Copies retained by callers are their memory.
func (result Result) Snapshot() Progress {
	if result.progress == nil {
		return Progress{}
	}
	copied := *result.progress
	copied.Steps = slices.Clone(copied.Steps)
	for index := range copied.Steps {
		step := &copied.Steps[index]
		step.Columns = slices.Clone(step.Columns)
		step.Rows = slices.Clone(step.Rows)
		for rowIndex, row := range step.Rows {
			step.Rows[rowIndex] = slices.Clone(row)
			for column, value := range row {
				step.Rows[rowIndex][column] = copyScalar(value)
			}
		}
	}
	return copied
}
