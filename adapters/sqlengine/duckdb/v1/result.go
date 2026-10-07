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

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"

	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

func result(receipt *adapters.Receipt[Result], err error) (Result, error) {
	if receipt == nil {
		return Result{}, err
	}
	snapshot, _ := receipt.Snapshot()
	value, _ := snapshot.ValueCopy()
	return value, combineResultErrors(snapshot.Info().Operation, err, outcomeError(snapshot))
}

// evidenceFamily serializes a retained native connection and its bounded custody queue.
// Different roots bind independent inboxes to the same native pool.
type evidenceFamily struct {
	gate  sync.Mutex
	inbox *invocation.Inbox[native.Result]
	id    uint64
}

func newFamily(id uint64, capacity int, bytes int64) (*evidenceFamily, error) {
	inbox, err := invocation.NewInbox[native.Result](capacity, bytes)
	if err != nil {
		return nil, err
	}
	return &evidenceFamily{inbox: inbox, id: id}, nil
}

// correlation is called under gate. It never interprets public opaque IDs.
func (family *evidenceFamily) correlation(call *adapters.Call[Result]) fault.Correlation {
	value, _ := call.Receipt().Snapshot()
	return fault.Correlation{Call: "duck-" + strconv.FormatUint(family.id, 10) + "-" + strconv.FormatUint(value.Info().Sequence, 10)}
}

func (family *evidenceFamily) take(receipt *invocation.Receipt[native.Result]) (*invocation.DeliveryRecord[native.Result], error) {
	record, err := family.inbox.Next(context.Background())
	if err != nil {
		return nil, err
	}
	expected, _ := receipt.Result()
	actual, _ := record.Receipt().Result()
	if expected.Context.Correlation != actual.Context.Correlation {
		return nil, errors.New("database: native receipt custody mismatch")
	}
	return record, nil
}

// finish transfers one finite record after native local use has actually ended.
// The caller holds gate through native dispatch, take and transfer.
func (family *evidenceFamily) finish(call *adapters.Call[Result], receipt *invocation.Receipt[native.Result], setup error) error {
	if receipt == nil {
		if setup == nil {
			setup = errors.New("database: missing native receipt")
		}
		err := translate(setup, "operation")
		_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
		return err
	}
	record, err := family.take(receipt)
	if err != nil {
		// Contract violations cannot authorize discarding native custody.
		return err
	}
	value, err := receipt.WaitReleased(context.Background())
	if err != nil {
		return err
	}
	return family.transfer(call, record, value, setup)
}

func (family *evidenceFamily) transfer(call *adapters.Call[Result], record *invocation.DeliveryRecord[native.Result], value invocation.Result[native.Result], setup error) error {
	metadata, _ := call.Receipt().Snapshot()
	primary := translate(errors.Join(setup, value.Outcome.Primary), "operation")
	cleanup := translate(value.Outcome.Cleanup, "cleanup")
	projected, projectionErr := project(value, metadata.Info())
	primary = combineResultErrors("operation", primary, projectionErr)
	outcome := adapters.Outcome[Result]{Value: projected, Present: true, Primary: primary, Cleanup: cleanup}
	if err := call.Resolve(outcome); err != nil {
		return err
	}
	return record.Release()
}

// retainedResult holds one native record and one public guard. Only bounded retained
// handles create a join worker; finite SQL calls never create a worker per result.
type retainedResult struct {
	receipt *adapters.Receipt[Result]
	record  *invocation.DeliveryRecord[native.Result]
	guard   adapters.Guard
}

// retain requires a guard taken before fallible native acquisition. gate remains
// held until the creation method returns; the final join acquires it before
// releasing public use, including for automatic native finalization.
func (family *evidenceFamily) retain(call *adapters.Call[Result], guard adapters.Guard, receipt *invocation.Receipt[native.Result], lifetime context.Context, stop func(), cleanup func(context.Context)) (*retainedResult, error) {
	record, err := family.take(receipt)
	if err != nil {
		return nil, err
	}
	retained := &retainedResult{receipt: call.Receipt(), record: record, guard: guard}
	go func() {
		value, err := receipt.WaitReleased(lifetime)
		if err != nil {
			if cleanup != nil {
				family.gate.Lock()
				cleanup(context.Background())
				family.gate.Unlock()
			}
			value, err = receipt.WaitReleased(context.Background())
		}
		family.gate.Lock()
		stop()
		if err == nil {
			err = family.transfer(call, retained.record, value, nil)
		}
		if err != nil {
			// A broken bridge keeps the original reservation rather than claiming release.
			_ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "evidence")})
			family.gate.Unlock()
			return
		}
		// Release publishes completion; a sequential caller must find the gate free.
		family.gate.Unlock()
		_ = retained.guard.Release()
	}()
	return retained, nil
}

func (retained *retainedResult) result(ctx context.Context) (Result, error) {
	var zero Result
	if retained == nil || ctx == nil {
		return zero, adapters.ErrHandle
	}
	value, err := retained.receipt.WaitReleased(ctx)
	result, present := value.ValueCopy()
	if !present {
		result = zero
	}
	return result, combineResultErrors(value.Info().Operation, err, outcomeError(value))
}

func info(value source.Info) sqlengine.Info {
	result := sqlengine.Info{Scope: value.Scope, Provider: value.Configuration.Identity.Provider, Name: value.Configuration.Identity.Name, FormatVersion: value.Configuration.Format, Revision: value.Configuration.Revision}
	for _, layer := range value.Configuration.Provenance {
		result.Provenance = append(result.Provenance, sqlengine.LayerInfo{Kind: uint8(layer.Kind), Fields: slices.Clone(layer.Fields)})
	}
	return result
}

// Result owns immutable, process-local outcome evidence. Snapshot returns detached
// provider-owned scalars; no SDK values or native handles escape through any.
type Result struct {
	private
	progress    *Progress
	present     bool
	source      sqlengine.Info
	attribution sqlengine.Attribution
	attempts    sqlengine.Attempts
}

// ResultRepresentationVersion identifies the supported scalar/progress shape,
// independently of API, configuration, SDK, bindings and native engine versions.
// It does not define a durable serialization format.
const ResultRepresentationVersion uint32 = 1

// RepresentationVersion is zero when native progress is absent.
func (value Result) RepresentationVersion() uint32 {
	if !value.present {
		return 0
	}
	return ResultRepresentationVersion
}

func project(value invocation.Result[native.Result], metadata adapters.Info) (Result, error) {
	progress, err := projectProgress(value.Outcome.Value.Snapshot())
	return Result{progress: &progress, present: value.Outcome.Present, source: info(value.Source),
		attribution: sqlengine.Attribution{Runtime: metadata.Runtime, Operation: metadata.Operation, ID: metadata.ID,
			Sequence: metadata.Sequence, Parent: metadata.Parent, Depth: metadata.Depth, Source: metadata.Source},
		attempts: sqlengine.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}}, err
}

// HasData distinguishes absent native progress from a present empty result.
func (value Result) HasData() bool { return value.present }

// Source is detached preparation provenance, not a public resource generation.
func (value Result) Source() sqlengine.Info { return value.source.Clone() }

// Attribution captures the public generation and operation used for this result.
func (value Result) Attribution() sqlengine.Attribution { return value.attribution }

// Attempts does not infer unobserved native submissions.
func (value Result) Attempts() sqlengine.Attempts { return value.attempts }

// Snapshot copies all mutable scalar and container storage.
func (value Result) Snapshot() Progress {
	if value.progress == nil {
		return Progress{}
	}
	output := *value.progress
	if output.Reader != nil {
		reader := *output.Reader
		output.Reader = &reader
	}
	output.Steps = slices.Clone(output.Steps)
	for index := range output.Steps {
		step := &output.Steps[index]
		step.Columns = slices.Clone(step.Columns)
		step.Rows = slices.Clone(step.Rows)
		for rowIndex, row := range step.Rows {
			step.Rows[rowIndex] = slices.Clone(row)
			for column, cell := range row {
				step.Rows[rowIndex][column] = copyScalar(cell)
			}
		}
	}
	return output
}

// Column retains exact native names and types positionally, including duplicates.
type Column struct {
	private
	Name, Type string
}

// Step preserves native execution and Appender stages. A submitted failure may
// have effects even with zero acknowledged executions. Counts are aggregates.
type Step struct {
	private
	Mode                                   Mode
	Prepared, Submitted                    bool
	Executions                             int
	RowsChanged                            int64
	RowsChangedKnown                       bool
	AcceptedRows                           int
	FlushAttempted, Flushed                bool
	FlushedRows                            int
	AppenderClosed, AppenderCloseSucceeded bool
	Columns                                []Column
	Rows                                   [][]any
	Complete, Limited                      bool
}

// Progress keeps transaction acknowledgements and local release distinct from
// data visibility, per-item completion, external effects and crash durability.
type Progress struct {
	private
	Steps                                                                                           []Step
	Transaction, Began, CommitAttempted, Committed, RollbackAttempted, RolledBack, ConnectionClosed bool
	Reader                                                                                          *ReadProgress
}

// ReadProgress describes one bounded delivery or the compact terminal summary.
// Offset is zero-based; totals count validated rows delivered so far, not EOF.
// Closed confirms local native cleanup, independently of primary/cleanup errors.
type ReadProgress struct {
	private
	Chunk                 uint64
	Offset                int64
	Rows                  int
	Bytes                 int64
	TotalRows, TotalBytes int64
	Closed                bool
}

func projectProgress(input native.Progress) (Progress, error) {
	output := Progress{Transaction: input.Transaction, Began: input.Began,
		CommitAttempted: input.CommitAttempted, Committed: input.Committed,
		RollbackAttempted: input.RollbackAttempted, RolledBack: input.RolledBack, ConnectionClosed: input.ConnectionClosed}
	if input.Reader != nil {
		reader := input.Reader
		output.Reader = &ReadProgress{Chunk: reader.Chunk, Offset: reader.Offset, Rows: reader.Rows, Bytes: reader.Bytes,
			TotalRows: reader.TotalRows, TotalBytes: reader.TotalBytes, Closed: reader.Closed}
	}
	if input.Steps != nil {
		output.Steps = make([]Step, len(input.Steps))
	}
	for index, step := range input.Steps {
		projected := Step{Mode: Mode(step.Mode), Prepared: step.Prepared, Submitted: step.Submitted, Executions: step.Executions,
			RowsChanged: step.RowsChanged, RowsChangedKnown: step.RowsChangedKnown, AcceptedRows: step.AcceptedRows,
			FlushAttempted: step.FlushAttempted, Flushed: step.Flushed, FlushedRows: step.FlushedRows,
			AppenderClosed: step.AppenderClosed, AppenderCloseSucceeded: step.AppenderCloseSucceeded,
			Complete: step.Complete, Limited: step.Limited}
		if step.Columns != nil {
			projected.Columns = make([]Column, len(step.Columns))
		}
		for column, value := range step.Columns {
			projected.Columns[column] = Column{Name: value.Name, Type: value.Type}
		}
		if step.Rows != nil || step.Mode == native.Query && step.Complete {
			projected.Rows = make([][]any, len(step.Rows))
		}
		output.Steps[index] = projected
		for rowIndex, row := range step.Rows {
			if len(row) != len(step.Columns) {
				return output, fail(ErrUnsupported, "result-shape")
			}
			converted := make([]any, len(row))
			for column, value := range row {
				if column >= len(step.Columns) {
					return output, fail(ErrUnsupported, "result-shape")
				}
				scalar, err := outward(value, step.Columns[column].Type)
				if err != nil {
					return output, err
				}
				converted[column] = scalar
			}
			output.Steps[index].Rows[rowIndex] = converted
		}
	}
	return output, nil
}
