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

package mysql

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"

	"github.com/frost-leo/fathomry/adapters/database/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/mysql/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
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
	return fault.Correlation{Call: "db-" + strconv.FormatUint(family.id, 10) + "-" + strconv.FormatUint(value.Info().Sequence, 10)}
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
	outcome := adapters.Outcome[Result]{Value: project(value, metadata.Info()), Present: true, Primary: primary, Cleanup: cleanup}
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

func info(value source.Info) database.Info {
	result := database.Info{Scope: value.Scope, Provider: value.Configuration.Identity.Provider, Name: value.Configuration.Identity.Name, FormatVersion: value.Configuration.Format, Revision: value.Configuration.Revision}
	for _, layer := range value.Configuration.Provenance {
		result.Provenance = append(result.Provenance, database.LayerInfo{Kind: uint8(layer.Kind), Fields: slices.Clone(layer.Fields)})
	}
	return result
}

// Result is immutable process-local evidence. Getter copies belong to the caller.
// SQL success is not a commit, durability, visibility or business guarantee.
type Result struct {
	private
	native      native.Result
	present     bool
	source      database.Info
	attribution database.Attribution
	attempts    database.Attempts
}

func project(value invocation.Result[native.Result], metadata adapters.Info) Result {
	return Result{native: value.Outcome.Value, present: value.Outcome.Present, source: info(value.Source),
		attribution: database.Attribution{Runtime: metadata.Runtime, Operation: metadata.Operation, ID: metadata.ID, Sequence: metadata.Sequence, Parent: metadata.Parent, Depth: metadata.Depth, Source: metadata.Source},
		attempts:    database.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}}
}

// HasData distinguishes an absent native result from an accepted failed call.
func (value Result) HasData() bool { return value.present }

// Source returns detached native source attribution captured for this result.
func (value Result) Source() database.Info { return value.source.Clone() }

// Attribution returns the captured public operation and generation identity.
func (value Result) Attribution() database.Attribution { return value.attribution }

// Attempts returns observed native dispatch counts; it does not infer effect state.
func (value Result) Attempts() database.Attempts { return value.attempts }

// Complete describes native statement consumption, independently of cleanup errors.
func (value Result) Complete() bool { return value.native.Complete() }

// RowsRead includes the first over-limit witness, when one was observed.
func (value Result) RowsRead() int { return value.native.RowsRead() }

// ServerVersion is native server text, not an attested deployment identity.
func (value Result) ServerVersion() string { return value.native.ServerVersion() }

// Column is positional native metadata. Names may repeat and are sensitive.
type Column struct {
	private
	Name, DatabaseType      string
	Nullable, NullableKnown bool
	Precision, Scale        int64
	PrecisionKnown          bool
}

// ColumnsCopy returns independent positional metadata, or nil when unavailable.
func (value Result) ColumnsCopy() []Column {
	input := value.native.ColumnsCopy()
	if input == nil {
		return nil
	}
	output := make([]Column, len(input))
	for index, column := range input {
		output[index] = Column{Name: column.Name, DatabaseType: column.DatabaseType, Nullable: column.Nullable, NullableKnown: column.NullableKnown, Precision: column.Precision, Scale: column.Scale, PrecisionKnown: column.PrecisionKnown}
	}
	return output
}

// Row contains only immutable native cells, with no scanner or owning handle.
type Row struct {
	private
	native native.Row
}

// ValuesCopy distinguishes SQL NULL (nil) from present empty bytes (non-nil).
// Numeric/time values preserve the provider's encodings without float64 coercion.
func (value Row) ValuesCopy() [][]byte { return value.native.ValuesCopy() }

// RowsCopy includes partial rows. Nil means no retained query result; successful
// empty Query returns a non-nil empty slice. Exec never retains rows.
func (value Result) RowsCopy() []Row {
	input := value.native.RowsCopy()
	if input == nil {
		return nil
	}
	output := make([]Row, len(input))
	for index, row := range input {
		output[index] = Row{native: row}
	}
	return output
}

// First requires complete consumption; it preserves the native no-row sentinel.
// It does not require uniqueness.
func (value Result) First() (Row, error) {
	row, err := value.native.First()
	return Row{native: row}, translate(err, "first")
}

// TransactionOutcome states what finalization actually observed, not retry policy.
type TransactionOutcome string

const (
	TransactionUnobserved TransactionOutcome = ""
	CommitAcknowledged    TransactionOutcome = "commit-acknowledged"
	RollbackAcknowledged  TransactionOutcome = "rollback-acknowledged"

	FinalizationUnknown TransactionOutcome = "finalization-unknown"
)

// TransactionOutcome is absent for ordinary statements, including Tx statements.
func (value Result) TransactionOutcome() TransactionOutcome {
	return TransactionOutcome(value.native.TransactionOutcome())
}

// RowsAffected returns an exact native count and whether command metadata exists.
func (value Result) RowsAffected() (int64, bool) { return value.native.RowsAffected() }

// LastInsertID preserves native signed-64-bit limits and metadata presence.
func (value Result) LastInsertID() (int64, bool) { return value.native.LastInsertID() }
