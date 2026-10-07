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

package trino

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
	native "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
)

func result(receipt *adapters.Receipt[Result], err error) (Result, error) {
	if receipt == nil {
		return Result{}, err
	}
	snapshot, _ := receipt.Snapshot()
	value, _ := snapshot.ValueCopy()
	return value, combineResultErrors(snapshot.Info().Operation, err, outcomeError(snapshot))
}

// evidenceFamily serializes one root's bounded native evidence custody.
// Different roots bind independent inboxes and own fresh statement transports.
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
	return fault.Correlation{Call: "trino-" + strconv.FormatUint(family.id, 10) + "-" + strconv.FormatUint(value.Info().Sequence, 10)}
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

func info(value source.Info) sqlengine.Info {
	result := sqlengine.Info{Scope: value.Scope, Provider: value.Configuration.Identity.Provider, Name: value.Configuration.Identity.Name, FormatVersion: value.Configuration.Format, Revision: value.Configuration.Revision}
	for _, layer := range value.Configuration.Provenance {
		result.Provenance = append(result.Provenance, sqlengine.LayerInfo{Kind: uint8(layer.Kind), Fields: slices.Clone(layer.Fields)})
	}
	return result
}

// Result is immutable technical evidence. Explicit copies can contain sensitive
// query IDs, metadata and payloads. It is not a durable DTO or restart token.
type Result struct {
	private
	native      native.Result
	present     bool
	source      sqlengine.Info
	attribution sqlengine.Attribution
	attempts    sqlengine.Attempts
}

func project(value invocation.Result[native.Result], metadata adapters.Info) Result {
	return Result{native: value.Outcome.Value, present: value.Outcome.Present, source: info(value.Source),
		attribution: attribution(metadata), attempts: sqlengine.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}}
}
func attribution(value adapters.Info) sqlengine.Attribution {
	return sqlengine.Attribution{Runtime: value.Runtime, Operation: value.Operation, ID: value.ID, Sequence: value.Sequence,
		Parent: value.Parent, Depth: value.Depth, Source: value.Source}
}

// ResultRepresentationVersion identifies exact direct JSON and metadata shape,
// independently of public API, configuration, SDK and coordinator versions.
const ResultRepresentationVersion uint32 = 1

func (value Result) RepresentationVersion() uint32 {
	if !value.present {
		return 0
	}
	return ResultRepresentationVersion
}
func (value Result) HasData() bool                      { return value.present }
func (value Result) Source() sqlengine.Info             { return value.source.Clone() }
func (value Result) Attribution() sqlengine.Attribution { return value.attribution }
func (value Result) Attempts() sqlengine.Attempts       { return value.attempts }

// Effect is mutation evidence, not retry permission or connector durability.
type Effect uint8

const (
	NotSubmitted Effect = iota
	Unknown
	Acknowledged
	ReadOnly
)

// Effect does not infer per-row success, durability, or absence of effects.
func (value Result) Effect() Effect { return Effect(value.native.Effect()) }

// QueryID is sensitive coordinator correlation, never a durable restart token.
func (value Result) QueryID() string { return value.native.QueryID() }

// Terminal records a valid identified reply without nextUri, even after failure.
func (value Result) Terminal() bool { return value.native.Terminal() }

// Succeeded records terminal protocol success, separately from fidelity/cleanup.
func (value Result) Succeeded() bool { return value.native.Succeeded() }

// Complete requires successful terminal validation and native draining. Cleanup
// errors remain separate; reader pages are always provisional and return false.
func (value Result) Complete() bool { return value.native.Complete() }

// CancellationAttempted records entry into the explicitly owned DELETE path.
func (value Result) CancellationAttempted() bool { return value.native.CancellationAttempted() }

// CancellationAcknowledged means HTTP 204, not remote termination or rollback.
func (value Result) CancellationAcknowledged() bool { return value.native.CancellationAcknowledged() }

// Submissions counts statement POST entries (zero or one), not commit success.
func (value Result) Submissions() uint64 { return value.native.Submissions() }

// Pages includes buffered error and progress responses, excluding cleanup.
func (value Result) Pages() uint64 { return value.native.Pages() }

// WireBytes includes bounded cleanup bodies and an oversize-detection byte,
// but excludes HTTP/TLS headers.
func (value Result) WireBytes() int64 { return value.native.WireBytes() }

// Rows is retained finite rows, one transferred page's rows, or aggregate
// delivered rows in a reader terminal summary. It is not an affected-row count.
func (value Result) Rows() int { return value.native.Rows() }

// UpdateCount distinguishes an absent native aggregate from aggregate zero.
func (value Result) UpdateCount() (int64, bool) { return value.native.UpdateCount() }

// DataCopy returns detached exact JSON rows, possibly a validated incomplete
// prefix. Numbers do not pass through float64. nil is absent, [] is present empty.
func (value Result) DataCopy() []byte { return value.native.DataCopy() }

// Column preserves received text and exact structured-signature JSON.
type Column struct {
	private
	Name, Type string
	Signature  []byte
}

// ColumnsCopy transfers detached exact metadata. Failed results may retain
// rejected first-page metadata for inspection; presence does not validate it.
func (value Result) ColumnsCopy() []Column {
	input := value.native.ColumnsCopy()
	if input == nil {
		return nil
	}
	columns := make([]Column, len(input))
	for index, column := range input {
		columns[index] = Column{Name: column.Name, Type: column.Type, Signature: column.Signature}
	}
	return columns
}
