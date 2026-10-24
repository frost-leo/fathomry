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

package create

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	"github.com/frost-leo/fathomry/failure/v1"
)

// WorkBytes reserves bounded creation/profile scratch independently of schema
// installation. Provider row-copy and evidence envelopes are charged separately.
const WorkBytes = 8 << 20

func fail(code failure.Code, causes ...error) error {
	return databasecmd.Fail(code, "database.create", causes...)
}

// DatabaseEffect describes this attempt's acknowledged physical mutation.
type DatabaseEffect string

const (
	DatabaseNotStarted    DatabaseEffect = "not_started"
	DatabaseUnchanged     DatabaseEffect = "unchanged"
	DatabaseCreated       DatabaseEffect = "created"
	DatabaseUnknown       DatabaseEffect = "unknown"
	DatabaseNotApplicable DatabaseEffect = "not_applicable"
)

// DatabaseChange survives failure. Ready means target access/profile validation,
// not installed tables or Worker readiness. CleanupComplete covers retained SQL.
type DatabaseChange struct {
	Effect          DatabaseEffect `json:"effect"`
	SchemaEffect    DatabaseEffect `json:"schema_effect"`
	Stage           string         `json:"stage"`
	Ready           bool           `json:"ready"`
	CleanupComplete bool           `json:"cleanup_complete"`
}

// CreateOptions names the explicit physical target and bounds operation/cleanup
// waits. Zero durations select 45s/5s; explicit values accept 1ms..1min.
type CreateOptions struct {
	Dialect, Database, Schema, Owner string
	Timeout, CleanupTimeout          time.Duration
}

// Creator borrows resource-backed admin/target clients for one creation attempt.
// Copies share state. Close joins retained SQL, never the source or its pool.
type Creator struct {
	creationState *creationState
}
type creationState struct {
	mu                sync.Mutex
	closed            atomic.Bool
	options           CreateOptions
	sql               statements
	pgAdmin, pgTarget *postgres.Client
	myAdmin, myTarget *mysql.Client
	statement         *postgres.Statement
	transaction       *postgres.Transaction
	session           *mysql.Session
	used, commitSent  bool
	outcome           postgres.TransactionOutcome
	calls             int
	cleanup           error
	last              DatabaseChange
}

func normalizeCreation(value CreateOptions) (CreateOptions, statements, error) {
	options, err := (databasecmd.Options{Timeout: value.Timeout, CleanupTimeout: value.CleanupTimeout}).Normalize()
	if err != nil {
		return value, statements{}, fail(databasecmd.ErrInput, err)
	}
	value.Timeout, value.CleanupTimeout = options.Timeout, options.CleanupTimeout
	database, err := quoteDatabase(value.Dialect, value.Database)
	if err != nil {
		return value, statements{}, err
	}
	var quotedSchema, owner string
	if value.Dialect == "postgresql" {
		if err := databasecmd.ValidateTarget(value.Dialect, value.Schema); err != nil {
			return value, statements{}, fail(databasecmd.ErrInput, err)
		}
		quotedSchema, err = quotePG(value.Schema)
		if err != nil {
			return value, statements{}, err
		}
		owner, err = quotePG(value.Owner)
		if err != nil {
			return value, statements{}, err
		}
	} else if value.Schema != "" || value.Owner != "" {
		return value, statements{}, fail(databasecmd.ErrInput)
	}
	sql, err := loadCreation(value.Dialect, database, quotedSchema, owner)
	return value, sql, err
}

// ValidateCreation checks reviewed assets and target identifiers without service I/O.
func ValidateCreation(value CreateOptions) error { _, _, err := normalizeCreation(value); return err }
func PostgreSQLCreation(admin, target *postgres.Client, options CreateOptions) (*Creator, error) {
	if admin == nil || target == nil || options.Dialect != "postgresql" {
		return nil, fail(databasecmd.ErrInput)
	}
	value, sql, err := normalizeCreation(options)
	if err != nil {
		return nil, err
	}
	return &Creator{creationState: &creationState{options: value, sql: sql, pgAdmin: admin, pgTarget: target}}, nil
}
func MySQLCreation(admin, target *mysql.Client, options CreateOptions) (*Creator, error) {
	if admin == nil || target == nil || options.Dialect != "mysql" {
		return nil, fail(databasecmd.ErrInput)
	}
	value, sql, err := normalizeCreation(options)
	if err != nil {
		return nil, err
	}
	return &Creator{creationState: &creationState{options: value, sql: sql, myAdmin: admin, myTarget: target}}, nil
}
func (runner *Creator) Create(ctx context.Context) (result DatabaseChange, err error) {
	result = DatabaseChange{Effect: DatabaseNotStarted, SchemaEffect: DatabaseNotStarted, Stage: "admission"}
	if runner == nil || runner.creationState == nil || ctx == nil {
		return result, fail(databasecmd.ErrInput)
	}
	creationState := runner.creationState
	if !creationState.mu.TryLock() {
		return result, fail(databasecmd.ErrBusy)
	}
	defer creationState.mu.Unlock()
	if creationState.closed.Load() {
		return result, fail(databasecmd.ErrClosed)
	}
	if creationState.used {
		return creationState.last, fail(databasecmd.ErrInput)
	}
	if ctx.Err() != nil {
		return result, fail(databasecmd.ErrDatabase, ctx.Err(), context.Cause(ctx))
	}
	creationState.used = true
	if creationState.myAdmin != nil {
		result.SchemaEffect = DatabaseNotApplicable
	}
	work, cancel := context.WithTimeout(ctx, creationState.options.Timeout)
	defer cancel()
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), creationState.options.CleanupTimeout)
		problem := creationState.release(cleanup)
		stop()
		switch creationState.outcome {
		case postgres.CommitAcknowledged:
			if creationState.commitSent {
				if creationState.options.Schema != "public" {
					result.SchemaEffect = DatabaseCreated
				}
				result.Stage, result.Ready = "ready", true
			}
		case postgres.RollbackAcknowledged, postgres.CommitRolledBack:
			if result.SchemaEffect == DatabaseUnknown {
				result.SchemaEffect = DatabaseUnchanged
			}
		}
		result.CleanupComplete = creationState.released()
		if problem != nil {
			err = fail(databasecmd.ErrCleanup, err, problem)
		}
		creationState.last = result
	}()
	result.Stage = "preflight"
	if creationState.pgAdmin != nil {
		err = creationState.createPostgres(work, &result)
	} else {
		err = creationState.createMySQL(work, &result)
	}
	return
}
func (creationState *creationState) admit(ctx context.Context) error {
	if creationState.calls >= 24 {
		return fail(databasecmd.ErrLimit)
	}
	if ctx.Err() != nil {
		return fail(databasecmd.ErrDatabase, ctx.Err(), context.Cause(ctx))
	}
	creationState.calls++
	return nil
}
func receiptReleased[T any](receipt *adapters.Receipt[T]) bool {
	if receipt == nil {
		return false
	}
	snapshot, _ := receipt.Snapshot()
	return snapshot.Info().Released
}
func (creationState *creationState) released() bool {
	return creationState.statement == nil && creationState.transaction == nil && creationState.session == nil
}
func (creationState *creationState) release(ctx context.Context) error {
	if creationState.statement != nil {
		_, problem := creationState.statement.Close(ctx)
		final, wait := creationState.statement.Receipt().WaitReleased(ctx)
		if !final.Info().Released {
			return fail(databasecmd.ErrCleanup, problem, wait)
		}
		creationState.cleanup = errors.Join(creationState.cleanup, problem, wait, final.Cleanup())
		creationState.statement = nil
	}
	if creationState.transaction != nil {
		var problem error
		if !receiptReleased(creationState.transaction.Receipt()) {
			_, problem = creationState.transaction.Rollback(ctx)
		}
		final, wait := creationState.transaction.Receipt().WaitReleased(ctx)
		if value, present := final.ValueCopy(); present && value.TransactionOutcome() != postgres.TransactionUnobserved {
			creationState.outcome = value.TransactionOutcome()
		}
		if !final.Info().Released {
			return fail(databasecmd.ErrCleanup, problem, wait)
		}
		creationState.cleanup = errors.Join(creationState.cleanup, problem, wait, final.Cleanup())
		creationState.transaction = nil
	}
	if creationState.session != nil {
		problem := creationState.session.Close(ctx)
		final, wait := creationState.session.Receipt().WaitReleased(ctx)
		if !final.Info().Released {
			return fail(databasecmd.ErrCleanup, problem, wait)
		}
		creationState.cleanup = errors.Join(creationState.cleanup, problem, wait, final.Cleanup())
		creationState.session = nil
	}
	if creationState.cleanup != nil {
		return fail(databasecmd.ErrCleanup, creationState.cleanup)
	}
	return nil
}
func (creationState *creationState) finish(ctx context.Context) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), creationState.options.CleanupTimeout)
	defer cancel()
	return creationState.release(cleanup)
}
func (runner *Creator) Close(ctx context.Context) error {
	if runner == nil || runner.creationState == nil || ctx == nil {
		return fail(databasecmd.ErrInput)
	}
	creationState := runner.creationState
	creationState.closed.Store(true)
	if !creationState.mu.TryLock() {
		return fail(databasecmd.ErrBusy)
	}
	defer creationState.mu.Unlock()
	return creationState.release(ctx)
}
func (runner *Creator) Released() bool {
	if runner == nil || runner.creationState == nil {
		return false
	}
	if !runner.creationState.mu.TryLock() {
		return false
	}
	defer runner.creationState.mu.Unlock()
	return runner.creationState.released()
}

func (Creator) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("create.Creator")) }
func (Creator) LogValue() slog.Value           { return slog.StringValue("create.Creator") }
func (Creator) MarshalJSON() ([]byte, error)   { return nil, fail(databasecmd.ErrSerialization) }
func (*Creator) UnmarshalJSON([]byte) error    { return fail(databasecmd.ErrSerialization) }
