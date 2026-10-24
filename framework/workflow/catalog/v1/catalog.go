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

package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)

// WorkBytes is the caller-reserved scratch envelope for one non-overlapping
// catalog operation, in addition to prepared input and Adapter evidence/copy
// envelopes. It is a logical admission charge, not a measured RSS guarantee.
const WorkBytes = 128 << 20

// Options bounds catalog work and subsequent cleanup waiting. Zero selects
// 45s/5s; nonzero values must be 1ms..1min. Expiry does not prove native release.
type Options struct{ Timeout, CleanupTimeout time.Duration }

// Effect describes this registration attempt, not workflow enablement or hosting.
type Effect string

const (
	NotStarted Effect = "not_started"
	Applied    Effect = "applied"
	Unchanged  Effect = "unchanged"
	RolledBack Effect = "rolled_back"
	Unknown    Effect = "unknown"
)

// Acknowledgement reports native transaction finalization, never inferred rows.
type Acknowledgement string

const (
	Unobserved             Acknowledgement = "unobserved"
	CommitAcknowledged     Acknowledgement = "commit_acknowledged"
	RollbackAcknowledged   Acknowledgement = "rollback_acknowledged"
	CommitRolledBack       Acknowledgement = "commit_rolled_back"
	AcknowledgementUnknown Acknowledgement = "unknown"
)

// Changes counts new rows only after acknowledged successful registration.
// Unknown/rolled-back attempts expose no tentative IDs or durable change counts.
type Changes struct {
	DefinitionCreated bool `json:"definition_created"`
	Categories        int  `json:"categories"`
	Versions          int  `json:"versions"`
	Modes             int  `json:"modes"`
	Revisions         int  `json:"revisions"`
	Resources         int  `json:"resources"`
	Texts             int  `json:"texts"`
	Tags              int  `json:"tags"`
}

// Publication is a detached observation even when Register returns an error.
// CleanupComplete means no catalog-owned retained transaction remains; it does
// not mean source/runtime shutdown, absence of cleanup errors or remote rollback.
// DefinitionID is a positive decimal string only for Applied/Unchanged. The zero
// value is not a completed result. Explicit JSON may expose project identities.
type Publication struct {
	Namespace       string          `json:"namespace,omitempty"`
	Code            string          `json:"code,omitempty"`
	Effect          Effect          `json:"effect"`
	Acknowledgement Acknowledgement `json:"acknowledgement"`
	DefinitionID    string          `json:"definition_id,omitempty"`
	Changes         Changes         `json:"changes"`
	CleanupComplete bool            `json:"cleanup_complete"`
	CleanupFailed   bool            `json:"cleanup_failed"`
}

// Catalog borrows one Resource-backed Adapter client and owns only its retained
// operation. Copies share one authority. Calls refuse overlap rather than queue;
// pending cleanup blocks new work. Close never closes the client or its source.
// The caller owns prepared input, WorkBytes, incremental evidence reception and
// the provider's full-document transport/copy budget.
type Catalog struct{ state *catalogState }
type catalogState struct {
	mu                          sync.Mutex
	closed                      atomic.Bool
	pg                          *postgres.Client
	my                          *mysql.Client
	target                      string
	options                     Options
	pending                     *publicationTx
	cancel                      context.CancelFunc
	accepted, started, complete bool
	changes                     publicationChanges
	last                        Publication
	cleanup                     error
	reading                     bool
}

// PostgreSQL constructs an offline catalog capability for an explicit schema.
// The supplied client must use Adapter Using; Register verifies the captured
// Resource generation on the transaction before any business SQL.
func PostgreSQL(client *postgres.Client, target string, options Options) (*Catalog, error) {
	if client == nil {
		return nil, publicationError(ErrInput, "client", "")
	}
	result, err := newCatalog("postgresql", target, options)
	if err == nil {
		result.state.pg = client
	}
	return result, err
}

// MySQL constructs an offline capability for an explicit existing database.
// The retained transaction's DATABASE() must match; no database is auto-created.
func MySQL(client *mysql.Client, target string, options Options) (*Catalog, error) {
	if client == nil {
		return nil, publicationError(ErrInput, "client", "")
	}
	result, err := newCatalog("mysql", target, options)
	if err == nil {
		result.state.my = client
	}
	return result, err
}
func newCatalog(dialect, target string, options Options) (*Catalog, error) {
	if _, err := publicationTarget(dialect, target); err != nil {
		return nil, publicationError(ErrInput, "target", "", err)
	}
	if options.Timeout == 0 {
		options.Timeout = 45 * time.Second
	}
	if options.CleanupTimeout == 0 {
		options.CleanupTimeout = 5 * time.Second
	}
	if options.Timeout < time.Millisecond || options.Timeout > time.Minute || options.CleanupTimeout < time.Millisecond || options.CleanupTimeout > time.Minute {
		return nil, publicationError(ErrInput, "options", "")
	}
	var err error
	if dialect == "postgresql" {
		_, err = loadSchema(pgassets.Assets(), dialect)
	} else {
		_, err = loadSchema(mysqlassets.Assets(), dialect)
	}
	if err != nil {
		return nil, err
	}
	return &Catalog{state: &catalogState{target: target, options: options}}, nil
}
func noPublication() Publication {
	return Publication{Effect: NotStarted, Acknowledgement: Unobserved, CleanupComplete: true}
}

// Register atomically creates or compares one prepared workflow aggregate. It
// always admits current schema on the actual retained transaction. It never
// retries or enables workflows. A lost commit reply is Unknown; after cleanup,
// inspect stored state or explicitly retry the same immutable registration.
// On incomplete cleanup retain this Catalog, call Close with a fresh context,
// and inspect LastPublication. The returned value is not retroactively updated.
func (catalog *Catalog) Register(ctx context.Context, prepared *PreparedRegistration) (Publication, error) {
	result := noPublication()
	if catalog == nil || catalog.state == nil || ctx == nil {
		return result, publicationError(ErrInput, "catalog", "")
	}
	state := catalog.state
	if !state.mu.TryLock() {
		return result, publicationError(ErrState, "busy", "")
	}
	defer state.mu.Unlock()
	if state.closed.Load() {
		return result, publicationError(ErrState, "closed", "")
	}
	if state.pending != nil {
		return result, publicationError(ErrCleanup, "pending", "")
	}
	if prepared == nil || prepared.state == nil {
		return result, publicationError(ErrHandle, "prepared", "")
	}
	if ctx.Err() != nil {
		return result, publicationError(ErrCanceled, "context", "", ctx.Err(), context.Cause(ctx))
	}
	work, cancel := context.WithTimeout(ctx, state.options.Timeout)
	state.cancel = cancel
	state.accepted, state.started, state.complete = true, false, false
	state.reading = false
	state.changes = publicationChanges{}
	state.last = result
	state.last.Namespace, state.last.Code = prepared.state.info.Namespace, prepared.state.info.Code
	var err error
	if state.pg != nil {
		tx, problem := state.pg.Begin(work, postgres.TxOptions{Isolation: postgres.ReadCommitted, Access: postgres.ReadWrite})
		err = problem
		if tx != nil {
			state.pending, _ = postgresPublication(tx, state.target)
		}
	} else {
		tx, problem := state.my.Begin(work, mysql.TxOptions{Isolation: sql.LevelReadCommitted})
		err = problem
		if tx != nil {
			state.pending, _ = mysqlPublication(tx, state.target)
		}
	}
	if state.pending == nil {
		cancel()
		state.cancel = nil
		return state.last, publicationError(ErrDatabase, "begin", "", err)
	}
	if err == nil {
		state.started = true
		state.changes, err = state.pending.register(work, prepared)
		state.complete = err == nil
	}
	var finalError error
	if err == nil {
		final, problem := state.pending.finish(work, true)
		finalError = problem
		state.observe(final)
	}
	if state.pending != nil {
		cleanup, stop := context.WithTimeout(context.Background(), state.options.CleanupTimeout)
		final, problem := state.pending.finish(cleanup, false)
		stop()
		finalError = publicationErrors(finalError, problem)
		state.observe(final)
	}
	if state.pending != nil {
		finalError = publicationErrors(finalError, publicationError(ErrCleanup, "pending", ""))
	}
	if state.last.Effect != Applied && state.last.Effect != Unchanged && err == nil && finalError == nil {
		finalError = publicationError(ErrDatabase, "registration_unproven", "")
	}
	return state.last, publicationErrors(err, finalError, state.cleanup)
}

func publicationErrors(problems ...error) error {
	var result error
	for _, problem := range problems {
		if problem == nil {
			continue
		}
		if result == nil {
			result = problem
		} else {
			result = errors.Join(result, problem)
		}
	}
	return result
}
func (state *catalogState) observe(final publicationFinal) {
	if !state.reading {
		state.last = publicationResult(state.last, final, state.started, state.complete, state.changes)
	}
	if final.cleanup != nil && state.cleanup == nil {
		state.cleanup = publicationError(ErrCleanup, "failed", "", final.cleanup)
		state.closed.Store(true)
	}
	if !state.reading {
		state.last.CleanupFailed = state.last.CleanupFailed || state.cleanup != nil
	}
	if final.released {
		if state.cancel != nil {
			state.cancel()
			state.cancel = nil
		}
		state.pending = nil
		state.reading = false
	}
}
func publicationResult(previous Publication, final publicationFinal, started, complete bool, changes publicationChanges) Publication {
	value := noPublication()
	value.Namespace, value.Code = previous.Namespace, previous.Code
	value.CleanupComplete, value.CleanupFailed = final.released, final.cleanup != nil
	if started {
		value.Effect = Unknown
	}
	switch final.acknowledgement {
	case "commit-acknowledged":
		value.Acknowledgement = CommitAcknowledged
		if started && complete && changes.definitionID > 0 {
			value.Effect = Unchanged
			if changes.changed() {
				value.Effect = Applied
			}
			value.DefinitionID = strconv.FormatInt(changes.definitionID, 10)
			value.Changes = Changes{changes.definitionCreated, changes.categories, changes.versions, changes.modes, changes.revisions, changes.resources, changes.texts, changes.tags}
		}
	case "rollback-acknowledged":
		value.Acknowledgement = RollbackAcknowledged
		if started {
			value.Effect = RolledBack
		}
	case "commit-rolled-back":
		value.Acknowledgement = CommitRolledBack
		if started {
			value.Effect = RolledBack
		}
	case "finalization-unknown":
		value.Acknowledgement = AcknowledgementUnknown
	}
	return value
}

// Close seals the catalog and joins retained work without committing or replaying
// publication. A failed wait is retryable on this same handle; it does not close
// the borrowed client/source. Nil means cleanup settled, not business success.
// An overlapping operation is not canceled: Close seals future work and returns ErrState.
func (catalog *Catalog) Close(ctx context.Context) error {
	if catalog == nil || catalog.state == nil || ctx == nil {
		return publicationError(ErrInput, "close", "")
	}
	state := catalog.state
	state.closed.Store(true)
	if !state.mu.TryLock() {
		return publicationError(ErrState, "busy", "")
	}
	defer state.mu.Unlock()
	if state.pending == nil {
		return state.cleanup
	}
	final, err := state.pending.finish(ctx, false)
	state.observe(final)
	if !final.released {
		return publicationError(ErrCleanup, "pending", "", err)
	}
	return state.cleanup
}

// Released reports absence of catalog-owned retained native work, not source
// shutdown or remote effects. An overlapping observation conservatively returns false.
func (catalog *Catalog) Released() bool {
	if catalog == nil || catalog.state == nil || !catalog.state.mu.TryLock() {
		return false
	}
	defer catalog.state.mu.Unlock()
	return catalog.state.pending == nil
}

// LastPublication returns the detached observation last recorded by Register or
// Close. It performs no database query and does not discard the original call's
// error. Refused/overlapping calls do not overwrite an earlier accepted report.
func (catalog *Catalog) LastPublication() (Publication, error) {
	if catalog == nil || catalog.state == nil {
		return Publication{}, publicationError(ErrInput, "catalog", "")
	}
	state := catalog.state
	if !state.mu.TryLock() {
		return Publication{}, publicationError(ErrState, "busy", "")
	}
	defer state.mu.Unlock()
	if !state.accepted {
		return Publication{}, publicationError(ErrState, "no_publication", "")
	}
	return state.last, nil
}
func (Catalog) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("catalog.Catalog")) }
func (Catalog) LogValue() slog.Value           { return slog.StringValue("catalog.Catalog") }
func (Catalog) MarshalJSON() ([]byte, error) {
	return nil, publicationError(ErrSerialization, "catalog", "")
}
func (*Catalog) UnmarshalJSON([]byte) error        { return publicationError(ErrSerialization, "catalog", "") }
func (Publication) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("catalog.Publication")) }
func (Publication) LogValue() slog.Value           { return slog.StringValue("catalog.Publication") }
