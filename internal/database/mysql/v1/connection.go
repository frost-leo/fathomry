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
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

// Source is an opaque assembly token; it exposes no native or owner methods.
type Source struct {
	private
	owner *pool
}
type pool struct {
	settings    settings
	db          *sql.DB
	mu          sync.Mutex
	sealed      bool
	connects    sync.WaitGroup
	connections sync.WaitGroup
	errors      []error
	closeOnce   sync.Once
	closed      chan struct{}
	dial        func(context.Context, string, string) (net.Conn, error)
}

// Select freezes typed version-1 settings and strict optional overlays before
// constructing local pool ownership. No server query or remote resource is created.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: 1, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, s settings) (resource.Resource[Source], error) {
		owner := &pool{settings: s, closed: make(chan struct{}), dial: (&net.Dialer{}).DialContext}
		owner.db = sql.OpenDB(owner)
		owner.db.SetMaxOpenConns(s.MaxConnections)
		owner.db.SetMaxIdleConns(s.MaxIdleConnections)
		owner.db.SetConnMaxIdleTime(s.MaxIdleTime)
		owner.db.SetConnMaxLifetime(s.MaxLifetime)
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: owner}, Release: owner.close}, nil
	}), nil
}

// Database is a non-owning concurrent facade. Copies share the same original
// pool, admission allowance and independent evidence inbox.
type Database struct {
	private
	owner    *pool
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

func Bind(assembly *resource.Assembly, selection resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Database, error) {
	source, _, err := resource.Bind(assembly, selection)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selection)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	s, limits := source.owner.settings, access.Limits()
	if limits.Active > s.MaxConnections || limits.Bytes < s.reservation() || limits.MaxLeases < 4 || limits.Queued > 64 ||
		limits.Queued > 0 && limits.QueuedBytes < s.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Database{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}
func (p *pool) Driver() driver.Driver { return &sdk.MySQLDriver{} }

type acquisitionKey struct{}
type acquisitionError struct{ cause error }

func (*acquisitionError) Error() string { return "mysql: native acquisition failed" }

func (p *pool) Connect(ctx context.Context) (connection driver.Conn, resultError error) {
	_, directlyOwned := ctx.Value(acquisitionKey{}).(bool)
	defer func() {
		if resultError == nil {
			return
		}
		if !directlyOwned {
			p.mu.Lock()
			if !p.sealed {
				p.sealed = true
				p.errors = append(p.errors, resultError)
			}
			p.mu.Unlock()
		}
		// Hide ErrBadConn only from database/sql's private acquisition retry
		// loop. take restores the complete original cause before public handoff.
		resultError = &acquisitionError{cause: resultError}
	}()
	p.mu.Lock()
	if p.sealed {
		err := failure(ErrState, "pool", p.errors...)
		p.mu.Unlock()
		return nil, err
	}
	p.connects.Add(1)
	p.mu.Unlock()
	defer p.connects.Done()
	ctx, cancel := context.WithTimeout(ctx, p.settings.Timeout)
	defer cancel()
	w, err := openWire(ctx, p.settings, p.dial)
	if err != nil {
		return nil, failure(ErrConnect, "dial", err)
	}
	stop := w.activate(ctx)
	defer stop()
	cfg := nativeConfig(p.settings)
	// The SDK has already skipped its TLS-upgrade branch when this owned Write
	// performs the upgrade. Mark only this per-connection config secure afterward,
	// so native caching_sha2 full authentication uses its correct TLS branch.
	_ = cfg.Apply(sdk.BeforeConnect(func(_ context.Context, actual *sdk.Config) error {
		w.secured = func(config *tls.Config) { actual.TLS = config }
		return nil
	}))
	var handed bool
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		if handed {
			return nil, failure(ErrState, "dial-reuse")
		}
		handed = true
		return w, nil
	}
	connector, err := sdk.NewConnector(cfg)
	var native driver.Conn
	if err == nil {
		native, err = connector.Connect(nativeContext{ctx})
	}
	if err != nil {
		_ = w.Close()
		transport, cleanup := w.evidence()
		failed := &connectFailure{primary: joined(ErrConnect, "connect", err, transport, contextCause(ctx, err)), cleanup: joined(ErrCleanup, "connect-close", cleanup)}
		if cleanup != nil {
			p.mu.Lock()
			p.sealed = true
			p.errors = append(p.errors, failed)
			p.mu.Unlock()
		}
		return nil, failed
	}
	p.connections.Add(1)
	return &managedConn{Conn: native, wire: w, pool: p}, nil
}

type connectFailure struct{ primary, cleanup error }

func (*connectFailure) Error() string { return "mysql: connection failed" }
func (e *connectFailure) Unwrap() []error {
	if e.cleanup == nil {
		return []error{e.primary}
	}
	return []error{e.primary, e.cleanup}
}

func contextCause(ctx context.Context, err error) error {
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return context.Cause(ctx)
	}
	return nil
}
func (p *pool) close(ctx context.Context) resource.ReleaseResult {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.sealed = true
		p.mu.Unlock()
		go func() {
			err := p.db.Close()
			p.connects.Wait()
			p.connections.Wait()
			p.mu.Lock()
			if err != nil {
				p.errors = append(p.errors, err)
			}
			p.mu.Unlock()
			close(p.closed)
		}()
	})
	select {
	case <-p.closed:
		p.mu.Lock()
		err := errors.Join(p.errors...)
		p.mu.Unlock()
		return resource.ReleaseResult{Quiescent: true, Released: true, Err: err}
	case <-ctx.Done():
		return resource.ReleaseResult{Err: failure(ErrCleanup, "pool-close", ctx.Err(), context.Cause(ctx)), Continue: p.close}
	}
}
func (p *pool) take(ctx context.Context) (*sql.Conn, *managedConn, error) {
	p.mu.Lock()
	if p.sealed {
		err := failure(ErrState, "pool-unavailable", p.errors...)
		p.mu.Unlock()
		return nil, nil, err
	}
	p.mu.Unlock()
	conn, err := p.db.Conn(context.WithValue(nativeContext{ctx}, acquisitionKey{}, true))
	if err != nil {
		if failure, ok := err.(*acquisitionError); ok {
			err = failure.cause
		}
		var failed *connectFailure
		if errors.As(err, &failed) {
			err = &connectFailure{primary: joined(ErrConnect, "acquire", failed.primary, contextCause(ctx, err)), cleanup: failed.cleanup}
		}
		return nil, nil, joined(ErrConnect, "acquire", err, contextCause(ctx, err))
	}
	var owned *managedConn
	err = conn.Raw(func(raw any) error {
		var ok bool
		owned, ok = raw.(*managedConn)
		if !ok {
			return failure(ErrState, "native-connection")
		}
		return nil
	})
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	owned.start()
	return conn, owned, nil
}
func give(conn *sql.Conn, owned *managedConn, retire bool) error {
	if retire {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	err := conn.Close()
	if errors.Is(err, sql.ErrConnDone) {
		err = nil
	}
	return joined(ErrCleanup, "connection-return", err, owned.cleanup())
}

type managedConn struct {
	driver.Conn
	wire        *wire
	pool        *pool
	mu          sync.Mutex
	cleanups    []error
	transaction *nativeTransaction
	closeOnce   sync.Once
	closeError  error
}

func (c *managedConn) start() { c.mu.Lock(); c.cleanups = nil; c.mu.Unlock(); c.wire.clearEvidence() }
func (c *managedConn) record(err error) {
	if err != nil {
		c.mu.Lock()
		c.cleanups = append(c.cleanups, err)
		c.mu.Unlock()
	}
}
func (c *managedConn) cleanup() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, wireClose := c.wire.evidence()
	return joined(ErrCleanup, "native-cleanup", append(append([]error(nil), c.cleanups...), wireClose)...)
}
func (c *managedConn) Close() error {
	c.closeOnce.Do(func() {
		defer c.pool.connections.Done()
		ctx, cancel := context.WithTimeout(context.Background(), c.pool.settings.CloseTimeout)
		defer cancel()
		stop := c.wire.activate(ctx)
		defer stop()
		err := c.Conn.Close()
		wireErr := c.wire.Close()
		c.closeError = joined(ErrCleanup, "native-close", err, wireErr)
		c.record(c.closeError)
		c.pool.mu.Lock()
		if c.closeError != nil {
			// Stop replacement/admission after lost native cleanup evidence. This
			// bounds history by the already-open population, including idle expiry.
			c.pool.sealed = true
			c.pool.errors = append(c.pool.errors, c.closeError)
		}
		c.pool.mu.Unlock()
	})
	return c.closeError
}
func (c *managedConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}
func (c *managedConn) IsValid() bool {
	return !c.wire.closed.Load() && c.Conn.(driver.Validator).IsValid()
}
func (c *managedConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	work, cancel := context.WithTimeout(ctx, c.pool.settings.Timeout)
	defer cancel()
	stop := c.wire.activate(work)
	defer stop()
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(work, options)
	if err != nil {
		return nil, operationError(work, err, c)
	}
	wrapped := &nativeTransaction{Tx: tx, owner: c, done: make(chan struct{})}
	c.transaction = wrapped
	return wrapped, nil
}

// Stats reports native pool counters without granting client or lifecycle access.
// Counts and wait durations do not establish service health or memory usage.
func (db *Database) Stats() sql.DBStats {
	if db == nil || db.owner == nil {
		return sql.DBStats{}
	}
	return db.owner.db.Stats()
}

// Ping performs a controlled native COM_PING and retains independent evidence.
// Local construction and a successful Ping are not application acceptance.
func (db *Database) Ping(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	call, err := db.beginCall(ctx, id, "ping", invocation.Finite, nil)
	if err != nil {
		return nil, err
	}
	receipt := call.Receipt()
	work, cancel, err := (invocation.Budget{Limit: db.owner.settings.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return receipt, nil
	}
	defer cancel()
	conn, owned, err := db.owner.take(work)
	if err != nil {
		call.Complete(connectionOutcome(err))
		return receipt, nil
	}
	err = rawNative(conn, func(c *managedConn) error {
		stop := c.wire.activate(work)
		defer stop()
		if err := work.Err(); err != nil {
			return err
		}
		_, _ = call.Attempt()
		nativeErr := c.Conn.(driver.Pinger).Ping(nativeContext{work})
		stop()
		return operationError(work, nativeErr, c)
	})
	data := &resultData{complete: err == nil, serverVersion: owned.wire.version}
	cleanup := give(conn, owned, err != nil || owned.wire.closed.Load())
	call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: err, Cleanup: cleanup})
	return receipt, nil
}

type nativeTransaction struct {
	driver.Tx
	owner     *managedConn
	mu        sync.Mutex
	next      context.Context
	done      chan struct{}
	err       error
	committed bool
}

func (tx *nativeTransaction) setContext(ctx context.Context) {
	tx.mu.Lock()
	tx.next = ctx
	tx.mu.Unlock()
}
func (tx *nativeTransaction) Commit() error   { return tx.finish(true) }
func (tx *nativeTransaction) Rollback() error { return tx.finish(false) }
func (tx *nativeTransaction) finish(commit bool) error {
	defer func() {
		tx.Tx = nil
		tx.mu.Lock()
		tx.next = nil
		tx.mu.Unlock()
		close(tx.done)
	}()
	tx.mu.Lock()
	parent := tx.next
	tx.mu.Unlock()
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, tx.owner.pool.settings.CloseTimeout)
	defer cancel()
	stop := tx.owner.wire.activate(ctx)
	defer stop()
	if ctx.Err() != nil {
		_ = tx.owner.wire.Close()
		tx.err = failure(ErrQuery, "finalize", ctx.Err(), context.Cause(ctx))
		return tx.err
	}
	tx.committed = commit
	if commit {
		tx.err = tx.Tx.Commit()
	} else {
		tx.err = tx.Tx.Rollback()
	}
	tx.err = operationError(ctx, tx.err, tx.owner)
	return tx.err
}
