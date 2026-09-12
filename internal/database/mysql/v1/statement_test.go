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
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestReusablePreparationPinsOnlyItsConnection(t *testing.T) {
	peer := newPeer(t, true, true)
	f := bindFixture(t, peer.options(), 2)
	ctx, cancel := context.WithCancel(context.Background())
	stmt, receipt, err := f.db.Prepare(ctx, correlation("prepared"), "SELECT ?")
	if err != nil || stmt == nil {
		t.Fatal("prepare failed", err)
	}
	cancel()
	t.Cleanup(func() { _, _ = stmt.Close(context.Background()) })
	initial, ready := receipt.Result()
	if !ready || initial.Final || initial.Released || !initial.Outcome.Value.Complete() {
		t.Fatal("preparation lost its live ownership")
	}
	if f.db.Stats().InUse != 1 || f.db.Stats().OpenConnections != 1 {
		t.Fatal("prepared statement did not pin native connection")
	}
	if _, err := f.db.Query(context.Background(), correlation("other"), "SELECT empty"); !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("prepared statement acquired no bounded allowance")
	}

	root, err := f.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"first", "second", "third"} {
		child, err := stmt.Query(context.Background(), correlation(value), value)
		result := observe(t, child, err)
		row, rowErr := result.Outcome.Value.First()
		if result.Err() != nil || rowErr != nil || string(row.ValuesCopy()[0]) != value || !result.Nested || result.Context.Correlation.Parent != "prepared" {
			t.Fatal("reused statement lost native data or ownership", result.Err())
		}
		drain(t, f.inbox, 1)
	}
	if !errors.Is(root.Release(), invocation.ErrPending) {
		t.Fatal("live prepared evidence was released")
	}
	final, err := stmt.Close(context.Background())
	result := observe(t, final, err)
	if result.Err() != nil || f.db.Stats().InUse != 0 {
		t.Fatal("statement cleanup failed", result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err = stmt.Query(context.Background(), correlation("closed"), "later"); !errors.Is(err, ErrState) {
		t.Fatal("closed preparation reused native handle")
	}
	if _, err = stmt.Close(context.Background()); err != nil {
		t.Fatal("idempotent close changed evidence")
	}
	conformance.Facade(t, stmt, "Query", "Exec", "Close", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
}

func TestTransactionPreparedStatementsCloseWithTheirOwner(t *testing.T) {
	peer := newPeer(t, false, false)
	f := bindFixture(t, peer.options(), 4)
	tx := beginTx(t, f, context.Background())
	stmt, _, err := tx.Prepare(context.Background(), correlation("tx-prepare"), "SELECT ?")
	if err != nil || stmt == nil {
		t.Fatal("transaction prepare failed", err)
	}
	child, err := stmt.Query(context.Background(), correlation("tx-query"), "inside")
	result := observe(t, child, err)
	row, rowErr := result.Outcome.Value.First()
	if result.Err() != nil || rowErr != nil || string(row.ValuesCopy()[0]) != "inside" || result.Context.Correlation.Parent != "tx-prepare" {
		t.Fatal("transaction preparation lost context", result.Err())
	}
	final, err := tx.Commit(context.Background())
	if result := observe(t, final, err); result.Err() != nil || result.Outcome.Value.TransactionOutcome() != CommitAcknowledged {
		t.Fatal("transaction with prepared handle did not finish", result.Err())
	}
	if _, err = stmt.Query(context.Background(), correlation("after"), "outside"); !errors.Is(err, ErrState) {
		t.Fatal("statement outlived transaction")
	}
	drain(t, f.inbox, 3)
}

type failedStatement struct {
	driver.Stmt
	calls *atomic.Int32
}

func (s *failedStatement) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	s.calls.Add(1)
	return nil, driver.ErrBadConn
}
func (s *failedStatement) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	s.calls.Add(1)
	return nil, driver.ErrBadConn
}
func TestPreparedNativeErrorsAreNotRetriedOrHidden(t *testing.T) {
	peer := newPeer(t, false, false)
	f := bindFixture(t, peer.options(), 2)
	stmt, _, err := f.db.Prepare(context.Background(), correlation("prepare-error"), "INSERT INTO fixture VALUES (?)")
	if err != nil || stmt == nil {
		t.Fatal("prepare failed", err)
	}
	var calls atomic.Int32
	stmt.native = &failedStatement{Stmt: stmt.native, calls: &calls}
	receipt, err := stmt.Exec(context.Background(), correlation("bad-connection"), "value")
	result := observe(t, receipt, err)
	if calls.Load() != 1 || !errors.Is(result.Err(), driver.ErrBadConn) {
		t.Fatal("controlled native statement replayed or hid ErrBadConn")
	}
	_, _ = stmt.Close(context.Background())
	drain(t, f.inbox, 2)
}

type retryProbe struct{ calls atomic.Int32 }

func (p *retryProbe) Connect(context.Context) (driver.Conn, error) { return &retryProbeConn{p}, nil }
func (*retryProbe) Driver() driver.Driver                          { return &unusedDriver{} }

type unusedDriver struct{}

func (*unusedDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unused") }

type retryProbeConn struct{ owner *retryProbe }

func (c *retryProbeConn) Prepare(string) (driver.Stmt, error) {
	return &retryProbeStmt{owner: c.owner}, nil
}
func (*retryProbeConn) Close() error              { return nil }
func (*retryProbeConn) Begin() (driver.Tx, error) { return retryProbeTx{}, nil }

type retryProbeTx struct{}

func (retryProbeTx) Commit() error   { return nil }
func (retryProbeTx) Rollback() error { return nil }

type retryProbeStmt struct{ owner *retryProbe }

func (*retryProbeStmt) Close() error  { return nil }
func (*retryProbeStmt) NumInput() int { return 0 }
func (s *retryProbeStmt) Exec([]driver.Value) (driver.Result, error) {
	s.owner.calls.Add(1)
	return nil, driver.ErrBadConn
}
func (*retryProbeStmt) Query([]driver.Value) (driver.Rows, error) { return nil, errors.New("unused") }
func TestStandardSQLTransactionStatementRetryCounterexample(t *testing.T) {
	probe := new(retryProbe)
	db := sql.OpenDB(probe)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("UPDATE fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if !errors.Is(err, driver.ErrBadConn) || probe.calls.Load() != 3 {
		t.Fatal("standard-library retry counterexample changed")
	}
	t.Log("Scripted stdlib control: a transaction's sql.Stmt.Exec made three driver calls. Controlled native statement execution makes one and retains the original cause.")
}
