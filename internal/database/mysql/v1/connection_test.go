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
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

func TestPingPoolStatisticsAndOrdinarySQL(t *testing.T) {
	peer := newPeer(t, true, false)
	f := bindFixture(t, peer.options(), 1)
	if f.db.Stats().OpenConnections != 0 {
		t.Fatal("construction performed hidden readiness work")
	}
	receipt, err := f.db.Ping(context.Background(), correlation("ping"))
	if result := observe(t, receipt, err); result.Err() != nil || !result.Outcome.Value.Complete() {
		t.Fatal("native Ping failed", result.Err())
	}
	drain(t, f.inbox, 1)
	for _, query := range []string{"CREATE TABLE fixture (value int)", "SET @fixture = 1"} {
		receipt, err = f.db.Exec(context.Background(), correlation("ordinary-sql"), query)
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal("ordinary authorized native SQL was filtered", result.Err())
		}
		drain(t, f.inbox, 1)
	}
	if f.db.Stats().Idle != 1 || f.db.Stats().InUse != 0 || peer.secure.Load() != 1 {
		t.Fatal("native pool state or connection reuse changed")
	}
}

func TestStatementOnlyErrorDoesNotAbortTransaction(t *testing.T) {
	peer := newPeer(t, false, false)
	f := bindFixture(t, peer.options(), 5)
	tx := beginTx(t, f, context.Background())
	receipt, err := tx.Query(context.Background(), correlation("duplicate"), "SELECT duplicate")
	result := observe(t, receipt, err)
	var native *sdk.MySQLError
	if !errors.As(result.Err(), &native) || native.Number != 1062 || tx.ended {
		t.Fatal("statement error invented a transaction rollback")
	}
	for _, query := range []string{"SAVEPOINT fixture", "ROLLBACK TO SAVEPOINT fixture", "RELEASE SAVEPOINT fixture"} {
		receipt, err = tx.Exec(context.Background(), correlation("savepoint"), query)
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal("native savepoint SQL was unavailable", result.Err())
		}
	}
	receipt, err = tx.Commit(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil || result.Outcome.Value.TransactionOutcome() != CommitAcknowledged {
		t.Fatal("recoverable native transaction was refused", result.Err())
	}
	drain(t, f.inbox, 5)
}

func TestLocalInfileRegistryCannotBypassTransport(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newPeer(t, secure, false)
		path := filepath.Join(t.TempDir(), "registered.txt")
		if err := os.WriteFile(path, []byte("synthetic-file-canary"), 0600); err != nil {
			t.Fatal(err)
		}
		sdk.RegisterLocalFile(path)
		t.Cleanup(func() { sdk.DeregisterLocalFile(path) })
		peer.upload = path
		f := bindFixture(t, peer.options(), 1)
		receipt, err := f.db.Query(context.Background(), correlation("infile"), "SELECT infile")
		result := observe(t, receipt, err)
		if !errors.Is(result.Err(), ErrUnsupported) || peer.fileCalls.Load() != 0 {
			t.Fatal("registered file crossed the instance transport boundary")
		}
		drain(t, f.inbox, 1)
	}
}
func TestRegisteredFileNativePositiveControl(t *testing.T) {
	peer := newPeer(t, false, false)
	path := filepath.Join(t.TempDir(), "native-registered.txt")
	if err := os.WriteFile(path, []byte("synthetic-native-canary"), 0600); err != nil {
		t.Fatal(err)
	}
	sdk.RegisterLocalFile(path)
	defer sdk.DeregisterLocalFile(path)
	peer.upload = path
	config := nativeConfig(defaults(peer.options()))
	connector, err := sdk.NewConnector(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.(driver.QueryerContext).QueryContext(ctx, "SELECT infile", nil)
	if err != nil {
		t.Fatal("native file control did not complete")
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	if peer.fileCalls.Load() == 0 {
		t.Fatal("native positive control did not upload the synthetic registered file")
	}
}

type closeWitness struct {
	net.Conn
	cause  error
	closes atomic.Int32
}

func (c *closeWitness) Close() error { c.closes.Add(1); _ = c.Conn.Close(); return c.cause }
func TestSocketCleanupErrorSurvivesNativeLoggingAndRepeatedClose(t *testing.T) {
	peer := newPeer(t, false, false)
	options := peer.options()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "cleanup", selected)
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("close-cause-canary")
	var witness *closeWitness
	source.owner.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		witness = &closeWitness{Conn: conn, cause: cause}
		return witness, nil
	}
	inbox, _ := invocation.NewInbox[Result](1, defaults(options).evidenceReservation())
	db, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := db.Ping(context.Background(), correlation("ping"))
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, inbox, 1)
	for range 2 {
		err := assembly.Close(context.Background())
		if !errors.Is(err, cause) || errors.Is(err, resource.ErrIncomplete) {
			t.Fatal("native socket cleanup evidence was discarded")
		}
		conformance.Private(t, err, "close-cause-canary")
	}
	if witness == nil || witness.closes.Load() != 1 {
		t.Fatal("socket ownership was not idempotent")
	}
}

func TestNativeIsolationOptions(t *testing.T) {
	peer := newPeer(t, false, false)
	f := bindFixture(t, peer.options(), 1)
	for _, isolation := range []sql.IsolationLevel{sql.LevelDefault, sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSerializable} {
		tx, receipt, err := f.db.Begin(context.Background(), correlation("isolation"), TxOptionsV1{Isolation: isolation, ReadOnly: true})
		if err != nil || tx == nil {
			t.Fatal("native MySQL isolation rejected", err)
		}
		receipt, err = tx.Rollback(context.Background())
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, f.inbox, 1)
	}
}

func TestBorrowedSourceKeepsOriginalIdentityAndPool(t *testing.T) {
	peer := newPeer(t, false, false)
	f := bindFixture(t, peer.options(), 1)
	borrowed := resource.Borrow("alias", f.assembly, f.selected)
	borrower, err := resource.Assemble(context.Background(), context.Background(), "borrower", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer borrower.Close(context.Background())
	alias, err := Bind(borrower, borrowed, f.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(f.assembly.Close(context.Background()), resource.ErrIncomplete) {
		t.Fatal("owner closed its borrowed source")
	}
	receipt, err := alias.Ping(context.Background(), correlation("alias"))
	result := observe(t, receipt, err)
	if result.Err() != nil || result.Source.Scope != "fixture" || result.Source.Configuration.Identity.Name != "fixture" {
		t.Fatal("alias relabeled or closed its actual source", result.Err())
	}
	drain(t, f.inbox, 1)
}

func TestNativePoolExpiresAndReplacesIdleConnections(t *testing.T) {
	for _, idle := range []bool{false, true} {
		peer := newPeer(t, true, false)
		options := peer.options()
		if idle {
			options.MaxIdleTime = 10 * time.Millisecond
		} else {
			options.MaxLifetime = 10 * time.Millisecond
		}
		fixture := bindFixture(t, options, 1)
		receipt, err := fixture.db.Ping(context.Background(), correlation("warm"))
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, fixture.inbox, 1)
		deadline := time.Now().Add(3 * time.Second)
		for {
			stats := fixture.db.Stats()
			if idle && stats.MaxIdleTimeClosed >= 1 || !idle && stats.MaxLifetimeClosed >= 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("native pool expiration did not finish")
			}
			time.Sleep(10 * time.Millisecond)
			if !idle {
				receipt, err = fixture.db.Ping(context.Background(), correlation("expire"))
				if result := observe(t, receipt, err); result.Err() != nil {
					t.Fatal(result.Err())
				}
				drain(t, fixture.inbox, 1)
			}
		}
		receipt, err = fixture.db.Ping(context.Background(), correlation("replacement"))
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, fixture.inbox, 1)
		if peer.secure.Load() < 2 || fixture.db.Stats().InUse != 0 {
			t.Fatal("expired native connection was not replaced")
		}
	}
}

func TestConnectorFailureIsNotImplicitlyRetried(t *testing.T) {
	peer := newPeer(t, false, false)
	fixture := bindFixture(t, peer.options(), 1)
	var calls atomic.Int32
	fixture.db.owner.dial = func(context.Context, string, string) (net.Conn, error) {
		calls.Add(1)
		return nil, driver.ErrBadConn
	}
	receipt, err := fixture.db.Ping(context.Background(), correlation("acquire"))
	result := observe(t, receipt, err)
	if !errors.Is(result.Err(), driver.ErrBadConn) || calls.Load() != 1 {
		t.Fatal("database/sql retried or concealed the original acquisition failure")
	}
	drain(t, fixture.inbox, 1)
}

func TestConnectionCancellationKeepsOwnerCause(t *testing.T) {
	peer := newPeer(t, false, false)
	fixture := bindFixture(t, peer.options(), 1)
	cause := errors.New("acquisition-cause-canary")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	fixture.db.owner.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		cancel(cause)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	receipt, err := fixture.db.Ping(ctx, correlation("cancel-acquire"))
	result := observe(t, receipt, err)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) {
		t.Fatal("owner cancellation cause was lost at native acquisition")
	}
	conformance.Private(t, result.Err(), "acquisition-cause-canary")
	drain(t, fixture.inbox, 1)
}

func TestRegisteredReaderDeniedBeforeCallback(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newPeer(t, secure, false)
		var calls atomic.Int32
		name := "gh25-reader-control"
		sdk.RegisterReaderHandler(name, func() io.Reader { calls.Add(1); return strings.NewReader("reader-canary") })
		t.Cleanup(func() { sdk.DeregisterReaderHandler(name) })
		peer.upload = "Reader::" + name
		fixture := bindFixture(t, peer.options(), 1)
		receipt, err := fixture.db.Query(context.Background(), correlation("reader"), "SELECT infile")
		if result := observe(t, receipt, err); !errors.Is(result.Err(), ErrUnsupported) || calls.Load() != 0 || peer.fileCalls.Load() != 0 {
			t.Fatal("global reader registration bypassed the instance boundary")
		}
		drain(t, fixture.inbox, 1)
	}
}

type delayedSocketClose struct {
	net.Conn
	started chan struct{}
	finish  <-chan struct{}
}

func (c *delayedSocketClose) Close() error {
	err := c.Conn.Close()
	close(c.started)
	<-c.finish
	return err
}
func TestPoolCloseJoinsAlreadyRunningIdleRetirement(t *testing.T) {
	peer := newPeer(t, false, false)
	options := peer.options()
	options.MaxIdleTime = 10 * time.Millisecond
	fixture := bindFixture(t, options, 1)
	started, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(finish) }) }
	defer unblock()
	fixture.db.owner.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &delayedSocketClose{Conn: conn, started: started, finish: finish}, nil
	}
	receipt, err := fixture.db.Ping(context.Background(), correlation("idle-close"))
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, fixture.inbox, 1)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("native idle retirement did not enter socket cleanup")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result := fixture.db.owner.close(ctx)
	if result.Released || result.Quiescent || result.Continue == nil || !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Error("DB.Close completion hid already-running native cleanup")
	}
	unblock()
	result = fixture.db.owner.close(context.Background())
	if !result.Released || !result.Quiescent || result.Err != nil {
		t.Fatal("eventual native cleanup was not joined")
	}
}

func TestIndependentAssembliesDoNotSharePoolsOrAdmission(t *testing.T) {
	peer := newPeer(t, true, false)
	first := bindFixture(t, peer.options(), 2)
	second := bindFixture(t, peer.options(), 1)
	if first.db.owner == second.db.owner || first.db.access == second.db.access {
		t.Fatal("independent sources shared ownership")
	}
	tx := beginTx(t, first, context.Background())
	if _, err := first.db.Ping(context.Background(), correlation("saturated")); !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("first source admission was not saturated")
	}
	receipt, err := second.db.Ping(context.Background(), correlation("independent"))
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("second source inherited first-source saturation", result.Err())
	}
	drain(t, second.inbox, 1)
	if err := second.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	receipt, err = tx.Commit(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("closing another source terminated this transaction", result.Err())
	}
	drain(t, first.inbox, 1)
	if peer.secure.Load() != 2 {
		t.Fatal("independent native TLS sessions were not constructed")
	}
}

func TestFailedTLSInitializationRetainsIndependentSocketCleanup(t *testing.T) {
	peer := newPeer(t, true, false)
	options := peer.options()
	options.ServerName = "wrong.fixture.invalid"
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "failed-tls", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = assembly.Close(context.Background()) })
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("failed-initialization-close-canary")
	var witness *closeWitness
	source.owner.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		witness = &closeWitness{Conn: conn, cause: cause}
		return witness, nil
	}
	inbox, err := invocation.NewInbox[Result](1, defaults(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	db, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := db.Ping(context.Background(), correlation("failed-tls"))
	result := observe(t, receipt, err)
	if !errors.Is(result.Outcome.Primary, ErrConnect) || !errors.Is(result.Outcome.Cleanup, cause) || witness == nil || witness.closes.Load() != 1 {
		t.Fatal("partial native initialization lost independent cleanup evidence")
	}
	drain(t, inbox, 1)
	for range 2 {
		if err := assembly.Close(context.Background()); !errors.Is(err, cause) || errors.Is(err, resource.ErrIncomplete) {
			t.Fatal("eventual source close erased partial-initialization cleanup")
		}
	}
}
