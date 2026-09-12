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

package pgx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/jackc/pgx/v5"
)

func poolLifecycleFixture(t *testing.T, options OptionsV1, expectedClose error) boundFixture {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "pool-lifecycle", selected)
	if err != nil {
		t.Fatal(err)
	}
	var inbox *invocation.Inbox[Result]
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if inbox != nil && inbox.Usage().Outstanding != 0 {
			t.Error("pool lifecycle test abandoned independent evidence")
			for inbox.Usage().Outstanding > 0 {
				record, err := inbox.Next(ctx)
				if err != nil {
					break
				}
				if _, err := record.Receipt().WaitReleased(ctx); err != nil {
					break
				}
				if err := record.Release(); err != nil {
					break
				}
			}
		}
		err := assembly.Close(ctx)
		if expectedClose == nil && err != nil || expectedClose != nil && (!errors.Is(err, expectedClose) || errors.Is(err, resource.ErrIncomplete)) {
			t.Error("pool lifecycle fixture cleanup changed", err)
		}
	})
	inbox, err = invocation.NewInbox[Result](3, 3*defaults(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	database, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	return boundFixture{assembly, selected, database, inbox}
}

type poolLifecycleIdle struct {
	connection *connection
	witness    *closeWitness
	unblock    func()
}

func seedPoolLifecycleIdle(t *testing.T, owner *pool, blocked bool, cause error) poolLifecycleIdle {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case owner.acquire <- struct{}{}:
		defer func() { <-owner.acquire }()
	case <-ctx.Done():
		t.Fatal("native idle setup could not acquire the maintenance gate")
	}
	if err := owner.native.CreateResource(ctx); err != nil {
		t.Fatal("native idle setup failed", err)
	}
	idle := owner.native.AcquireAllIdle()
	if len(idle) != 1 {
		for _, handle := range idle {
			handle.ReleaseUnused()
		}
		t.Fatal("native idle setup did not isolate one resource")
	}
	// No foreground give runs: only the unattended worker can retire this seed.
	defer idle[0].ReleaseUnused()
	connection := idle[0].Value()
	witness := &closeWitness{entered: make(chan struct{}), unblock: make(chan struct{}), cause: cause}
	var once sync.Once
	unblock := func() { once.Do(func() { close(witness.unblock) }) }
	t.Cleanup(unblock)
	if !blocked {
		unblock()
	}
	connection.wire.mu.Lock()
	if len(connection.wire.sockets) == 1 {
		for socket := range connection.wire.sockets {
			witness.Conn = socket.Conn
			socket.Conn = witness
		}
	}
	connection.wire.mu.Unlock()
	if witness.Conn == nil {
		t.Fatal("native idle setup could not attach its independent socket witness")
	}
	return poolLifecycleIdle{connection: connection, witness: witness, unblock: unblock}
}

func waitPoolLifecycle(t *testing.T, ready func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitPoolLifecycleSignal(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal(message)
	}
}

func ownPoolLifecycleCall(t *testing.T, fixture boundFixture, receipt *invocation.Receipt[Result], finish func(context.Context) (*invocation.Receipt[Result], error)) *invocation.DeliveryRecord[Result] {
	t.Helper()
	var record *invocation.DeliveryRecord[Result]
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if _, err := finish(ctx); err != nil && !errors.Is(err, sdk.ErrTxClosed) {
			t.Error("held pool lifecycle scope cleanup failed", err)
		}
		result, err := receipt.WaitReleased(ctx)
		if err != nil {
			t.Error("held pool lifecycle scope remained active", err)
			return
		}
		if result.Outcome.Cleanup != nil {
			t.Error("held pool lifecycle scope cleanup evidence failed", result.Outcome.Cleanup)
		}
		if record != nil {
			if err := record.Release(); err != nil {
				t.Error(err)
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var err error
	record, err = fixture.inbox.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	received, _ := record.Receipt().Result()
	direct, _ := receipt.Result()
	if received.Context != direct.Context {
		t.Fatal("held pool lifecycle scope received different evidence")
	}
	return record
}

func TestPoolLifecycleUnattendedRetirement(t *testing.T) {
	for _, mode := range []string{"idle", "lifetime"} {
		t.Run(mode, func(t *testing.T) {
			peer := newProtocolPeer(t, false)
			options := peer.options()
			if mode == "idle" {
				options.MaxIdleTime = 10 * time.Millisecond
			} else {
				options.MaxLifetime = 10 * time.Millisecond
			}
			fixture := poolLifecycleFixture(t, options, nil)
			seed := seedPoolLifecycleIdle(t, fixture.database.owner, false, nil)
			waitPoolLifecycleSignal(t, seed.witness.entered, "unattended retirement never reached socket Close")
			waitPoolLifecycle(t, func() bool { return fixture.database.Stats().TotalResources() == 0 }, "unattended retirement retained native ownership")
			waitPoolLifecycleSignal(t, seed.connection.native.PgConn().CleanupDone(), "unattended retirement did not join native cleanup")
			if peer.connects.Load() != 1 || peer.queries.Load() != 0 || seed.witness.count.Load() != 1 ||
				fixture.inbox.Usage() != (invocation.InboxUsage{}) || fixture.assembly.Snapshot().Sources[0].Usage != (resource.Usage{}) {
				t.Fatal("unattended retirement depended on foreground work or repeated closure")
			}
			receipt, err := fixture.database.Ping(context.Background(), correlation("after-expiration"))
			result := operationResult(t, receipt, err)
			drain(t, fixture.inbox, 1)
			if result.Err() != nil || !result.Outcome.Value.Complete() || peer.connects.Load() != 2 {
				t.Fatal("expired native resource was not replaced once", result.Err())
			}
		})
	}
}

func TestPoolLifecycleHeldScopesSurviveMaintenance(t *testing.T) {
	for _, mode := range []string{"transaction", "preparation"} {
		t.Run(mode, func(t *testing.T) {
			peer := newProtocolPeer(t, false)
			options := peer.options()
			options.MaxConnections = 2
			options.MaxIdleTime, options.MaxLifetime = 10*time.Millisecond, 10*time.Millisecond
			fixture := poolLifecycleFixture(t, options, nil)
			var held *connection
			var receipt *invocation.Receipt[Result]
			var finish func(context.Context) (*invocation.Receipt[Result], error)
			var query func() (*invocation.Receipt[Result], error)
			if mode == "transaction" {
				transaction, started, err := fixture.database.Begin(context.Background(), correlation("held-transaction"), TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadWrite})
				if err != nil || transaction == nil {
					t.Fatal("held transaction setup failed", err)
				}
				held, receipt, finish = transaction.handle.Value(), started, transaction.Rollback
				query = func() (*invocation.Receipt[Result], error) {
					return transaction.Query(context.Background(), correlation("held-query"), "SELECT $1::text", "still-held")
				}
			} else {
				statement, started, err := fixture.database.Prepare(context.Background(), correlation("held-preparation"), "SELECT $1::text")
				if err != nil || statement == nil {
					t.Fatal("held preparation setup failed", err)
				}
				held, receipt, finish = statement.connection, started, statement.Close
				query = func() (*invocation.Receipt[Result], error) {
					return statement.Query(context.Background(), correlation("held-query"), "still-held")
				}
			}
			root := ownPoolLifecycleCall(t, fixture, receipt, finish)
			seed := seedPoolLifecycleIdle(t, fixture.database.owner, false, nil)
			waitPoolLifecycleSignal(t, seed.witness.entered, "maintenance did not inspect the concurrent idle resource")
			waitPoolLifecycle(t, func() bool { return fixture.database.Stats().TotalResources() == 1 }, "maintenance did not remove only its idle resource")
			if held.native.IsClosed() || fixture.database.Stats().AcquiredResources() != 1 || fixture.assembly.Snapshot().Sources[0].Usage.Active != 1 {
				t.Fatal("maintenance expired a retained active scope")
			}
			child, err := query()
			result := operationResult(t, child, err)
			drain(t, fixture.inbox, 1)
			row, firstErr := result.Outcome.Value.First()
			if result.Err() != nil || firstErr != nil || string(row.ValuesCopy()[0]) != "still-held" || peer.connects.Load() != 2 {
				t.Fatal("held resource lost native affinity after maintenance", result.Err())
			}
			finished, err := finish(context.Background())
			if result := operationResult(t, finished, err); result.Err() != nil {
				t.Fatal("held resource finalization failed", result.Err())
			}
			if err := root.Release(); err != nil {
				t.Fatal(err)
			}
			waitPoolLifecycleSignal(t, held.native.PgConn().CleanupDone(), "expired held resource did not join cleanup on return")
			if fixture.database.Stats().TotalResources() != 0 || fixture.assembly.Snapshot().Sources[0].Usage != (resource.Usage{}) {
				t.Fatal("expired retained resource returned to the idle pool")
			}
		})
	}
}

func TestPoolLifecycleBlockedRetirementAndCloseContinuation(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.MaxIdleTime = 10 * time.Millisecond
	cause := errors.New("retirement-close-canary")
	fixture := poolLifecycleFixture(t, options, cause)
	owner := fixture.database.owner
	seed := seedPoolLifecycleIdle(t, owner, true, cause)
	waitPoolLifecycleSignal(t, seed.witness.entered, "maintenance never entered blocked Close")
	if fixture.database.Stats().TotalResources() != 1 || fixture.database.Stats().AcquiredResources() != 1 {
		t.Fatal("blocked retirement released native capacity before cleanup")
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	canceled := errors.New("retirement-acquisition-cancel-canary")
	type response struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done, exited := make(chan response, 1), make(chan struct{})
	t.Cleanup(func() {
		cancel(canceled)
		seed.unblock()
		waitPoolLifecycleSignal(t, exited, "concurrent acquisition worker remained active")
	})
	go func() {
		defer close(exited)
		receipt, err := fixture.database.Ping(ctx, correlation("during-retirement"))
		done <- response{receipt, err}
	}()
	receiving, stopReceiving := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopReceiving()
	record, err := fixture.inbox.Next(receiving)
	if err != nil {
		t.Fatal("concurrent acquisition was not admitted", err)
	}
	t.Cleanup(func() {
		cancel(canceled)
		seed.unblock()
		waitPoolLifecycleSignal(t, exited, "accepted acquisition did not finish before evidence release")
		if err := record.Release(); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-exited:
		t.Fatal("acquisition did not wait for maintenance-owned capacity")
	default:
	}
	cancel(canceled)
	waitPoolLifecycleSignal(t, exited, "acquisition cancellation did not return while retirement was blocked")
	responseValue := <-done
	result := operationResult(t, responseValue.receipt, responseValue.err)
	received, available := record.Receipt().Result()
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), canceled) || result.Attempts.Observed != 0 || peer.connects.Load() != 1 ||
		!available || !received.Released || received.Context != result.Context || !errors.Is(received.Err(), canceled) {
		t.Fatal("retirement contention lost cancellation or began another native connection", result.Err())
	}
	if err := record.Release(); err != nil {
		t.Fatal(err)
	}
	closeContext, stopClose := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stopClose()
	err = fixture.assembly.Close(closeContext)
	if !errors.Is(err, resource.ErrIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("source close certified blocked retirement complete", err)
	}
	status := fixture.assembly.Snapshot().Sources[0]
	if !status.Pending || !status.CanContinue || status.Quiescent || status.Released || fixture.database.Stats().TotalResources() != 1 {
		t.Fatal("blocked retirement lost native or source continuation ownership")
	}
	seed.unblock()
	waitPoolLifecycleSignal(t, owner.maintained, "maintenance did not terminate before pool cleanup")
	cleanup, stopCleanup := context.WithTimeout(context.Background(), 4*time.Second)
	defer stopCleanup()
	err = fixture.assembly.Close(cleanup)
	if !errors.Is(err, cause) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("continued cleanup lost its original native error or timeout history", err)
	}
	conformance.Private(t, err, "retirement-close-canary")
	waitPoolLifecycleSignal(t, owner.closed, "continued source close did not join native pool shutdown")
	if seed.witness.count.Load() != 1 || peer.connects.Load() != 1 || fixture.database.Stats().TotalResources() != 0 {
		t.Fatal("retirement was repeated or hidden connection ownership remained")
	}
	if again := fixture.assembly.Close(cleanup); !errors.Is(again, cause) || !errors.Is(again, context.DeadlineExceeded) {
		t.Fatal("repeated close erased retirement history", again)
	}
}

func TestPoolLifecycleBackgroundFailureSealsAcquisition(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.MaxLifetime = 10 * time.Millisecond
	cause := errors.New("background-retirement-canary")
	fixture := poolLifecycleFixture(t, options, cause)
	owner := fixture.database.owner
	seed := seedPoolLifecycleIdle(t, owner, true, cause)
	waitPoolLifecycleSignal(t, seed.witness.entered, "background retirement did not start")
	seed.unblock()
	waitPoolLifecycleSignal(t, owner.maintained, "background cleanup failure did not stop maintenance")
	for range 3 {
		receipt, err := fixture.database.Ping(context.Background(), correlation("sealed-pool"))
		result := operationResult(t, receipt, err)
		drain(t, fixture.inbox, 1)
		if !errors.Is(result.Err(), ErrState) || !errors.Is(result.Err(), cause) || result.Attempts.Observed != 0 || peer.connects.Load() != 1 {
			t.Fatal("sealed source reopened or lost the original background error", result.Err())
		}
	}
	owner.mu.Lock()
	sealed, retained := owner.sealed, len(owner.closeErrors)
	owner.mu.Unlock()
	if !sealed || retained != 1 || seed.witness.count.Load() != 1 || fixture.database.Stats().TotalResources() != 0 {
		t.Fatal("background failure history or retirement ownership was duplicated")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := fixture.assembly.Close(ctx); !errors.Is(err, cause) || errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("source close lost completed background failure evidence", err)
	}
}
