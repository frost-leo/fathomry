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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func retainSession(t *testing.T, fixture boundFixture, ctx context.Context, options SessionOptionsV1) (*Session, *invocation.Receipt[Result]) {
	t.Helper()
	session, receipt, err := fixture.db.Retain(ctx, correlation("session"), options)
	if err != nil || session == nil {
		if receipt != nil {
			result := observe(t, receipt, err)
			t.Fatal("session acquisition failed", result.Err())
		}
		t.Fatal("session admission failed", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = session.Close(ctx)
	})
	return session, receipt
}

func TestRetainedSessionKeepsConnectionAfterTransactionSQL(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newPeer(t, secure, false)
		fixture := bindFixture(t, peer.options(), 2)
		session, receipt := retainSession(t, fixture, context.Background(), SessionOptionsV1{})
		if value, ready := receipt.Result(); ready || value.Final || value.Released {
			t.Fatal("session acquisition fabricated final evidence")
		}
		record, err := fixture.inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{"START TRANSACTION", "COMMIT", "CREATE TABLE fixture (value int)", "SELECT cells"} {
			child, err := session.Query(context.Background(), correlation("child"), query)
			result := observe(t, child, err)
			if result.Err() != nil || !result.Nested || result.Context.Correlation.Parent != "session" {
				t.Fatal("retained SQL lost parent ownership", result.Err())
			}
			if fixture.db.Stats().InUse != 1 || fixture.db.Stats().Idle != 0 {
				t.Fatal("retained SQL returned its connection")
			}
			drain(t, fixture.inbox, 1)
		}
		final, err := session.Close(context.Background())
		result := observe(t, final, err)
		if result.Err() != nil || result.Outcome.Present || result.Outcome.Value.TransactionOutcome() != TransactionUnobserved ||
			fixture.db.Stats().OpenConnections != 0 {
			t.Fatal("session closure claimed a transaction or pooled the connection", result.Err())
		}
		if err := record.Release(); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Query(context.Background(), correlation("closed"), "SELECT cells"); !errors.Is(err, ErrState) {
			t.Fatal("closed session executed SQL")
		}
		if _, err := session.Close(context.Background()); err != nil {
			t.Fatal("repeated close lost ownership", err)
		}
		conformance.Facade(t, session, "Query", "Exec", "Close", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
		conformance.Runtime(t, session, new(Session), "credential-canary")
	}
}

func TestSessionFinalizationUsesReservedCapacity(t *testing.T) {
	peer := newPeer(t, false, false)
	fixture := bindFixture(t, peer.options(), 1)
	session, _ := retainSession(t, fixture, context.Background(), SessionOptionsV1{})
	if _, err := session.Query(context.Background(), correlation("full"), "SELECT cells"); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("full evidence admitted SQL", err)
	}
	if !errors.Is(fixture.assembly.Close(context.Background()), resource.ErrIncomplete) {
		t.Fatal("live session released its source")
	}
	receipt, err := session.Close(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("saturated close failed", result.Err())
	}
	drain(t, fixture.inbox, 1)
}

func TestSessionLifetimeAndNoReconnect(t *testing.T) {
	t.Run("canceled_call", func(t *testing.T) {
		peer := newPeer(t, false, false)
		fixture := bindFixture(t, peer.options(), 2)
		lifetime, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		cause := errors.New("session-lifetime-canary")
		session, receipt := retainSession(t, fixture, lifetime, SessionOptionsV1{})
		root, err := fixture.inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan invocation.Result[Result], 1)
		go func() {
			child, err := session.Query(context.Background(), correlation("wait"), "SELECT wait")
			if err != nil {
				done <- invocation.Result[Result]{Outcome: invocation.Outcome[Result]{Primary: err}}
				return
			}
			value, _ := child.WaitReleased(context.Background())
			done <- value
		}()
		select {
		case <-peer.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("query did not enter")
		}
		if _, err := session.Close(context.Background()); !errors.Is(err, ErrState) {
			t.Fatal("concurrent close was not refused", err)
		}
		cancel(cause)
		select {
		case value := <-done:
			if !errors.Is(value.Err(), cause) || !errors.Is(value.Err(), context.Canceled) {
				t.Fatal("session cancellation cause lost", value.Err())
			}
		case <-time.After(3 * time.Second):
			t.Fatal("canceled native work did not stop")
		}
		wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		value, err := receipt.WaitReleased(wait)
		if err != nil || !errors.Is(value.Err(), cause) {
			t.Fatal("session lifetime evidence lost", err, value.Err())
		}
		drain(t, fixture.inbox, 1)
		if err := root.Release(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("dead_connection", func(t *testing.T) {
		peer := newPeer(t, false, false)
		fixture := bindFixture(t, peer.options(), 2)
		session, _ := retainSession(t, fixture, context.Background(), SessionOptionsV1{})
		root, err := fixture.inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		child, err := session.Query(context.Background(), correlation("protocol"), "SELECT huge_header")
		if value := observe(t, child, err); value.Err() == nil {
			t.Fatal("oversized protocol unexpectedly completed")
		}
		drain(t, fixture.inbox, 1)
		before := peer.queries.Load()
		if _, err := session.Query(context.Background(), correlation("no-reconnect"), "SELECT cells"); !errors.Is(err, ErrState) {
			t.Fatal("dead session reconnected", err)
		}
		if peer.queries.Load() != before {
			t.Fatal("dead session reached another SQL operation")
		}
		final, err := session.Close(context.Background())
		if value := observe(t, final, err); value.Err() != nil {
			t.Fatal(value.Err())
		}
		if err := root.Release(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("bounded_lifetime", func(t *testing.T) {
		peer := newPeer(t, false, false)
		fixture := bindFixture(t, peer.options(), 1)
		_, receipt := retainSession(t, fixture, context.Background(), SessionOptionsV1{Timeout: 200 * time.Millisecond})
		wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		result, err := receipt.WaitReleased(wait)
		if err != nil || !errors.Is(result.Err(), context.DeadlineExceeded) || fixture.db.Stats().OpenConnections != 0 {
			t.Fatal("session deadline did not retire connection", err, result.Err())
		}
		drain(t, fixture.inbox, 1)
	})
}

type heldSessionClose struct {
	net.Conn
	entered chan struct{}
	allow   chan struct{}
	once    sync.Once
}

func (connection *heldSessionClose) Close() error {
	connection.once.Do(func() { close(connection.entered) })
	<-connection.allow
	return connection.Conn.Close()
}

func TestSessionCloseWaitCanContinue(t *testing.T) {
	peer := newPeer(t, false, false)
	fixture := bindFixture(t, peer.options(), 1)
	entered, allow := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(allow) }) })
	fixture.db.owner.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &heldSessionClose{Conn: connection, entered: entered, allow: allow}, nil
	}
	session, receipt := retainSession(t, fixture, context.Background(), SessionOptionsV1{})
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := session.Close(wait); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled wait claimed cleanup", err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup was not requested")
	}
	if result, _ := receipt.Result(); result.Released {
		t.Fatal("blocked close released ownership")
	}
	if !errors.Is(fixture.assembly.Close(context.Background()), resource.ErrIncomplete) {
		t.Fatal("blocked native close released the source")
	}
	release.Do(func() { close(allow) })
	joined, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	final, err := session.Close(joined)
	if result := observe(t, final, err); result.Err() != nil || !result.Released {
		t.Fatal("continued close did not join", result.Err())
	}
	drain(t, fixture.inbox, 1)
}

func TestSessionAdmissionBounds(t *testing.T) {
	peer := newPeer(t, false, false)
	fixture := bindFixture(t, peer.options(), 1)
	for _, timeout := range []time.Duration{-1, time.Nanosecond, time.Minute + 1} {
		session, receipt, err := fixture.db.Retain(context.Background(), correlation("invalid"), SessionOptionsV1{Timeout: timeout})
		if session != nil || receipt != nil || !errors.Is(err, ErrInput) {
			t.Fatal("invalid session lifetime admitted")
		}
	}
	if session, receipt, err := fixture.db.Retain(nil, correlation("nil"), SessionOptionsV1{}); session != nil || receipt != nil || !errors.Is(err, ErrInput) {
		t.Fatal("nil lifetime admitted")
	}
	if fixture.db.Stats().OpenConnections != 0 || fixture.inbox.Usage().Outstanding != 0 {
		t.Fatal("invalid options acquired ownership")
	}
}

func TestSessionLateCleanupErrorSurvivesCloseContinuation(t *testing.T) {
	peer := newPeer(t, false, false)
	cause := errors.New("retained-session-close-canary")
	fixture := bindFixture(t, peer.options(), 1, cause)
	entered, allow := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(allow) }) })
	var witness *closeWitness
	fixture.db.owner.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		witness = &closeWitness{Conn: connection, cause: cause}
		return &heldSessionClose{Conn: witness, entered: entered, allow: allow}, nil
	}
	session, receipt := retainSession(t, fixture, context.Background(), SessionOptionsV1{})
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := session.Close(wait); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled waiting context did not retain continuation", err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup was not requested")
	}
	if result, _ := receipt.Result(); result.Released || result.Final {
		t.Fatal("late cleanup evidence was finalized early")
	}
	release.Do(func() { close(allow) })
	joined, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	for range 2 {
		final, err := session.Close(joined)
		result := observe(t, final, err)
		if result.Outcome.Primary != nil || !errors.Is(result.Outcome.Cleanup, cause) || result.Outcome.Present {
			t.Fatal("repeated close lost independent cleanup evidence", result.Err())
		}
		conformance.Private(t, result.Err(), cause.Error())
	}
	if witness.closes.Load() != 1 {
		t.Fatal("session retirement repeated native cleanup")
	}
	drain(t, fixture.inbox, 1)
}
