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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFailedConnectionRetainsCausesAndRevokesNativeCallbacks(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.Password = "wrong-credential-canary"
	fixture := bindFixture(t, options, 1)
	result := queryResult(t, fixture.database, "denied", "SELECT cells")
	var native *pgconn.ConnectError
	var server *pgconn.PgError
	if !errors.As(result.Err(), &native) || !errors.As(result.Err(), &server) || server.Code != "28P01" {
		t.Fatal("native startup error graph lost")
	}
	conformance.Private(t, result.Err(), "credential-canary", "authentication-canary")
	address := peer.listener.Addr().String()
	if socket, err := native.Config.DialFunc(context.Background(), "tcp", address); socket != nil || !errors.Is(err, ErrState) {
		if socket != nil {
			socket.Close()
		}
		t.Fatal("retained connection error reopened a retired acquisition scope")
	}
	if fixture.database.owner.native.Stat().TotalResources() != 0 {
		t.Fatal("failed construction left pooled ownership")
	}
	drain(t, fixture.inbox, 1)
}
func TestCanceledSynchronousConstructionLeavesNoNativePoolWork(t *testing.T) {
	peer := newProtocolPeer(t, false)
	peer.blockStartup.Store(true)
	fixture := bindFixture(t, peer.options(), 1)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("connect-cancel-canary")
	finished := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, err := fixture.database.Query(ctx, correlation("connect"), "SELECT cells")
		if err != nil {
			t.Error(err)
		}
		finished <- receipt
	}()
	select {
	case <-peer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("constructor not entered")
	}
	if fixture.database.owner.native.Stat().ConstructingResources() != 1 {
		t.Fatal("native constructor oracle absent")
	}
	cancel(cause)
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("construction did not join after cancellation")
	}
	result := operationResult(t, receipt, nil)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) || fixture.database.owner.native.Stat().TotalResources() != 0 {
		t.Fatal("canceled construction lost cause or left detached native pool construction")
	}
	drain(t, fixture.inbox, 1)
}
func TestIndependentSourcesAndTLSVerification(t *testing.T) {
	firstPeer, secondPeer := newProtocolPeer(t, true), newProtocolPeer(t, true)
	firstOptions, secondOptions := firstPeer.options(), secondPeer.options()
	firstOptions.Name, secondOptions.Name = "first", "second"
	first, second := bindFixture(t, firstOptions, 2), bindFixture(t, secondOptions, 2)
	if result := queryResult(t, first.database, "first", "SELECT cells"); result.Err() != nil {
		t.Fatal("verified TLS first source failed", result.Err())
	}
	if err := first.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result := queryResult(t, second.database, "second", "SELECT cells"); result.Err() != nil {
		t.Fatal("other source was stopped", result.Err())
	}
	drain(t, first.inbox, 1)
	drain(t, second.inbox, 1)
	wrong := secondPeer.options()
	wrong.Name = "wrong"
	wrong.RootCAPEM = firstPeer.roots
	untrusted := bindFixture(t, wrong, 1)
	if result := queryResult(t, untrusted.database, "untrusted", "SELECT cells"); result.Err() == nil {
		t.Fatal("untrusted server was accepted")
	}
	drain(t, untrusted.inbox, 1)
}

type closeWitness struct {
	net.Conn
	entered chan struct{}
	unblock chan struct{}
	cause   error
	count   atomic.Int32
}

func (connection *closeWitness) Close() error {
	connection.count.Add(1)
	if connection.entered != nil {
		close(connection.entered)
		<-connection.unblock
	}
	_ = connection.Conn.Close()
	return connection.cause
}
func TestNativeCleanupWaitContinuationAndOriginalFailure(t *testing.T) {
	peer := newProtocolPeer(t, false)
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
	defer assembly.Close(context.Background())
	inbox, _ := invocation.NewInbox[Result](1, defaults(options).evidenceReservation())
	database, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result := queryResult(t, database, "seed", "SELECT cells"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, inbox, 1)
	handle, err := database.owner.take(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	connection := handle.Value()
	cause := errors.New("native-close-canary")
	witness := &closeWitness{entered: make(chan struct{}), unblock: make(chan struct{}), cause: cause}
	defer func() {
		select {
		case <-witness.unblock:
		default:
			close(witness.unblock)
		}
	}()
	connection.wire.mu.Lock()
	for socket := range connection.wire.sockets {
		witness.Conn = socket.Conn
		socket.Conn = witness
	}
	connection.wire.mu.Unlock()
	if witness.Conn == nil {
		t.Fatal("independent socket witness absent")
	}
	handle.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = assembly.Close(ctx)
	if !errors.Is(err, resource.ErrIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unjoined native close became completion")
	}
	select {
	case <-witness.entered:
	default:
		t.Fatal("native close witness not entered")
	}
	status := assembly.Snapshot().Sources[0]
	if !status.Pending || !status.CanContinue || status.Quiescent || status.Released {
		t.Fatal("pending cleanup facts changed")
	}
	close(witness.unblock)
	err = assembly.Close(context.Background())
	if !errors.Is(err, cause) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, resource.ErrIncomplete) ||
		assembly.Snapshot().Sources[0].Pending || witness.count.Load() != 1 {
		t.Fatal("actual close lost original failure, history or idempotent ownership")
	}
	conformance.Private(t, err, "native-close-canary")
	if again := assembly.Close(context.Background()); !errors.Is(again, cause) {
		t.Fatal("later close erased native cleanup evidence")
	}
}
func TestPgxpoolDestructorCounterexample(t *testing.T) {
	peer := newProtocolPeer(t, false)
	config, err := pgxpool.ParseConfig(parserSeed)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig, err = nativeConfig(defaults(peer.options()))
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("native-close-canary")
	var witness *closeWitness
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		socket, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		witness = &closeWitness{Conn: socket, cause: cause}
		return witness, nil
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := pool.Acquire(context.Background())
	if err != nil {
		pool.Close()
		t.Fatal("native comparison connect failed")
	}
	handle.Release()
	legacyCleanup := func() error { pool.Close(); return nil }
	observed := legacyCleanup()
	if witness == nil || witness.count.Load() == 0 || observed != nil || errors.Is(observed, cause) {
		t.Fatal("pgxpool destructor no longer demonstrates the missing native close error")
	}
	t.Log("Native socket Close returned an injected cause; pgxpool.Close supplied no error channel. This is not a PostgreSQL service test.")
}

func TestFailedTLSConfigCannotMutateAnotherConnectionsTrust(t *testing.T) {
	unrelated, peer := newProtocolPeer(t, true), newProtocolPeer(t, true)
	options := peer.options()
	options.RootCAPEM = unrelated.roots
	config, err := nativeConfig(defaults(options))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = connect(ctx, config, options.RootCAPEM)
	var native *pgconn.ConnectError
	if !errors.As(err, &native) || native.Config.TLSConfig.RootCAs == config.TLSConfig.RootCAs {
		t.Fatal("native connection error shares mutable trust with the source")
	}
	if !native.Config.TLSConfig.RootCAs.AppendCertsFromPEM([]byte(peer.roots)) {
		t.Fatal("trust mutation control failed")
	}
	connection, err := connect(ctx, config, options.RootCAPEM)
	if connection != nil {
		_ = connection.close(context.Background())
	}
	if err == nil {
		t.Fatal("mutating a failed config changed later source trust")
	}
}
func TestPartialAssemblyRetainsOriginalFailureAndCleanupResponsibility(t *testing.T) {
	first, err := Select(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := resource.Prepare(resource.Schema[struct{}]{Format: 1},
		resource.Input{Identity: resource.Identity{Provider: "fixture.partial", Name: "later"}, Format: 1})
	if err != nil {
		t.Fatal(err)
	}
	primary, cleanup := errors.New("initialization-canary"), errors.New("cleanup-canary")
	later := resource.Select(prepared, func(context.Context, struct{}) (resource.Resource[struct{}], error) {
		return resource.Resource[struct{}]{Acquired: true, Release: func(context.Context) resource.ReleaseResult {
			return resource.ReleaseResult{Err: cleanup, Continue: func(context.Context) resource.ReleaseResult {
				return resource.ReleaseResult{Quiescent: true, Released: true}
			}}
		}}, primary
	})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "partial", first, later)
	if assembly == nil || !errors.Is(err, primary) || !errors.Is(err, cleanup) || !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("partial construction lost primary or cleanup causes")
	}
	if !assembly.Snapshot().Sources[0].Pending {
		t.Fatal("earlier PostgreSQL owner released before dependent cleanup")
	}
	err = assembly.Close(context.Background())
	if errors.Is(err, resource.ErrIncomplete) || !errors.Is(err, cleanup) || assembly.Snapshot().Sources[0].Pending {
		t.Fatal("explicit continuation erased errors or stranded PostgreSQL ownership")
	}
	conformance.Private(t, err, "initialization-canary", "cleanup-canary")
}
