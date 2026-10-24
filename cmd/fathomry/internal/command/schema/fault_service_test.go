//go:build schema_service

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

package schema

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	"github.com/frost-leo/fathomry/failure/v1"
)

type replyProxy struct {
	listener    net.Listener
	upstream    string
	dialect     string
	match       func([]byte) bool
	commit      bool
	dropped     atomic.Bool
	witness     chan struct{}
	mu          sync.Mutex
	connections map[net.Conn]bool
	workers     sync.WaitGroup
	accepted    chan struct{}
}

func newReplyProxy(t *testing.T, dialect, upstream string, match func([]byte) bool, commit bool) *replyProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	value := &replyProxy{listener: listener, upstream: upstream, dialect: dialect, match: match, commit: commit, witness: make(chan struct{}), connections: map[net.Conn]bool{}, accepted: make(chan struct{})}
	go value.accept()
	t.Cleanup(func() {
		_ = value.listener.Close()
		<-value.accepted
		value.mu.Lock()
		for connection := range value.connections {
			_ = connection.Close()
		}
		value.mu.Unlock()
		done := make(chan struct{})
		go func() { value.workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("fault proxy ownership did not end")
		}
	})
	return value
}
func (proxy *replyProxy) port() uint16 {
	_, port, _ := net.SplitHostPort(proxy.listener.Addr().String())
	value, _ := strconv.ParseUint(port, 10, 16)
	return uint16(value)
}
func (proxy *replyProxy) accept() {
	defer close(proxy.accepted)
	for {
		client, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.DialTimeout("tcp", proxy.upstream, 5*time.Second)
		if err != nil {
			_ = client.Close()
			continue
		}
		proxy.mu.Lock()
		proxy.connections[client] = true
		proxy.connections[server] = true
		proxy.mu.Unlock()
		proxy.workers.Add(1)
		go func() {
			defer proxy.workers.Done()
			defer func() {
				proxy.mu.Lock()
				delete(proxy.connections, client)
				delete(proxy.connections, server)
				proxy.mu.Unlock()
			}()
			var armed atomic.Bool
			var workers sync.WaitGroup
			workers.Add(2)
			for _, direction := range []bool{true, false} {
				go func(outbound bool) {
					defer workers.Done()
					defer client.Close()
					defer server.Close()
					source, destination := net.Conn(server), net.Conn(client)
					if outbound {
						source, destination = client, server
					}
					startup := outbound && proxy.dialect == "postgresql"
					for {
						packet, payload, tag, err := readPacket(source, proxy.dialect, startup)
						startup = false
						if err != nil {
							return
						}
						if outbound && proxy.match != nil && proxy.match(payload) {
							armed.Store(true)
						}
						drop := false
						if !outbound {
							if proxy.dialect == "postgresql" {
								drop = tag == 'C' && (proxy.commit && bytes.Equal(payload, []byte("COMMIT\x00")) || armed.Load())
							} else {
								drop = len(payload) > 0 && payload[0] == 0 && armed.Load()
							}
						}
						if drop && proxy.dropped.CompareAndSwap(false, true) {
							close(proxy.witness)
							return
						}
						if _, err = io.Copy(destination, bytes.NewReader(packet)); err != nil {
							return
						}
					}
				}(direction)
			}
			workers.Wait()
		}()
	}
}
func readPacket(connection net.Conn, dialect string, startup bool) (packet, payload []byte, tag byte, err error) {
	if dialect == "mysql" {
		header := make([]byte, 4)
		if _, err = io.ReadFull(connection, header); err != nil {
			return
		}
		size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		if size > 8<<20 {
			err = errors.New("oversized fixture packet")
			return
		}
		payload = make([]byte, size)
		if _, err = io.ReadFull(connection, payload); err != nil {
			return
		}
		packet = append(header, payload...)
		return
	}
	size := 4
	if !startup {
		size = 5
	}
	header := make([]byte, size)
	if _, err = io.ReadFull(connection, header); err != nil {
		return
	}
	length := int(binary.BigEndian.Uint32(header[size-4:])) - 4
	if length < 0 || length > 8<<20 {
		err = errors.New("invalid fixture frame")
		return
	}
	if !startup {
		tag = header[0]
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(connection, payload); err != nil {
		return
	}
	packet = append(header, payload...)
	return
}
func proxiedRunner(t *testing.T, value *service, match func([]byte) bool, commit bool, expectedCleanup ...failure.Code) (*Runner, *replyProxy) {
	t.Helper()
	address, port := value.pgSettings.Address, value.pgSettings.Port
	if value.dialect == "mysql" {
		address, port = value.mySettings.Address, value.mySettings.Port
	}
	proxy := newReplyProxy(t, value.dialect, net.JoinHostPort(address, strconv.Itoa(int(port))), match, commit)
	var runner *Runner
	var err error
	if value.dialect == "postgresql" {
		settings := value.pgSettings
		settings.Port = proxy.port()
		runner, err = PostgreSQL(postgresClient(t, settings), value.target, databasecmd.Options{})
	} else {
		settings := value.mySettings
		settings.Port = proxy.port()
		runner, err = MySQL(mysqlClient(t, settings), value.target, databasecmd.Options{})
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := runner.Close(ctx); err != nil && (len(expectedCleanup) != 1 || !errors.Is(err, expectedCleanup[0])) {
			t.Error("fault runner cleanup failed", err)
		}
		if !runner.Released() {
			t.Error("fault runner retained work")
		}
	})
	return runner, proxy
}
func eventuallyState(t *testing.T, runner *Runner, want State) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, err := runner.Status(context.Background())
		if err == nil && status.State == want {
			return
		}
		if err != nil || status.State != Changing || time.Now().After(deadline) {
			t.Fatal("read-back did not reconcile", status, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestSchemaMySQLLostReplyAtEveryAsset(t *testing.T) {
	contract, err := load("mysql")
	if err != nil {
		t.Fatal(err)
	}
	for index, entry := range append([]asset{contract.manifest.Bootstrap}, contract.manifest.Migrations[0].Steps...) {
		t.Run(entry.ID, func(t *testing.T) {
			value := fixture(t, "mysql")
			sql := contract.bootstrap
			if index > 0 {
				sql = contract.sql[index-1]
			}
			sql = strings.ReplaceAll(sql, "{{target}}", value.quoted())
			runner, proxy := proxiedRunner(t, value, func(payload []byte) bool { return len(payload) > 1 && payload[0] == 3 && string(payload[1:]) == sql }, false)
			change, err := runner.Apply(context.Background())
			if err == nil || change.Effect != Unknown || change.After.State == Current || !change.CleanupComplete {
				t.Fatal("lost DDL/seed reply fabricated success or rollback", change, err)
			}
			select {
			case <-proxy.witness:
			default:
				t.Fatal("fault did not drop an actual server acknowledgement")
			}
			eventuallyState(t, runner, Incomplete)
			repeat, err := runner.Apply(context.Background())
			if !errors.Is(err, databasecmd.ErrSchema) || repeat.Effect != NotStarted || repeat.Before.State != Incomplete {
				t.Fatal("uncertain MySQL installation resumed", repeat, err)
			}
		})
	}
}
func TestSchemaLostTerminalReplyIsNotAttemptAcknowledgement(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			value := fixture(t, dialect)
			match := func(payload []byte) bool {
				return len(payload) > 1 && payload[0] == 0x17 && bytes.Contains(payload[1:], []byte("complete"))
			}
			runner, proxy := proxiedRunner(t, value, match, dialect == "postgresql")
			change, err := runner.Apply(context.Background())
			if err == nil || change.Effect != Unknown || change.After.State == Current {
				t.Fatal("lost terminal reply claimed original acknowledgement", change, err)
			}
			select {
			case <-proxy.witness:
			default:
				t.Fatal("terminal acknowledgement was not intercepted")
			}
			if !change.CleanupComplete {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := runner.Close(ctx); err != nil {
					t.Fatal(err)
				}
				runner = value.runner(t)
			}
			eventuallyState(t, runner, Current)
			repeat, err := runner.Apply(context.Background())
			if err != nil || repeat.Effect != Unchanged {
				t.Fatal("current read-back did not preserve no-op semantics", repeat, err)
			}
		})
	}
}
func TestSchemaPostgresLostDDLReplyDoesNotClaimRollback(t *testing.T) {
	value := fixture(t, "postgresql")
	runner, proxy := proxiedRunner(t, value, func(payload []byte) bool {
		return bytes.Contains(payload, []byte("CREATE TABLE "+value.quoted()+".workflow_definition ("))
	}, false)
	change, err := runner.Apply(context.Background())
	if err == nil || change.Effect != Unknown || change.After.State == Current {
		t.Fatal("lost reply claimed rollback without acknowledgement", change, err)
	}
	select {
	case <-proxy.witness:
	default:
		t.Fatal("DDL acknowledgement was not intercepted")
	}
	if !change.CleanupComplete {
		t.Fatal("local failed transaction not joined")
	}
	eventuallyState(t, runner, NotInstalled)
}

func TestSchemaCommitAcknowledgementSurvivesCanceledWait(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	for _, cancelAtReset := range []bool{false, true} {
		name := "control"
		if cancelAtReset {
			name = "cancel_after_commit_ack"
		}
		t.Run(name, func(t *testing.T) {
			value := fixture(t, "postgresql")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			var once sync.Once
			runner, _ := proxiedRunner(t, value, func(payload []byte) bool {
				if bytes.Contains(payload, []byte("DISCARD ALL")) {
					once.Do(func() {
						close(entered)
						<-release
					})
				}
				return false
			}, false)
			type observation struct {
				change Change
				err    error
			}
			done, stopped := make(chan observation, 1), make(chan struct{})
			go func() {
				defer close(stopped)
				change, err := runner.Apply(ctx)
				done <- observation{change, err}
			}()
			t.Cleanup(func() {
				cancel()
				unblock()
				select {
				case <-stopped:
				case <-time.After(10 * time.Second):
					t.Error("Apply retained work after reset was released")
				}
			})
			select {
			case <-entered:
			case result := <-done:
				t.Fatal("Apply returned before retained reset", result.change.Effect, result.err)
			case <-time.After(10 * time.Second):
				t.Fatal("retained reset was not observed")
			}
			// Reset follows native COMMIT acknowledgement but precedes receipt transfer.
			receipt := runner.state.pending.pg.Receipt()
			if cancelAtReset {
				cancel()
			}
			unblock()
			var result observation
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Apply did not join reset")
			}
			final, finished := receipt.Snapshot()
			native, present := final.ValueCopy()
			if !finished || !present || native.TransactionOutcome() != postgres.CommitAcknowledged || !result.change.CleanupComplete {
				t.Fatal("original transaction finalization was not joined")
			}
			if result.change.Effect != Applied || result.change.After.State != Current {
				t.Error("COMMIT acknowledgement learned during cleanup was discarded", result.change, result.err)
			}
			if cancelAtReset && !errors.Is(result.err, context.Canceled) || !cancelAtReset && result.err != nil {
				t.Error("caller cancellation evidence differs", result.err)
			}
			eventuallyState(t, value.runner(t), Current)
		})
	}
}

func TestSchemaCommitAcknowledgementSurvivesCleanupFailure(t *testing.T) {
	value := fixture(t, "postgresql")
	runner, proxy := proxiedRunner(t, value, func(payload []byte) bool { return bytes.Contains(payload, []byte("DISCARD ALL")) }, false, databasecmd.ErrCleanup)
	change, err := runner.Apply(context.Background())
	if err == nil || change.Effect != Applied || change.After.State != Current || !change.CleanupComplete {
		t.Fatal("acknowledged commit was overwritten by cleanup failure", change, err)
	}
	select {
	case <-proxy.witness:
	default:
		t.Fatal("cleanup reply was not intercepted")
	}
	for range 2 {
		if err := runner.Close(context.Background()); !errors.Is(err, databasecmd.ErrCleanup) {
			t.Fatal("cleanup history erased", err)
		}
	}
	independent := value.runner(t)
	eventuallyState(t, independent, Current)
}
