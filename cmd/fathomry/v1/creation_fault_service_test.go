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

package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	createcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create"
	schemaop "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema"
)

type creationProxy struct {
	listener         net.Listener
	destination      string
	dialect, tag     string
	dropped          atomic.Int32
	mu               sync.Mutex
	sockets          map[net.Conn]struct{}
	workers          sync.WaitGroup
	afterCommitReset func()
}

func openFaultCreator(t *testing.T, target creationSettings, admin creationAdministration) *createcmd.Creator {
	t.Helper()
	owner, err := makeCreator(t, target, admin)
	if owner != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := owner.Close(ctx)
			if !owner.Released() || err != nil && !errors.Is(err, databasecmd.ErrCleanup) {
				t.Error("fault cleanup did not settle", err)
			}
			again := owner.Close(ctx)
			if (again == nil) != (err == nil) {
				t.Error("terminal cleanup history changed")
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func newCreationProxy(t *testing.T, dialect, address string, port uint16, tag string, afterCommitReset func()) *creationProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &creationProxy{listener: listener, destination: net.JoinHostPort(address, strconv.Itoa(int(port))), dialect: dialect, tag: tag, sockets: map[net.Conn]struct{}{}, afterCommitReset: afterCommitReset}
	proxy.workers.Go(func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			proxy.mu.Lock()
			proxy.sockets[client] = struct{}{}
			proxy.mu.Unlock()
			proxy.workers.Go(func() { proxy.forward(client) })
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		proxy.mu.Lock()
		for connection := range proxy.sockets {
			_ = connection.Close()
		}
		proxy.mu.Unlock()
		proxy.workers.Wait()
	})
	return proxy
}
func (proxy *creationProxy) port() uint16 { return uint16(proxy.listener.Addr().(*net.TCPAddr).Port) }
func (proxy *creationProxy) forward(client net.Conn) {
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	server, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxy.destination)
	cancel()
	if err != nil {
		return
	}
	proxy.mu.Lock()
	proxy.sockets[server] = struct{}{}
	proxy.mu.Unlock()
	defer func() {
		server.Close()
		proxy.mu.Lock()
		delete(proxy.sockets, client)
		delete(proxy.sockets, server)
		proxy.mu.Unlock()
	}()
	_ = client.SetDeadline(time.Now().Add(90 * time.Second))
	_ = server.SetDeadline(time.Now().Add(90 * time.Second))
	var creation atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		if proxy.dialect == "postgresql" {
			_, _ = io.Copy(server, client)
			return
		}
		for {
			frame, err := mysqlPacket(client)
			if err != nil {
				return
			}
			if len(frame) > 5 && frame[4] == 3 && bytes.Contains(frame[5:], []byte("CREATE DATABASE ")) {
				creation.Store(true)
			}
			if _, err = server.Write(frame); err != nil {
				return
			}
		}
	}()
	defer func() { client.Close(); server.Close(); <-done }()
	if proxy.dialect == "postgresql" {
		suppress := false
		committed := false
		for {
			header := make([]byte, 5)
			if _, err := io.ReadFull(server, header); err != nil {
				return
			}
			size := int(binary.BigEndian.Uint32(header[1:]))
			if size < 4 || size > 1<<20 {
				return
			}
			body := make([]byte, size-4)
			if _, err := io.ReadFull(server, body); err != nil {
				return
			}
			if header[0] == 'C' {
				if string(body) == "COMMIT\x00" {
					committed = true
				}
				if committed && string(body) == "DISCARD ALL\x00" && proxy.afterCommitReset != nil {
					proxy.afterCommitReset()
				}
			}
			if header[0] == 'C' && string(bytes.TrimSuffix(body, []byte{0})) == proxy.tag && proxy.dropped.Load() == 0 {
				suppress = true
			}
			if suppress {
				if header[0] == 'Z' {
					proxy.dropped.Add(1)
					return
				}
				continue
			}
			if _, err := client.Write(append(header, body...)); err != nil {
				return
			}
		}
	}
	for {
		frame, err := mysqlPacket(server)
		if err != nil {
			return
		}
		if len(frame) > 4 && frame[4] == 0 && creation.Load() && proxy.dropped.CompareAndSwap(0, 1) {
			return
		}
		if _, err = client.Write(frame); err != nil {
			return
		}
	}
}
func mysqlPacket(reader io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}
	size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	if size > 1<<20 {
		return nil, io.ErrUnexpectedEOF
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	return append(header, body...), nil
}
func TestCreationLostAcknowledgementService(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			target, admin, inspector := creationFixture(t, dialect)
			var proxy *creationProxy
			if dialect == "postgresql" {
				proxy = newCreationProxy(t, dialect, target.PostgreSQL.Address, target.PostgreSQL.Port, "CREATE DATABASE", nil)
				target.PostgreSQL.Port = proxy.port()
			} else {
				proxy = newCreationProxy(t, dialect, target.MySQL.Address, target.MySQL.Port, "", nil)
				target.MySQL.Port = proxy.port()
			}
			owner := openFaultCreator(t, target, admin)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			change, err := owner.Create(ctx)
			if err == nil || change.Effect != createcmd.DatabaseUnknown || change.Ready || proxy.dropped.Load() != 1 {
				t.Fatal("lost create acknowledgement misreported", change, err)
			}
			var count string
			if dialect == "postgresql" {
				rows, err := inspector.pg.Query(ctx, "SELECT count(*)::text FROM pg_catalog.pg_database WHERE datname=$1", target.PostgreSQL.Database)
				if err != nil {
					t.Fatal(err)
				}
				count = string(rows.RowsCopy()[0].ValuesCopy()[0])
			} else {
				rows, err := inspector.mysql.Query(ctx, "SELECT CAST(count(*) AS CHAR) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?", target.MySQL.Database)
				if err != nil {
					t.Fatal(err)
				}
				count = string(rows.RowsCopy()[0].ValuesCopy()[0])
			}
			if count != "1" {
				t.Fatal("proxy did not drop an actually completed creation")
			}
			repeated, err := owner.Create(ctx)
			if err == nil || repeated != change || proxy.dropped.Load() != 1 {
				t.Fatal("unknown creation was replayed or rewritten")
			}
		})
	}
}
func TestCreationSchemaAcknowledgementSurvivesCanceledWaitService(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	for _, schema := range []string{"control", "public"} {
		for _, cancelAtReset := range []bool{false, true} {
			name := "control"
			if cancelAtReset {
				name = "cancel_after_commit_ack"
			}
			t.Run(schema+"/"+name, func(t *testing.T) {
				target, admin, _ := creationFixture(t, "postgresql")
				target.Schema = schema
				direct := target.copy()
				entered, resume := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(resume) })
				pause := sync.OnceFunc(func() { close(entered); <-resume })
				proxy := newCreationProxy(t, "postgresql", target.PostgreSQL.Address, target.PostgreSQL.Port, "", pause)
				target.PostgreSQL.Port = proxy.port()
				owner := openFaultCreator(t, target, admin)
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				cause := errors.New("private-cancel-after-schema-commit")
				type observation struct {
					change createcmd.DatabaseChange
					err    error
				}
				done, stopped := make(chan observation, 1), make(chan struct{})
				go func() {
					defer close(stopped)
					change, err := owner.Create(ctx)
					done <- observation{change, err}
				}()
				t.Cleanup(func() {
					cancel(nil)
					unblock()
					select {
					case <-stopped:
					case <-time.After(10 * time.Second):
						t.Error("creation retained work after reset was released")
					}
				})
				select {
				case <-entered:
				case result := <-done:
					t.Fatal("creation returned before acknowledged transaction reset", result.change, result.err)
				case <-time.After(10 * time.Second):
					t.Fatal("target schema commit reset not reached")
				}
				if cancelAtReset {
					cancel(cause)
				}
				unblock()
				var result observation
				select {
				case result = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("creation did not settle")
				}
				if !owner.Released() || !result.change.CleanupComplete {
					t.Fatal("creation retained unresolved ownership")
				}
				if cancelAtReset && (!errors.Is(result.err, cause) || !errors.Is(result.err, context.Canceled)) || !cancelAtReset && result.err != nil {
					t.Fatal("cancellation evidence differs", result.err)
				}
				want := createcmd.DatabaseCreated
				if schema == "public" {
					want = createcmd.DatabaseUnchanged
				}
				if result.change.Effect != createcmd.DatabaseCreated || result.change.SchemaEffect != want || !result.change.Ready || result.change.Stage != "ready" {
					t.Error("original acknowledged schema COMMIT was discarded", result.change, result.err)
				}
				repeated, err := owner.Create(context.Background())
				if !errors.Is(err, databasecmd.ErrInput) || repeated != result.change {
					t.Error("creation replayed or lost its final report", err)
				}
				status, err := creationTargetRunner(t, direct).Status(context.Background())
				if err != nil || status.State != schemaop.NotInstalled {
					t.Fatal("independent target did not prove committed schema", err)
				}
			})
		}
	}
}

func TestSchemaCommitLossKeepsDatabaseCreationService(t *testing.T) {
	target, admin, _ := creationFixture(t, "postgresql")
	direct := target.copy()
	proxy := newCreationProxy(t, "postgresql", target.PostgreSQL.Address, target.PostgreSQL.Port, "COMMIT", nil)
	target.PostgreSQL.Port = proxy.port()
	owner := openFaultCreator(t, target, admin)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	change, err := owner.Create(ctx)
	if err == nil || change.Effect != createcmd.DatabaseCreated || change.SchemaEffect != createcmd.DatabaseUnknown || change.Ready || proxy.dropped.Load() != 1 {
		t.Fatal("schema commit loss erased database acknowledgement", change, err)
	}
	independent := creationTargetRunner(t, direct)
	status, err := independent.Status(ctx)
	if err != nil || status.State != schemaop.NotInstalled {
		t.Fatal("independent state did not prove target schema exists", status, err)
	}
}
