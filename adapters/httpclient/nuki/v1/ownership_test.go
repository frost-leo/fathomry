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

package nuki

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type enteredReadConnection struct {
	net.Conn
	armed       atomic.Bool
	held        atomic.Bool
	entered     chan struct{}
	release     chan struct{}
	closed      chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (connection *enteredReadConnection) releaseRead() {
	connection.releaseOnce.Do(func() { close(connection.release) })
}
func (connection *enteredReadConnection) Read(data []byte) (int, error) {
	if connection.armed.Load() && connection.held.CompareAndSwap(false, true) {
		close(connection.entered)
		<-connection.release
	}
	return connection.Conn.Read(data)
}
func (connection *enteredReadConnection) Close() error {
	connection.once.Do(func() { close(connection.closed) })
	return connection.Conn.Close()
}

func TestCallbackReturnJoinsAlreadyEnteredReadBeforeGenerationRelease(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(peer.Close)
	connections := make(chan *enteredReadConnection, 1)
	native := testNative()
	native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		connection := &enteredReadConnection{Conn: raw, entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
		t.Cleanup(connection.releaseRead)
		connections <- connection
		return connection, nil
	}
	owner, dependencies := testOwner(t, testSettings("entered-read"), native)
	responseChannel := make(chan *Response, 1)
	readDone := make(chan error, 1)
	receipt, err := owner.Client().Consume(testContext(t), nativeRequest(t, "GET", peer.URL, nil), func(ctx context.Context, response *Response) error {
		var connection *enteredReadConnection
		select {
		case connection = <-connections:
		case <-ctx.Done():
			return ctx.Err()
		}
		connections <- connection
		connection.armed.Store(true)
		go func() { _, err := response.Read(make([]byte, 1)); readDone <- err }()
		select {
		case <-connection.entered:
		case <-ctx.Done():
			return ctx.Err()
		}
		responseChannel <- response
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var response *Response
	select {
	case response = <-responseChannel:
	case <-testContext(t).Done():
		t.Fatal("callback did not observe the entered read")
	}
	connection := <-connections
	unblock := connection.releaseRead
	defer unblock()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	until := testContext(t)
	for !response.revoked.Load() {
		select {
		case <-poll.C:
		case <-until.Done():
			t.Fatal("callback scope did not expire")
		}
	}
	if _, err := response.Read(make([]byte, 1)); !errors.Is(err, ErrState) {
		t.Fatal("new read entered expired callback", err)
	}
	snapshot, _ := receipt.Snapshot()
	if snapshot.Info().Released {
		t.Fatal("callback return released an actually entered read")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := owner.Close(canceled); !errors.Is(err, context.Canceled) || owner.ShutdownComplete() {
		t.Fatal("cleanup waiter released the active read", err)
	}
	status, _ := dependencies.Runtime.Inspect()
	if status.Active != 2 || status.WorkBytes == 0 {
		t.Fatal("actual read lost source/root reservation")
	}
	unblock()
	select {
	case <-readDone:
	case <-testContext(t).Done():
		t.Fatal("entered read did not terminate")
	}
	if _, err := receipt.WaitReleased(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("late cleanup did not finish", err)
	}
	select {
	case <-connection.closed:
	default:
		t.Fatal("physical socket close not observed")
	}
}
