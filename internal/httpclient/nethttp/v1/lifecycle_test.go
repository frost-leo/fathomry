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

package nethttp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestExpiredQueueNeverReachesNativeTransport(t *testing.T) {
	var requests atomic.Int64
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(writer, "body")
	})
	options.MaxActive, options.QueuedCalls = 1, 1
	f := bindFixture(t, options, 2)
	stream, first, err := f.client.Open(deadline(t), correlation("active"), newRequest(t, "GET", server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close(deadline(t))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	receipt, err := f.client.Do(ctx, deadline(t), correlation("expired"), newRequest(t, "GET", server.URL, nil))
	if receipt != nil || !errors.Is(err, context.DeadlineExceeded) || requests.Load() != 1 {
		t.Fatal("expired queue submitted native work", err)
	}
	status := f.assembly.Snapshot().Sources[0]
	if status.Usage.Active != 1 || status.Usage.Queued != 0 || f.inbox.Usage().Outstanding != 1 {
		t.Fatal("canceled queue leaked accounting")
	}
	if err := stream.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, f, first)
}

type uncertainCloseConn struct {
	net.Conn
	failure error
	calls   atomic.Int64
}

func (connection *uncertainCloseConn) Close() error {
	err := connection.Conn.Close()
	if connection.calls.Add(1) == 1 {
		return connection.failure
	}
	return err
}

func TestSocketCleanupCauseSurvivesConfirmedRelease(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "complete") })
	cause := errors.New("synthetic socket cleanup failure")
	options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &uncertainCloseConn{Conn: connection, failure: cause}, nil
	}
	f := bindFixture(t, options, 1)
	f.cleanupCause = cause
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("socket"), newRequest(t, "GET", server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	settle(t, f, receipt)
	if err := f.assembly.Close(deadline(t)); !errors.Is(err, cause) {
		t.Fatal("native cleanup error was discarded", err)
	}
	status := f.assembly.Snapshot().Sources[0]
	if !status.Released || !status.Quiescent || status.Pending || len(status.CleanupErrors) == 0 {
		t.Fatal("release or cleanup history is false")
	}
	if err := f.assembly.Close(deadline(t)); !errors.Is(err, cause) {
		t.Fatal("later close erased history", err)
	}
}

func TestInvalidSelectionPreflightDoesNotConstructOthers(t *testing.T) {
	var dials atomic.Int64
	options := OptionsV1{Name: "duplicate", Native: NativeOptionsV1{DialContext: func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("not reached")
	}}}
	first, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	assembly, err := resource.Assemble(deadline(t), deadline(t), "duplicate", first, second)
	if assembly != nil || err == nil || dials.Load() != 0 {
		t.Fatal("preflight performed native construction")
	}
}

type blockedDeadlineConn struct {
	net.Conn
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
	active  atomic.Bool
}

func (conn *blockedDeadlineConn) SetWriteDeadline(until time.Time) error {
	conn.once.Do(func() {
		conn.active.Store(true)
		close(conn.entered)
		<-conn.resume
		conn.active.Store(false)
	})
	return conn.Conn.SetWriteDeadline(until)
}

func TestAssemblyWaitsForNativeConnectionMethod(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	server, options := newPeer(t, true, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "complete") })
	options.Native.HTTP2 = &http.HTTP2Config{WriteByteTimeout: time.Second}
	options.Timeout = 150 * time.Millisecond
	var wrapped *blockedDeadlineConn
	options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		wrapped = &blockedDeadlineConn{Conn: raw, entered: entered, resume: resume}
		return wrapped, nil
	}
	f := bindFixture(t, options, 1)
	f.cleanupCause = context.DeadlineExceeded
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("native-deadline"), newRequest(t, "GET", server.URL, nil))
	if receipt == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("request control did not reach intended native boundary", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("native deadline callback not reached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closeErr := f.assembly.Close(ctx)
	status := f.assembly.Snapshot().Sources[0]
	if !wrapped.active.Load() || closeErr == nil || status.Released || status.Quiescent {
		t.Fatal("source released an active native connection method", closeErr)
	}
	unblock()
	settle(t, f, receipt)
	if err := f.assembly.Close(deadline(t)); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("source did not finish confirmed cleanup", err)
	}
	status = f.assembly.Snapshot().Sources[0]
	if wrapped.active.Load() || !status.Released || !status.Quiescent {
		t.Fatal("source cleanup did not join native use")
	}
}
