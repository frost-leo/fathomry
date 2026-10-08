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

package surf

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/frost-leo/fathomry/adapters/v1"
)

type reviewResolverAnswer struct {
	receipt *adapters.Receipt[Result]
	err     error
}

func reviewBeginResolver(t *testing.T, owner *Owner, ctx context.Context) <-chan reviewResolverAnswer {
	t.Helper()
	finished := make(chan reviewResolverAnswer, 1)
	go func() {
		input, _ := http.NewRequest("GET", "https://review-resolver.invalid.:443/", nil)
		stream, receipt, err := owner.Client().Open(ctx, input)
		if stream != nil {
			err = errors.Join(err, stream.Close(context.Background()))
		}
		finished <- reviewResolverAnswer{receipt: receipt, err: err}
	}()
	return finished
}

func reviewCanceledResolver(t *testing.T, finished <-chan reviewResolverAnswer) reviewResolverAnswer {
	t.Helper()
	select {
	case answer := <-finished:
		if answer.receipt == nil || !errors.Is(answer.err, context.Canceled) {
			t.Fatal("DNS waiter cancellation not returned independently", answer.err)
		}
		return answer
	case <-testContext(t).Done():
		t.Fatal("DNS cancellation did not stop request waiter")
		return reviewResolverAnswer{}
	}
}

func reviewResolverStillOwned(t *testing.T, owner *Owner, receipt *adapters.Receipt[Result]) {
	t.Helper()
	wait, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := receipt.WaitReleased(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Error("live DNS work released public operation after canceled waiter", err)
	}
	closeWait, closeCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer closeCancel()
	if err := owner.Close(closeWait); err == nil || owner.ShutdownComplete() {
		t.Error("source declared shutdown before actual DNS callback/connection cleanup", err)
	}
}

func TestPublicResolverCancellationRetainsCallback(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1Only, PreferHTTP3} {
		for _, panics := range []bool{false, true} {
			name := string(mode) + "/late-error"
			if panics {
				name = string(mode) + "/late-panic"
			}
			t.Run(name, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				var once sync.Once
				cause := errors.New("late synthetic Resolver callback failure")
				resolver := &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
					once.Do(func() { close(entered) })
					<-release
					if panics {
						panic(cause)
					}
					return nil, cause
				}}
				prepared, err := Prepare(Settings{Name: "late-dns", Mode: pointer(mode)}, NativeOptions{Resolver: resolver})
				if err != nil {
					t.Fatal(err)
				}
				dependencies := testMechanisms(t, prepared)
				owner, err := prepared.Open(testContext(t), dependencies)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { unblock(); _ = owner.Close(testContext(t)) })
				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()
				finished := reviewBeginResolver(t, owner, ctx)
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("Resolver callback did not start")
				}
				cancel()
				answer := reviewCanceledResolver(t, finished)
				reviewResolverStillOwned(t, owner, answer.receipt)
				unblock()
				value, final := reviewPublicSettled(t, dependencies, answer.receipt)
				if !errors.Is(final, context.Canceled) || value.Complete() {
					t.Fatal("late DNS work changed canceled operation outcome", final)
				}
				closeErr := owner.Close(testContext(t))
				if !owner.ShutdownComplete() {
					t.Fatal("source failed to finish actual DNS callback cleanup", closeErr)
				}
				if panics {
					if !errors.Is(closeErr, cause) || !errors.Is(closeErr, sdk.ErrFathomryCallback) {
						t.Fatal("late Resolver panic absent from owner cleanup", closeErr)
					}
				} else if closeErr != nil {
					t.Fatal("ordinary DNS refusal poisoned source cleanup", closeErr)
				}
			})
		}
	}
}

type reviewHeldDNSConn struct {
	net.Conn
	reading      chan struct{}
	closing      chan struct{}
	releaseClose <-chan struct{}
	readOnce     sync.Once
	closeOnce    sync.Once
	closes       atomic.Int64
}

func (conn *reviewHeldDNSConn) Read(data []byte) (int, error) {
	conn.readOnce.Do(func() { close(conn.reading) })
	return conn.Conn.Read(data)
}

func (conn *reviewHeldDNSConn) Close() error {
	conn.closes.Add(1)
	err := conn.Conn.Close()
	conn.closeOnce.Do(func() { close(conn.closing) })
	<-conn.releaseClose
	return err
}

func TestPublicResolverCancellationRetainsConnectionClose(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1Only, PreferHTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			created := make(chan *reviewHeldDNSConn, 8)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			var mu sync.Mutex
			var connections []*reviewHeldDNSConn
			var pending sync.WaitGroup
			resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				client, server := net.Pipe()
				conn := &reviewHeldDNSConn{Conn: client, reading: make(chan struct{}), closing: make(chan struct{}), releaseClose: release}
				mu.Lock()
				connections = append(connections, conn)
				mu.Unlock()
				pending.Go(func() { defer server.Close(); _, _ = io.Copy(io.Discard, server) })
				created <- conn
				return conn, nil
			}}
			prepared, err := Prepare(Settings{Name: "late-dns-conn", Mode: pointer(mode)}, NativeOptions{Resolver: resolver})
			if err != nil {
				t.Fatal(err)
			}
			dependencies := testMechanisms(t, prepared)
			owner, err := prepared.Open(testContext(t), dependencies)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				unblock()
				mu.Lock()
				for _, conn := range connections {
					_ = conn.Conn.Close()
				}
				mu.Unlock()
				_ = owner.Close(testContext(t))
				pending.Wait()
			})
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			finished := reviewBeginResolver(t, owner, ctx)
			var held *reviewHeldDNSConn
			select {
			case held = <-created:
			case <-ctx.Done():
				t.Fatal("Resolver connection not created")
			}
			select {
			case <-held.reading:
			case <-ctx.Done():
				t.Fatal("native resolver did not enter DNS response read")
			}
			cancel()
			answer := reviewCanceledResolver(t, finished)
			reviewResolverStillOwned(t, owner, answer.receipt)
			select {
			case <-held.closing:
			case <-time.After(time.Second):
				t.Error("operation cancellation did not start DNS connection cleanup")
			}
			unblock()
			value, final := reviewPublicSettled(t, dependencies, answer.receipt)
			if !errors.Is(final, context.Canceled) || value.Complete() {
				t.Fatal("DNS connection cleanup changed operation cancellation", final)
			}
			if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
				t.Fatal("DNS connection cleanup did not finish", err)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, conn := range connections {
				if conn.closes.Load() != 1 {
					t.Error("transferred DNS connection did not close exactly once", conn.closes.Load())
				}
			}
		})
	}
}
