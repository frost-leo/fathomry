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

package trino

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestReadinessCleanupUsesAssemblyAuthority(t *testing.T) {
	for _, mode := range []string{"canceled-work", "live-work", "canceled-cleanup", "lost-delete-reply", "canceled-delete"} {
		t.Run(mode, func(t *testing.T) {
			getStarted := make(chan struct{})
			deleteStarted := make(chan struct{})
			var posts, gets, deletes atomic.Int64
			var base string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					posts.Add(1)
					reply(t, w, map[string]any{"id": "ready", "nextUri": base + "/v1/statement/executing/ready/slug/1"})
				case http.MethodGet:
					gets.Add(1)
					close(getStarted)
					if mode == "canceled-work" {
						<-r.Context().Done()
						return
					}
					w.WriteHeader(http.StatusServiceUnavailable)
				case http.MethodDelete:
					if deletes.Add(1) == 1 {
						close(deleteStarted)
					}
					if mode == "canceled-delete" {
						<-r.Context().Done()
						return
					}
					if mode == "lost-delete-reply" {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = conn.Close()
						return
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected method %q", r.Method)
				}
			}))
			defer server.Close()
			base = server.URL
			options := OptionsV1{Name: "readiness", Endpoint: base, User: "roundtable", Plaintext: true,
				Timeout: 2 * time.Second, CleanupTimeout: time.Second}
			selected, err := Select(options)
			if err != nil {
				t.Fatal(err)
			}
			selected = resource.WithLimits(selected, LimitsV1(options))
			work, cancelWork := context.WithCancelCause(deadline(t))
			defer cancelWork(nil)
			cleanup, cancelCleanup := context.WithCancel(deadline(t))
			defer cancelCleanup()
			if mode == "canceled-cleanup" {
				cancelCleanup()
			}
			type assembled struct {
				assembly *resource.Assembly
				err      error
			}
			done := make(chan assembled, 1)
			go func() {
				assembly, err := resource.Assemble(work, cleanup, "roundtable", selected)
				done <- assembled{assembly: assembly, err: err}
			}()
			select {
			case <-getStarted:
			case <-time.After(3 * time.Second):
				t.Fatal("readiness did not reach the paged GET")
			}
			cause := errors.New("synthetic-readiness-cancel")
			if mode == "canceled-work" {
				cancelWork(cause)
			}
			if mode == "canceled-delete" {
				select {
				case <-deleteStarted:
				case <-time.After(3 * time.Second):
					t.Fatal("readiness DELETE did not start")
				}
				cancelCleanup()
			}
			var result assembled
			select {
			case result = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("readiness did not end")
			}
			if result.err == nil || result.assembly == nil || posts.Load() != 1 || gets.Load() != 1 {
				t.Fatalf("missing primary failure or repeated work: err=%v posts=%d gets=%d", result.err, posts.Load(), gets.Load())
			}
			if mode == "canceled-work" && (!errors.Is(result.err, context.Canceled) || !errors.Is(result.err, cause)) {
				t.Error("readiness lost caller cancellation causality")
			}
			wantDeletes := int64(1)
			if mode == "canceled-cleanup" {
				wantDeletes = 0
			}
			beforeClose := deletes.Load()
			if beforeClose != wantDeletes {
				t.Errorf("readiness DELETE count before explicit close = %d, want %d; initialization and cleanup authority must remain separate", beforeClose, wantDeletes)
			}
			_ = result.assembly.Close(deadline(t))
			if mode == "canceled-cleanup" {
				if deletes.Load() != 1 {
					t.Error("fresh authorized cleanup did not attempt pending readiness cancellation exactly once")
				}
			} else if deletes.Load() != beforeClose {
				t.Error("repeated Close retried an already attempted readiness cancellation")
			}
			settledDeletes := deletes.Load()
			_ = result.assembly.Close(deadline(t))
			if deletes.Load() != settledDeletes {
				t.Error("second Close retried an already attempted readiness cancellation")
			}
			for _, source := range result.assembly.Snapshot().Sources {
				if !source.Quiescent || !source.Released {
					t.Error("readiness retained local resources after authorized cleanup")
				}
			}
			if (mode == "lost-delete-reply" || mode == "canceled-delete") && !errors.Is(result.err, ErrCleanup) {
				t.Error("failed readiness DELETE did not retain cleanup failure")
			}
		})
	}
}

func TestReadinessReleaseContinuesOnlyBeforeFirstDelete(t *testing.T) {
	var deletes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/v1/statement/queued/ready/slug/1" {
			t.Error("cleanup escaped the pending readiness query")
		}
		deletes.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	owner := &connection{settings: defaults(OptionsV1{Endpoint: server.URL, User: "roundtable", Plaintext: true}),
		readinessCancel: server.URL + "/v1/statement/queued/ready/slug/1"}
	cleanup, cancel := context.WithCancelCause(deadline(t))
	cause := errors.New("synthetic-readiness-cleanup-cancel")
	cancel(cause)
	first := owner.release(cleanup)
	if !first.Quiescent || first.Released || first.Continue == nil || deletes.Load() != 0 ||
		!errors.Is(first.Err, context.Canceled) || !errors.Is(first.Err, cause) {
		t.Fatal("unattempted readiness cleanup lost its cause or ownership continuation")
	}
	second := first.Continue(deadline(t))
	if second.Err != nil || !second.Quiescent || !second.Released || second.Continue != nil || deletes.Load() != 1 {
		t.Fatal("fresh authority did not settle the pending first DELETE")
	}
	last := owner.release(deadline(t))
	if last.Err != nil || !last.Quiescent || !last.Released || deletes.Load() != 1 {
		t.Fatal("settled readiness cancellation was retried")
	}
}

func TestContinuationStaysWithSubmittedQuery(t *testing.T) {
	for _, phase := range []string{"queued", "executing"} {
		for _, late := range []bool{false, true} {
			name := phase + "/initial"
			if late {
				name = phase + "/late"
			}
			t.Run(name, func(t *testing.T) {
				var base string
				var gets, deletes atomic.Int64
				var deletePath atomic.Value
				ownedPath := "/v1/statement/" + phase + "/owned/slug/1"
				otherPath := "/v1/statement/" + phase + "/other/slug/2"
				_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
					switch r.Method {
					case http.MethodPost:
						nextPath := otherPath
						if late {
							nextPath = ownedPath
						}
						reply(t, w, map[string]any{"id": "owned", "nextUri": base + nextPath})
					case http.MethodGet:
						gets.Add(1)
						if r.URL.Path == otherPath {
							reply(t, w, map[string]any{"id": "other"})
							return
						}
						reply(t, w, map[string]any{"id": "owned", "nextUri": base + otherPath})
					case http.MethodDelete:
						deletes.Add(1)
						deletePath.Store(r.URL.Path)
						w.WriteHeader(http.StatusNoContent)
					}
				})
				base = options.Endpoint
				fixture := bindFixture(t, options, 1)
				receipt, err := fixture.client.Query(deadline(t), deadline(t), correlation("ownership"), Statement{SQL: "SELECT 1"})
				result := settle(t, receipt, err)
				wantGets, wantDeletePath := int64(0), "/v1/query/owned"
				if late {
					wantGets, wantDeletePath = 1, ownedPath
				}
				if !errors.Is(result.Err(), ErrAuthority) || result.Outcome.Value.Complete() ||
					result.Outcome.Value.QueryID() != "owned" || gets.Load() != wantGets || deletes.Load() != 1 || deletePath.Load() != wantDeletePath {
					t.Fatalf("continuation escaped query ownership: gets=%d deletes=%d path=%v authority=%t", gets.Load(), deletes.Load(), deletePath.Load(), errors.Is(result.Err(), ErrAuthority))
				}
			})
		}
	}
}

func TestContinuationUsesSelectedProfilePaths(t *testing.T) {
	exchange := newExchange(defaults(OptionsV1{Endpoint: "https://example.invalid"}), false, true, nil)
	exchange.data.queryID = "owned"
	for _, path := range []string{"/v1/statement/queued/owned/slug/0", "/v1/statement/executing/owned/slug/1"} {
		if !exchange.allowedURI(exchange.settings.Endpoint + path) {
			t.Errorf("selected continuation path rejected: %q", path)
		}
	}
	for _, path := range []string{
		"/v1/statement/owned/1", "/v1/statement/queued/other/slug/1", "/v1/statement/other/owned/slug/1",
		"/v1/statement/queued/owned/slug/-1", "/v1/statement/queued/owned/slug/01",
		"/v1/statement/queued/owned/slug/9223372036854775808", "/v1/statement/queued/owned/slug/1/extra",
	} {
		if exchange.allowedURI(exchange.settings.Endpoint + path) {
			t.Errorf("unowned or unsupported continuation path accepted: %q", path)
		}
	}
}

type socketCloseFailure struct {
	net.Conn
	err    error
	closes atomic.Int64
}

func (conn *socketCloseFailure) Close() error {
	conn.closes.Add(1)
	return conn.err
}

func TestOwnedSocketRemovalIncludesCleanupEvidence(t *testing.T) {
	cause := errors.New("synthetic-socket-close")
	for range 256 {
		exchange := newExchange(defaults(OptionsV1{}), false, true, nil)
		conn := &socketCloseFailure{err: cause}
		socket := &ownedSocket{Conn: conn, owner: exchange}
		exchange.sockets[socket] = struct{}{}
		done := make(chan error, 2)
		for range 2 {
			go func() { done <- socket.Close() }()
		}
		for {
			exchange.mu.Lock()
			_, present := exchange.sockets[socket]
			cleanup := exchange.cleanup
			exchange.mu.Unlock()
			if !present {
				if !errors.Is(cleanup, cause) {
					t.Error("socket ownership ended before its cleanup evidence was recorded")
				}
				break
			}
			runtime.Gosched()
		}
		if !errors.Is(<-done, cause) || !errors.Is(<-done, cause) || conn.closes.Load() != 1 {
			t.Fatal("socket closure lost its native cause or ran twice")
		}
		exchange.closeLocal()
	}
}
