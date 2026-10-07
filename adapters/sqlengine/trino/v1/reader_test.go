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

package trino_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestReaderProvisionalPagesBackpressureAndTerminalEvidence(t *testing.T) {
	initial := make(chan struct{})
	var peer *wirePeer
	peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
		page := map[string]any{"id": "read"}
		switch {
		case request.Method == http.MethodPost:
			page["columns"] = []any{wireColumn("value", "bigint", "bigint")}
			page["data"] = [][]int{{1}}
			page["nextUri"] = peer.next("read", 1)
		case strings.HasSuffix(request.URL.Path, "/1"):
			page["nextUri"] = peer.next("read", 2)
		case request.Method == http.MethodGet:
			page["data"] = [][]int{{2}}
		default:
			t.Error("fully consumed reader unexpectedly canceled")
		}
		writePage(t, writer, page)
		if request.Method == http.MethodPost {
			close(initial)
		}
	})
	owner, inbox, runtime := openPublic(t, peer.settings(), 0)
	setup, stopSetup := context.WithCancel(boundedContext(t))
	reader, err := owner.Client().Read(setup, boundedContext(t), trino.Statement{SQL: "SELECT bounded_rows"})
	stopSetup()
	if err != nil || reader == nil {
		t.Fatal("reader admission", err)
	}
	select {
	case <-initial:
	case <-boundedContext(t).Done():
		t.Fatal("reader never submitted initial request")
	}
	if peer.gets.Load() != 0 {
		t.Fatal("absent consumer permitted continuation prefetch")
	}
	if state, _ := runtime.Inspect(); state.Active != 2 {
		t.Fatal("reader and source were not independently owned")
	}
	if snapshot, _ := reader.Receipt().Snapshot(); snapshot.Info().Released || snapshot.Info().Resolved {
		t.Fatal("unconsumed reader prematurely finalized")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.Next(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("page waiter lost its cancellation", err)
	}
	for index, expected := range []struct {
		data   string
		rows   int
		offset int
	}{{"[[1]]", 1, 0}, {"[]", 0, 1}, {"[[2]]", 1, 1}} {
		value, err := reader.Next(boundedContext(t))
		progress := value.ReadProgress()
		if err != nil || value.Complete() || !value.HasData() || string(value.DataCopy()) != expected.data || value.Rows() != expected.rows || progress.Sequence != uint64(index+1) || progress.Offset != expected.offset {
			t.Fatal("provisional page or exact position changed", index, err)
		}
		root, _ := reader.Receipt().Snapshot()
		if value.Attribution().Parent != root.Info().Sequence || value.Attribution().Depth != root.Info().Depth+1 || value.Source().Name != "fixture" {
			t.Fatal("page lacks owned root attribution")
		}
		acknowledge(t, inbox)
	}
	if _, err := reader.Next(boundedContext(t)); !errors.Is(err, io.EOF) {
		t.Fatal("successful reader exhaustion is not EOF", err)
	}
	terminal, err := reader.Result(boundedContext(t))
	if err != nil || !terminal.Complete() || !terminal.Terminal() || !terminal.Succeeded() || terminal.Rows() != 2 || terminal.Pages() != 3 || terminal.Submissions() != 1 || string(terminal.DataCopy()) != "[]" {
		t.Fatal("compact terminal summary lost total consumption", err)
	}
	if terminal.ReadProgress().Sequence != 0 || terminal.ReadProgress().Offset != 0 {
		t.Fatal("terminal summary was confused with a provisional chunk")
	}
	if _, err := reader.Next(boundedContext(t)); !errors.Is(err, io.EOF) {
		t.Fatal("released terminal receipt no longer supplies EOF", err)
	}
	if closed, err := reader.Close(boundedContext(t)); err != nil || !closed.Complete() || peer.posts.Load() != 1 || peer.gets.Load() != 2 || peer.deletes.Load() != 0 {
		t.Fatal("closing a completed reader changed effect or replayed", err)
	}
}

func TestReaderInputSnapshotAndSuccessfulEmptyOutput(t *testing.T) {
	peer := newWirePeer(t, func(writer http.ResponseWriter, _ *http.Request, _ []byte) {
		writePage(t, writer, map[string]any{"id": "empty", "columns": []any{wireColumn("value", "bigint", "bigint")}, "data": []any{}})
	})
	owner, inbox, _ := openPublic(t, peer.settings(), 0)
	binary := []byte{0, 255}
	nested := []any{trino.Numeric("12345678901234567890.100"), binary}
	arguments := []any{binary, nested}
	reader, err := owner.Client().Read(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT ?, ?", Args: arguments})
	if err != nil || reader == nil {
		t.Fatal(err)
	}
	binary[0] = 12
	nested[0] = trino.Numeric("9")
	arguments[0] = "changed"
	page, err := reader.Next(boundedContext(t))
	if err != nil || page.Complete() || page.Rows() != 0 || string(page.DataCopy()) != "[]" {
		t.Fatal("successful empty provisional page lost", err)
	}
	acknowledge(t, inbox)
	if _, err := reader.Next(boundedContext(t)); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	result, err := reader.Result(boundedContext(t))
	if err != nil || !result.Complete() || result.Rows() != 0 || result.Pages() != 1 || string(result.DataCopy()) != "[]" {
		t.Fatal("successful zero rows conflated with missing/failed result", err)
	}
	statements := peer.statements()
	if len(statements) != 1 || strings.Contains(statements[0], "changed") || !strings.Contains(statements[0], "X'00ff'") || !strings.Contains(statements[0], "12345678901234567890.100") {
		t.Fatal("asynchronous reader borrowed mutable input beyond return")
	}
}

func TestReaderFastTerminalRepeated(t *testing.T) {
	peer := newWirePeer(t, func(writer http.ResponseWriter, _ *http.Request, _ []byte) {
		writePage(t, writer, map[string]any{"id": "fast", "columns": []any{wireColumn("value", "bigint", "bigint")}, "data": []any{}})
	})
	owner, inbox, _ := openPublic(t, peer.settings(), 0)
	for range 40 {
		reader, err := owner.Client().Read(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT empty"})
		if err != nil || reader == nil {
			t.Fatal("fast completion lost reader ownership", err)
		}
		if _, err := reader.Next(boundedContext(t)); err != nil {
			t.Fatal("fast terminal page disappeared", err)
		}
		if _, err := reader.Next(boundedContext(t)); !errors.Is(err, io.EOF) {
			t.Fatal("terminal parent release raced EOF", err)
		}
		if result, err := reader.Result(boundedContext(t)); err != nil || !result.Complete() {
			t.Fatal("fast terminal summary lost", err)
		}
		for status, _ := inbox.Inspect(); status.Outstanding > 1; status, _ = inbox.Inspect() {
			acknowledge(t, inbox)
		}
	}
}

func TestReaderCloseTimeoutRetainsOwnershipAndReservedCleanup(t *testing.T) {
	deleteStarted, releaseDelete := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseDelete) })
	t.Cleanup(unblock)
	defer unblock()
	var peer *wirePeer
	peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
		if request.Method == http.MethodDelete {
			close(deleteStarted)
			<-releaseDelete
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		writePage(t, writer, map[string]any{"id": "closing", "columns": []any{wireColumn("value", "bigint", "bigint")}, "data": [][]int{{1}}, "nextUri": peer.next("closing", int(peer.gets.Load())+1)})
	})
	owner, inbox, runtime := openPublic(t, peer.settings(), 3)
	reader, err := owner.Client().Read(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT rows"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Next(boundedContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Next(boundedContext(t)); !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("reader page bypassed full public custody", err)
	}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = reader.Close(short)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked cleanup did not retain a timed-out close", err)
	}
	select {
	case <-deleteStarted:
	default:
		t.Fatal("saturated evidence prevented reserved cancellation")
	}
	if snapshot, _ := reader.Receipt().Snapshot(); snapshot.Info().Released {
		t.Fatal("timed-out cleanup released the reader")
	}
	if state, _ := runtime.Inspect(); state.Active != 2 {
		t.Fatal("timed-out cleanup dropped native source/work ownership")
	}
	unblock()
	value, err := reader.Close(boundedContext(t))
	if !errors.Is(err, context.Canceled) || value.Complete() || value.Rows() != 1 || !value.CancellationAttempted() || !value.CancellationAcknowledged() || peer.posts.Load() != 1 || peer.deletes.Load() != 1 {
		t.Fatal("continued close lost prefix/cancellation or replayed", err)
	}
	if snapshot, _ := reader.Receipt().Snapshot(); !snapshot.Info().Released {
		t.Fatal("continued close did not release the original receipt")
	}
	if _, nextErr := reader.Next(boundedContext(t)); !errors.Is(nextErr, context.Canceled) || errors.Is(nextErr, io.EOF) {
		t.Fatal("closed partial reader lost its terminal failure", nextErr)
	}
	if state, _ := inbox.Inspect(); state.Outstanding != 3 {
		t.Fatal("cleanup discarded existing terminal or page custody")
	}
}

func TestReaderLateFailureLimitsAndAbsentConsumer(t *testing.T) {
	for _, mode := range []string{"server-error", "metadata-error", "row-limit", "page-limit", "cleanup-failure", "absent"} {
		t.Run(mode, func(t *testing.T) {
			var peer *wirePeer
			peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
				if request.Method == http.MethodDelete {
					status := http.StatusNoContent
					if mode == "cleanup-failure" {
						status = http.StatusServiceUnavailable
					}
					writer.WriteHeader(status)
					return
				}
				page := map[string]any{"id": "failure", "columns": []any{wireColumn("value", "bigint", "bigint")}, "data": [][]int{{1}}}
				if request.Method == http.MethodPost {
					page["nextUri"] = peer.next("failure", 1)
				} else if mode == "server-error" {
					delete(page, "data")
					page["error"] = map[string]any{"errorCode": 99, "errorName": "TEST_FAILURE", "errorType": "EXTERNAL", "message": "private_canary"}
				} else if mode == "metadata-error" {
					page["columns"] = []any{wireColumn("value", "decimal(38,9)", "decimal", longArgument(5), longArgument(2))}
					page["data"] = [][]string{{"1.23"}}
				}
				writePage(t, writer, page)
			})
			options := peer.settings()
			if mode == "row-limit" {
				options.MaxReadRows = 1
			}
			if mode == "page-limit" || mode == "cleanup-failure" {
				options.MaxReadPages = 1
			}
			if mode == "absent" {
				options.ReadTimeout = 25 * time.Millisecond
			}
			owner, inbox, _ := openPublic(t, options, 0)
			reader, err := owner.Client().Read(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT result"})
			if err != nil || reader == nil {
				t.Fatal(err)
			}
			if mode != "absent" {
				page, err := reader.Next(boundedContext(t))
				if err != nil || page.Complete() || string(page.DataCopy()) != "[[1]]" {
					t.Fatal("valid provisional prefix lost", err)
				}
				acknowledge(t, inbox)
				if _, err := reader.Next(boundedContext(t)); err == nil || errors.Is(err, io.EOF) {
					t.Fatal("late failure/limit became successful exhaustion", err)
				}
			}
			value, err := reader.Result(boundedContext(t))
			if err == nil || value.Complete() || peer.posts.Load() != 1 || string(value.DataCopy()) != "[]" {
				t.Fatal("failed reader lost compact terminal evidence", err)
			}
			if mode == "absent" {
				if !errors.Is(err, context.DeadlineExceeded) || value.Rows() != 0 || peer.gets.Load() != 0 || peer.deletes.Load() != 1 {
					t.Fatal("absent consumer escaped lifetime/backpressure bounds", err)
				}
			} else if value.Rows() != 1 {
				t.Fatal("failed terminal summary lost actually transferred rows")
			}
			if mode == "cleanup-failure" {
				snapshot, _ := reader.Receipt().Snapshot()
				if snapshot.Primary() == nil || snapshot.Cleanup() == nil || !errors.Is(err, trino.ErrCleanup) || value.CancellationAcknowledged() {
					t.Fatal("late cleanup failure disappeared behind the primary bound", err)
				}
				if _, err := reader.Next(boundedContext(t)); !errors.Is(err, trino.ErrCleanup) || errors.Is(err, io.EOF) {
					t.Fatal("released terminal Next erased cleanup failure", err)
				}
			}
		})
	}
}

func TestOwnerCloseTimeoutRetainsAbandonedReader(t *testing.T) {
	getStarted, deleteStarted, releaseDelete := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseDelete) })
	t.Cleanup(unblock)
	defer unblock()
	var peer *wirePeer
	peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
		if request.Method == http.MethodDelete {
			close(deleteStarted)
			<-releaseDelete
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.Method == http.MethodGet {
			close(getStarted)
			<-request.Context().Done()
			return
		}
		writePage(t, writer, map[string]any{"id": "abandoned", "nextUri": peer.next("abandoned", 1)})
	})
	owner, inbox, runtime := openPublic(t, peer.settings(), 3)
	reader, err := owner.Client().Read(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT abandoned"})
	if err != nil || reader == nil {
		t.Fatal(err)
	}
	if _, err := reader.Next(boundedContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-getStarted:
	case <-boundedContext(t).Done():
		t.Fatal("abandoned query never reached bounded initial page")
	}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := owner.Close(short); !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
		t.Fatal("timed-out source shutdown dropped reader ownership", err)
	}
	select {
	case <-deleteStarted:
	default:
		t.Fatal("full custody prevented source-owned cancellation")
	}
	if state, _ := runtime.Inspect(); state.Active != 2 {
		t.Fatal("source timeout released active reservations")
	}
	if snapshot, _ := reader.Receipt().Snapshot(); snapshot.Info().Released {
		t.Fatal("blocked DELETE incorrectly published release")
	}
	if _, err := owner.Client().Query(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT new"}); err == nil || peer.posts.Load() != 1 {
		t.Fatal("closing owner admitted new business work")
	}
	unblock()
	if err := owner.Close(boundedContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("same owner failed to continue actual cleanup", err)
	}
	value, err := reader.Result(boundedContext(t))
	if !errors.Is(err, context.Canceled) || value.Complete() || value.Rows() != 0 || !value.CancellationAcknowledged() {
		t.Fatal("abandoned-reader terminal evidence lost", err)
	}
	if state, _ := inbox.Inspect(); state.Outstanding != 3 {
		t.Fatal("source shutdown erased abandoned result custody")
	}
}
