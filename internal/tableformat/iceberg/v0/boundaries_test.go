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

package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestMalformedCommitRepliesRetainUnknownAppliedEffects(t *testing.T) {
	for _, mode := range []string{"empty", "non-json-503", "wrong-204", "lost", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			peer, options := newPeer(t)
			f := bindFixture(t, options, 12)
			setupTable(t, f)
			applied := make(chan struct{})
			release := make(chan struct{})
			peer.mu.Lock()
			peer.afterCommit = func(writer http.ResponseWriter, request *http.Request, body []byte) bool {
				close(applied)
				switch mode {
				case "empty":
					writer.Header().Set("Content-Length", "0")
					writer.WriteHeader(200)
				case "non-json-503":
					writer.WriteHeader(503)
					_, _ = writer.Write([]byte("<html>failed</html>"))
				case "wrong-204":
					writer.WriteHeader(204)
				case "lost":
					conn, _, err := writer.(http.Hijacker).Hijack()
					if err == nil {
						_ = conn.Close()
					}
				case "canceled":
					<-release
					_, _ = writer.Write(body)
				}
				return true
			}
			peer.mu.Unlock()
			work, cancel := context.WithCancelCause(deadline(t))
			defer cancel(nil)
			cancelCause := errors.New("caller-cancellation-cause")
			receipt, err := f.client.Write(work, correlation("uncertain"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{makeBatch(t, 0, 31, "applied")}})
			if mode == "canceled" {
				select {
				case <-applied:
				case <-deadline(t).Done():
					t.Fatal("commit not observed")
				}
				cancel(cancelCause)
				defer close(release)
			}
			result := settle(t, receipt, err)
			if mode == "canceled" && !errors.Is(result.Err(), cancelCause) {
				t.Fatal("caller cancellation cause lost")
			}
			if result.Err() == nil || result.Outcome.Value.Effect() != Unknown || !result.Outcome.Value.Staged() ||
				result.Outcome.Value.Reloaded() || result.Outcome.Value.CommittedSnapshotID() != 0 {
				t.Fatal("ambiguous applied mutation certified or erased")
			}
			if mode == "non-json-503" && !errors.Is(result.Err(), rest.ErrCommitStateUnknown) {
				t.Fatal("HTTP classification lost")
			}
			peer.mu.Lock()
			peer.afterCommit = nil
			commits := peer.commits
			peer.mu.Unlock()
			if commits != 1 {
				t.Fatalf("unexpected retry: commits=%d", commits)
			}
			inspectRows(t, readAll(t, f), expectedRows(0, 31, "applied"))
			found := false
			for f.inbox.Usage().Outstanding > 0 {
				delivery, err := f.inbox.Next(deadline(t))
				if err != nil {
					t.Fatal(err)
				}
				retained, err := delivery.Receipt().WaitReleased(deadline(t))
				if err != nil {
					t.Fatal(err)
				}
				if retained.Context.Correlation.Call == "uncertain" {
					found = true
					if retained.Err() == nil || retained.Outcome.Value.Effect() != Unknown || len(retained.Outcome.Value.FilesCopy()) == 0 {
						t.Fatal("handled error lost independent effect evidence")
					}
				}
				if err = delivery.Release(); err != nil {
					t.Fatal(err)
				}
			}
			if !found {
				t.Fatal("uncertain delivery absent")
			}
		})
	}
}

func TestConcurrentTransactionsPreserveAcceptedRowSets(t *testing.T) {
	for _, secondMode := range []WriteMode{Append, Overwrite, Delete} {
		t.Run(string(secondMode), func(t *testing.T) {
			peer, options := newPeer(t)
			f := bindFixture(t, options, 12)
			setupTable(t, f)
			appendBatch(t, f, 0, 20, "base")
			arrived := make(chan struct{}, 2)
			release := make(chan struct{})
			peer.mu.Lock()
			peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				if request.Method == "POST" && strings.HasSuffix(request.URL.Path, "/tables/data") {
					arrived <- struct{}{}
					<-release
				}
				return false
			}
			peer.mu.Unlock()
			first, err := f.client.Write(deadline(t), correlation("first"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{makeBatch(t, 100, 10, "append")}})
			if err != nil {
				t.Fatal(err)
			}
			secondRequest := BatchWrite{Mode: secondMode, Predicates: []Predicate{{Column: "id", Operator: "lt", Value: "10"}}}
			if secondMode != Delete {
				secondRequest.Batches = []arrow.RecordBatch{makeBatch(t, 0, 10, "second")}
			}
			if secondMode == Append {
				secondRequest.Predicates = nil
				secondRequest.Batches = []arrow.RecordBatch{makeBatch(t, 200, 10, "second")}
			}
			second, err := f.client.Write(deadline(t), correlation("second"), "data", secondRequest)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				select {
				case <-arrived:
				case <-deadline(t).Done():
					close(release)
					t.Fatal("concurrent staging did not reach commit")
				}
			}
			close(release)
			one, two := settle(t, first, nil), settle(t, second, nil)
			peer.mu.Lock()
			peer.hook = nil
			count := peer.commits
			peer.mu.Unlock()
			if (one.Err() == nil) == (two.Err() == nil) || count != 3 {
				t.Fatal("expected one accepted stale-base commit and one rejected conflict")
			}
			rejected := one
			if one.Err() == nil {
				rejected = two
			}
			if !errors.Is(rejected.Err(), table.ErrCommitFailed) || rejected.Outcome.Value.Effect() != Rejected {
				t.Fatal("conflict classified as unknown or retried")
			}
			expected := expectedRows(0, 20, "base")
			if one.Err() == nil {
				for id, value := range expectedRows(100, 10, "append") {
					expected[id] = value
				}
			} else {
				switch secondMode {
				case Append:
					for id, value := range expectedRows(200, 10, "second") {
						expected[id] = value
					}
				case Overwrite:
					for id, value := range expectedRows(0, 10, "second") {
						expected[id] = value
					}
				case Delete:
					for id := int64(0); id < 10; id++ {
						delete(expected, id)
					}
				}
			}
			inspectRows(t, readAll(t, f), expected)
		})
	}
}

func TestPartitionHistoryAndMetadataRefusals(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 8)
	setupTable(t, f)
	peer.mu.Lock()
	raw := bytes.Clone(peer.tables["data"])
	peer.mu.Unlock()
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	metadata["partition-specs"] = []any{
		map[string]any{"spec-id": 0, "fields": []any{map[string]any{"source-id": 1, "field-id": 1000, "name": "old", "transform": "identity"}}},
		map[string]any{"spec-id": 1, "fields": []any{}},
	}
	metadata["default-spec-id"] = 1
	metadata["last-partition-id"] = 1000
	encode := func() table.Metadata {
		data, err := json.Marshal(metadata)
		if err != nil {
			t.Fatal(err)
		}
		value, err := table.ParseMetadataBytes(data)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if err := validateMetadata(defaults(options), encode()); err != nil {
		t.Fatal("valid historical counter rejected", err)
	}
	metadata["last-partition-id"] = 999
	if err := validateMetadata(defaults(options), encode()); !errors.Is(err, ErrProtocol) {
		t.Fatal("historical ID reuse admitted")
	}
	for _, format := range []any{"invalid", 1, 3} {
		metadata["format-version"] = format
		body, _ := json.Marshal(map[string]any{"metadata": metadata, "metadata-location": options.Location + "data/metadata/current.json"})
		request, _ := http.NewRequest("POST", options.CatalogURI+"/v1/warehouse/namespaces/isolated/tables/data", nil)
		if validateResponse(defaults(options), request, 200, body) == nil {
			t.Fatal("unsupported or malformed format admitted")
		}
	}
}

func TestFileIOAbortAndBudgets(t *testing.T) {
	peer, options := newPeer(t)
	options.MaxFileOps = 2
	f := bindFixture(t, options, 4)
	state := newExchange(f.client.owner, nil)
	files := &fileIO{ctx: deadline(t), state: state}
	writer, err := files.Create(options.Location + "partial.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write([]byte("PAR1unfinished")); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	before := peer.requests
	peer.mu.Unlock()
	first := writer.Close()
	if !errors.Is(first, ErrProtocol) || !errors.Is(writer.Close(), ErrProtocol) {
		t.Fatal("unfinished parquet uploaded or close retried")
	}
	_, cleanup := state.errors()
	if !errors.Is(cleanup, ErrProtocol) {
		t.Fatal("ignored native abort failure hidden")
	}
	peer.mu.Lock()
	after := peer.requests
	peer.mu.Unlock()
	if before != after {
		t.Fatal("abort uploaded partial file")
	}
	if _, err = files.Open("s3://another/owned/data"); !errors.Is(err, ErrAuthority) {
		t.Fatal("bucket escaped")
	}
	if _, err = files.Open(options.Location + "../outside"); !errors.Is(err, ErrAuthority) {
		t.Fatal("subtree escaped")
	}
	if err = files.Remove(options.Location + "owned"); !errors.Is(err, ErrUnsupported) {
		t.Fatal("automatic cleanup allowed")
	}
	if _, err = files.begin(); err != nil {
		t.Fatal(err)
	}
	if _, err = files.begin(); err != nil {
		t.Fatal(err)
	}
	if _, err = files.begin(); !errors.Is(err, ErrLimit) {
		t.Fatal("file operation budget not enforced")
	}
}

func TestFileIOConcurrencyIsBoundedAtActualPeer(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 4)
	peer.mu.Lock()
	peer.objects["owned/data"] = []byte("fixture")
	peer.mu.Unlock()
	var mu sync.Mutex
	active, peak := 0, 0
	first := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "GET" && strings.HasPrefix(request.URL.Path, "/fixture/") {
			mu.Lock()
			active++
			peak = max(peak, active)
			mu.Unlock()
			once.Do(func() { close(first) })
			<-release
			mu.Lock()
			active--
			mu.Unlock()
		}
		return false
	}
	peer.mu.Unlock()
	state := newExchange(f.client.owner, nil)
	files := &fileIO{ctx: deadline(t), state: state}
	var group sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		group.Go(func() {
			file, err := files.Open(options.Location + "data")
			if err == nil {
				err = file.Close()
			}
			results <- err
		})
	}
	select {
	case <-first:
	case <-deadline(t).Done():
		close(release)
		t.Fatal("no file read")
	}
	close(release)
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	observed := peak
	mu.Unlock()
	if observed != 1 {
		t.Fatalf("actual concurrent reads=%d", observed)
	}
}
func TestPrivacyAndFacade(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 4)
	for _, value := range []any{options, f.client, Result{}, failure(ErrInput, "private", errors.New("test-secret"))} {
		conformance.Private(t, value, "test-access", "test-secret")
	}
	for _, value := range []any{(*OptionsV1)(nil), (*Client)(nil)} {
		if text := fmt.Sprintf("%v %+v %#v %s %q", value, value, value, value, value); strings.Contains(text, "PANIC") {
			t.Fatal("standard nil formatting panicked")
		}
		if slog.AnyValue(value).Resolve().String() != "iceberg[restricted]" {
			t.Fatal("nil logging did not redact")
		}
	}
	conformance.Facade(t, f.client, "CreateNamespace", "DropNamespace", "ListTables", "CreateTable", "InspectTable", "DropTable",
		"EvolveSchema", "EvolvePartitions", "RollbackSnapshot", "Write", "Read", "Compact", "Profile", "EvidenceBytes", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
}
func TestBorrowedSourceRetainsAuthoritativeResource(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 4)
	alias := resource.Borrow("alias", f.assembly, f.selected)
	borrowing, err := resource.Assemble(deadline(t), deadline(t), "borrow", alias)
	if err != nil {
		t.Fatal(err)
	}
	inbox, _ := invocation.NewInbox[Result](1, defaults(options).evidenceBytes())
	client, err := Bind(borrowing, alias, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("owner released live borrowing")
	}
	receipt, err := client.ListTables(deadline(t), correlation("borrowed"))
	requireOK(t, settle(t, receipt, err))
	delivery, err := inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = delivery.Release(); err != nil {
		t.Fatal(err)
	}
	if err = borrowing.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}
func TestBatchInputRefusals(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 4)
	if _, _, err := freezeBatches(defaults(options), nil); !errors.Is(err, ErrInput) {
		t.Fatal("nil batch accepted")
	}
	batch := makeBatch(t, 0, 10, "limit")
	s := defaults(options)
	s.MaxRows = 9
	if _, _, err := freezeBatches(s, []arrow.RecordBatch{batch}); !errors.Is(err, ErrLimit) {
		t.Fatal("row limit ignored")
	}
	s = defaults(options)
	s.MaxBatchBytes = 1
	if _, _, err := freezeBatches(s, []arrow.RecordBatch{batch}); !errors.Is(err, ErrLimit) {
		t.Fatal("byte limit ignored")
	}
	if _, err := f.client.Write(deadline(t), correlation("accidental-delete"), "data", BatchWrite{Mode: Delete}); !errors.Is(err, ErrInput) {
		t.Fatal("implicit all-row delete admitted")
	}
}
