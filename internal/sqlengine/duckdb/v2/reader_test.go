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

package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	sdk "github.com/duckdb/duckdb-go/v2"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func startReader(t testing.TB, fixture *fixture, setup, lifetime context.Context, query string) (*Reader, *invocation.DeliveryRecord[Result]) {
	t.Helper()
	reader, receipt, err := fixture.database.Read(setup, lifetime, fixture.id(), Request{Mode: Query, SQL: query})
	if err != nil || reader == nil || receipt == nil {
		t.Fatal("read not admitted", err)
	}
	root, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal("terminal custody missing", err)
	}
	left, _ := root.Receipt().Result()
	right, _ := receipt.Result()
	if left.Context != right.Context {
		t.Fatal("terminal custody changed")
	}
	t.Cleanup(func() {
		if err := reader.Close(context.Background()); err != nil {
			t.Error(err)
		}
		if err := root.Release(); err != nil {
			t.Error(err)
		}
	})
	return reader, root
}

func nextReader(t testing.TB, fixture *fixture, reader *Reader) invocation.Result[Result] {
	t.Helper()
	receipt, err := reader.Next(deadline(t), fixture.id())
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	return result
}

func readerOK(t testing.TB, result invocation.Result[Result]) Progress {
	t.Helper()
	if result.Err() != nil || !result.Outcome.Present {
		t.Fatal("reader operation failed", result.Err())
	}
	return result.Outcome.Value.Snapshot()
}

func TestReaderLargeExactSingleExecution(t *testing.T) {
	fixture := openFixture(t, OptionsV1{Connections: 2, ReaderChunkRows: 257,
		Path: filepath.Join(t.TempDir(), "gh109-owned.duckdb")})
	fixture.exec(t, "CREATE SEQUENCE gh109_sequence START 1")
	setup, stop := context.WithCancel(deadline(t))
	reader, root := startReader(t, fixture, setup, deadline(t),
		"SELECT i, nextval('gh109_sequence')::BIGINT, (i::DECIMAL(38,9)/1)::DECIMAL(38,9), from_hex('0001ff') FROM range(70001) r(i) ORDER BY i")
	stop()
	if _, ready := reader.Receipt().Result(); ready {
		t.Fatal("setup prematurely finalized reader")
	}
	if err := root.Release(); !errors.Is(err, invocation.ErrPending) {
		t.Fatal("root released before cleanup")
	}
	var count int64
	var chunks uint64
	for {
		result := nextReader(t, fixture, reader)
		progress := readerOK(t, result)
		chunk := progress.Reader
		chunks++
		if chunk.Offset != count || chunk.Chunk != chunks || chunk.Rows != len(progress.Steps[0].Rows) || chunk.Rows > 257 ||
			chunk.Bytes > fixture.database.owner.config.ReaderChunkBytes {
			t.Fatal("incorrect delivery bounds/range")
		}
		for _, row := range progress.Steps[0].Rows {
			if row[0] != count || row[1] != count+1 {
				t.Fatal("order, count or execution changed", count)
			}
			decimal, ok := row[2].(sdk.Decimal)
			if !ok || decimal.Width != 38 || decimal.Scale != 9 || decimal.Value.Cmp(new(big.Int).Mul(big.NewInt(count), big.NewInt(1_000_000_000))) != 0 {
				t.Fatal("exact decimal changed", count)
			}
			if !reflect.DeepEqual(row[3], []byte{0, 1, 255}) {
				t.Fatal("blob changed")
			}
			count++
		}
		if len(progress.Steps[0].Rows) != 0 {
			progress.Steps[0].Rows[0][3].([]byte)[0] = 123
			if result.Outcome.Value.Snapshot().Steps[0].Rows[0][3].([]byte)[0] != 0 {
				t.Fatal("snapshot aliases evidence")
			}
		}
		state := reader.Snapshot()
		if len(state.Steps[0].Rows) != 0 || state.Reader.TotalRows != count || len(reader.pending) > MaxColumns {
			t.Fatal("reader retained history or lost aggregate progress")
		}
		if progress.Steps[0].Complete {
			break
		}
	}
	terminal := requireOK(t, settle(t, reader.Receipt(), nil))
	if count != 70001 || chunks != 273 || !terminal.ConnectionClosed || !terminal.Reader.Closed || len(terminal.Steps[0].Rows) != 0 {
		t.Fatal("large-result completion mismatch")
	}
	if err := root.Release(); err != nil {
		t.Fatal(err)
	}
	if got := fixture.rows(t, "SELECT last_value FROM duckdb_sequences() WHERE sequence_name='gh109_sequence'")[0][0]; got != int64(70001) {
		t.Fatal("reader re-executed SQL", got)
	}
	t.Logf("70001 exact rows; chunks=%d; maximum rows=257; no row history; native materialization remains", chunks)
}

func TestReaderEOFAndTotalBounds(t *testing.T) {
	for _, test := range []struct {
		name        string
		rows, total int64
		chunk       int
		complete    bool
	}{
		{"empty", 0, 4, 2, true}, {"chunk-boundary", 2, 4, 2, true},
		{"exact-total", 4, 4, 2, true}, {"over-total", 5, 4, 2, false},
		{"total-smaller-than-chunk", 5, 3, 8, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := openFixture(t, OptionsV1{ReaderChunkRows: test.chunk, ReaderTotalRows: test.total})
			reader, _ := startReader(t, fixture, deadline(t), deadline(t), fmt.Sprintf("SELECT i FROM range(%d) r(i) ORDER BY i", test.rows))
			var count int64
			for {
				result := nextReader(t, fixture, reader)
				progress := result.Outcome.Value.Snapshot()
				count += int64(len(progress.Steps[0].Rows))
				if progress.Reader.Closed {
					if progress.Steps[0].Complete != test.complete || progress.Steps[0].Limited == test.complete ||
						(!test.complete && !errors.Is(result.Err(), ErrLimit)) || test.complete && result.Err() != nil {
						t.Fatal("EOF/limited state conflated", result.Err())
					}
					break
				}
				if test.name == "chunk-boundary" && progress.Steps[0].Complete {
					t.Fatal("chunk bound invented EOF")
				}
			}
			if count != min(test.rows, test.total) || reader.values != nil || reader.pending != nil {
				t.Fatal("wrong prefix or native scalar storage retained")
			}
		})
	}
}

func TestReaderByteBoundsAndLookahead(t *testing.T) {
	for _, test := range []struct {
		name     string
		rows     int
		total    int64
		complete bool
	}{
		{"exact", 4, 1099, true}, {"limited", 5, 1099, false}, {"lookahead", 10, 10000, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Metadata costs 75 bytes; each 176-byte VARCHAR row costs 256.
			fixture := openFixture(t, OptionsV1{ReaderChunkRows: 99, ReaderChunkBytes: 1024, ReaderTotalBytes: test.total})
			reader, _ := startReader(t, fixture, deadline(t), deadline(t), fmt.Sprintf("SELECT repeat('x',176) AS data FROM range(%d)", test.rows))
			count := 0
			for {
				result := nextReader(t, fixture, reader)
				progress := result.Outcome.Value.Snapshot()
				count += len(progress.Steps[0].Rows)
				if progress.Reader.Bytes > 1024 || len(progress.Steps[0].Rows) > 3 {
					t.Fatal("byte bound exceeded")
				}
				if progress.Reader.Closed {
					if progress.Steps[0].Complete != test.complete || progress.Steps[0].Limited == test.complete {
						t.Fatal("byte-bound EOF mismatch", progress.Reader.TotalBytes, result.Err())
					}
					break
				}
				if len(reader.pending) != 1 || reader.pendingSize != 256 {
					t.Fatal("lookahead not bounded to one owned row")
				}
			}
			if count != min(test.rows, int((test.total-75)/256)) {
				t.Fatal("wrong byte-bound prefix", count)
			}
		})
	}
	fixture := openFixture(t, OptionsV1{ReaderChunkBytes: 1024})
	reader, _ := startReader(t, fixture, deadline(t), deadline(t), "SELECT repeat('x',2000)")
	result := nextReader(t, fixture, reader)
	if !errors.Is(result.Err(), ErrLimit) || len(result.Outcome.Value.Snapshot().Steps[0].Rows) != 0 || !reader.Snapshot().Reader.Closed {
		t.Fatal("oversized scalar was published or kept alive")
	}
}

func TestReaderSaturationCannotBlockTerminalCleanup(t *testing.T) {
	fixture := openFixture(t, OptionsV1{ReaderChunkRows: 1})
	config := fixture.database.owner.config
	inbox, err := invocation.NewInbox[Result](2, 2*config.readerEvidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	fixture.inbox = inbox
	fixture.database, err = Bind(fixture.assembly, fixture.selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader, root := startReader(t, fixture, deadline(t), deadline(t), "SELECT i FROM range(10) r(i)")
	receipt, err := reader.Next(deadline(t), fixture.id())
	readerOK(t, settle(t, receipt, err))
	child, err := inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	before := reader.Snapshot()
	if receipt, err := reader.Next(deadline(t), fixture.id()); receipt != nil || !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("full evidence permitted read advancement")
	}
	if !reflect.DeepEqual(before, reader.Snapshot()) {
		t.Fatal("rejected Next advanced")
	}
	if err := child.Release(); err != nil {
		t.Fatal(err)
	}
	receipt, err = reader.Next(deadline(t), fixture.id())
	resumed := readerOK(t, settle(t, receipt, err))
	if resumed.Steps[0].Rows[0][0] != int64(1) {
		t.Fatal("rejected Next consumed a hidden row")
	}
	child, err = inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	terminal := requireOK(t, settle(t, root.Receipt(), nil))
	if terminal.Steps[0].Complete || !terminal.ConnectionClosed || terminal.Reader.TotalRows != 2 || inbox.Usage().Outstanding != 2 {
		t.Fatal("saturation erased evidence or invented EOF")
	}
	if err := child.Release(); err != nil {
		t.Fatal(err)
	}
	if err := root.Release(); err != nil {
		t.Fatal(err)
	}
	if inbox.Usage().Outstanding != 0 {
		t.Fatal("evidence capacity leaked")
	}
}

func TestReaderAbandonmentAndOwnership(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	life, cancel := context.WithCancelCause(deadline(t))
	reader, root := startReader(t, fixture, deadline(t), life, "SELECT i FROM range(70001) r(i)")
	if receipt, err := fixture.database.Run(deadline(t), deadline(t), fixture.id(), Request{Mode: Query, SQL: "SELECT 42"}); receipt != nil || !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("reader did not retain original source reservation", err)
	}
	reader.mu.Lock()
	_, nextErr := reader.Next(deadline(t), fixture.id())
	closeErr := reader.Close(deadline(t))
	reader.mu.Unlock()
	if !errors.Is(nextErr, ErrState) || !errors.Is(closeErr, ErrState) {
		t.Fatal("concurrent use not refused")
	}
	short, stop := context.WithTimeout(context.Background(), time.Millisecond)
	defer stop()
	if err := fixture.assembly.Close(short); err == nil {
		t.Fatal("source released live reader")
	}
	cause := errors.New("gh109-reader-owner-canceled")
	cancel(cause)
	terminal := settle(t, reader.Receipt(), nil)
	if !errors.Is(terminal.Err(), cause) || !errors.Is(terminal.Err(), context.Canceled) || terminal.Outcome.Value.Snapshot().Steps[0].Complete {
		t.Fatal("abandonment invented completion or lost cause")
	}
	if err := root.Release(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Next(deadline(t), fixture.id()); !errors.Is(err, ErrState) {
		t.Fatal("closed reader usable")
	}
}

func TestReaderLifetimeReclaimsWithoutConsumer(t *testing.T) {
	fixture := openFixture(t, OptionsV1{ReaderLifetime: 20 * time.Millisecond})
	reader, _ := startReader(t, fixture, deadline(t), deadline(t), "SELECT 1")
	terminal := settle(t, reader.Receipt(), nil)
	if !errors.Is(terminal.Err(), context.DeadlineExceeded) || !terminal.Outcome.Value.Snapshot().Reader.Closed {
		t.Fatal("finite lifetime failed to reclaim abandonment")
	}
}

func TestReaderClosedCleanupIsIdempotentDuringWatcherExit(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	reader, _ := startReader(t, fixture, deadline(t), deadline(t), "SELECT 1")
	readerOK(t, nextReader(t, fixture, reader))
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for range 100 {
				if err := reader.Close(context.Background()); err != nil {
					t.Error("closed cleanup ceased being idempotent", err)
					return
				}
			}
		})
	}
	workers.Wait()
}

type cancelAfterRow struct {
	driver.Rows
	cancel func()
}

func (rows *cancelAfterRow) Next(values []driver.Value) error {
	err := rows.Rows.Next(values)
	rows.cancel()
	return err
}

func TestReaderAcceptedNextCancellationJoinsNativeCleanup(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	reader, _ := startReader(t, fixture, deadline(t), deadline(t), "SELECT i FROM range(10) r(i)")
	ctx, cancel := context.WithCancelCause(deadline(t))
	cause := errors.New("gh109-next-canceled")
	reader.rows = &cancelAfterRow{Rows: reader.rows, cancel: func() { cancel(cause) }}
	receipt, err := reader.Next(ctx, fixture.id())
	result := settle(t, receipt, err)
	fixture.drain(t, receipt)
	progress := result.Outcome.Value.Snapshot()
	if !errors.Is(result.Err(), cause) || !errors.Is(result.Err(), context.Canceled) || !progress.ConnectionClosed ||
		!progress.Reader.Closed || progress.Steps[0].Complete || len(progress.Steps[0].Rows) != 0 {
		t.Fatal("canceled accepted read fabricated delivery/EOF or lost cleanup")
	}
	if reader.conn != nil || reader.statement != nil || reader.rows != nil {
		t.Fatal("cancellation left native handles")
	}
}

func TestReaderOwningLifetimeCancelsNativeSetup(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	life, cancel := context.WithTimeout(deadline(t), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	reader, _ := startReader(t, fixture, deadline(t), life,
		"SELECT sum(a.i*b.i) FROM range(1000000) a(i), range(1000000) b(i)")
	result := settle(t, reader.Receipt(), nil)
	if !errors.Is(result.Err(), context.DeadlineExceeded) || !result.Outcome.Value.Snapshot().ConnectionClosed {
		t.Fatal("owning deadline failed to cancel and join native setup")
	}
	t.Logf("reader native setup cancellation and cleanup=%s (cooperative)", time.Since(started))
}

func TestReaderSafetyRefusalsAndRuntimePrivacy(t *testing.T) {
	fixture := openFixture(t, OptionsV1{})
	fixture.exec(t, "CREATE TABLE gh109_rows (id BIGINT)")
	for _, query := range []string{"SELECT 1; INSERT INTO gh109_rows VALUES(1)", "EXPLAIN SELECT 1", "INSERT INTO gh109_rows VALUES(1) RETURNING id", "SELECT [1,2]", "SELECT '9007199254740993'::JSON", "SELECT TIME '24:00:00'"} {
		reader, root := startReader(t, fixture, deadline(t), deadline(t), query)
		result := settle(t, reader.Receipt(), nil)
		if result.Err() == nil || !result.Outcome.Value.Snapshot().Reader.Closed {
			t.Fatal("unsafe reader route accepted")
		}
		if err := root.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if got := fixture.rows(t, "SELECT count(*) FROM gh109_rows")[0][0]; got != int64(0) {
		t.Fatal("rejected reader mutated data")
	}
	reader, _ := startReader(t, fixture, deadline(t), deadline(t), "SELECT CAST(TIME '24:00:00' AS VARCHAR) AS private_column_canary")
	conformance.Runtime(t, reader, new(Reader), "private_column_canary")
	progress := requireOK(t, nextReader(t, fixture, reader))
	if progress.Steps[0].Rows[0][0] != "24:00:00" {
		t.Fatal("explicit representation changed")
	}
}
