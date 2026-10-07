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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestReaderCapacity70000SingleExecution(t *testing.T) {
	selected := testSettings()
	selected.ReaderChunkRows = 257
	selected.ReaderChunkBytes = 64 << 10
	selected.ReaderTotalRows = 70000
	selected.ReaderTotalBytes = 16 << 20
	owner, inbox, runtime := testOwner(t, selected, 0)
	execute(t, owner, inbox, "CREATE SEQUENCE reader_once")
	setup, cancel := context.WithCancel(context.Background())
	reader, err := owner.Client().Read(setup, "SELECT range, nextval('reader_once') FROM range(70000) ORDER BY range")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if state, _ := reader.Receipt().Snapshot(); state.Info().Resolved || state.Info().Released {
		t.Fatal("reader setup invented terminal evidence")
	}
	total, chunks := int64(0), uint64(0)
	for {
		value, err := reader.Next(context.Background())
		if err != nil {
			t.Fatal("capacity consumption", err)
		}
		progress := value.Snapshot()
		if progress.Reader == nil || len(progress.Steps) != 1 || len(progress.Steps[0].Rows) > selected.ReaderChunkRows || progress.Reader.Bytes > selected.ReaderChunkBytes || progress.Reader.Offset != total || progress.Reader.Chunk != chunks+1 {
			t.Fatal("chunk bounds, sequence, or range changed")
		}
		for _, row := range progress.Steps[0].Rows {
			if row[0] != total || row[1] != total+1 {
				t.Fatal("order changed or SQL was re-executed")
			}
			total++
		}
		chunks++
		if progress.Reader.TotalRows != total || progress.Reader.Rows != len(progress.Steps[0].Rows) {
			t.Fatal("chunk aggregate counters changed")
		}
		progress.Reader.TotalRows = -1
		if value.Snapshot().Reader.TotalRows != total {
			t.Fatal("reader counters alias caller storage")
		}
		ack(t, inbox)
		if progress.Steps[0].Complete {
			break
		}
		if status, _ := inbox.Inspect(); status.Outstanding != 2 {
			t.Fatal("incremental acknowledgement retained row history evidence")
		}
	}
	terminal, err := reader.Result(context.Background())
	if err != nil || total != 70000 || chunks != 273 || !terminal.Snapshot().Reader.Closed || !terminal.Snapshot().ConnectionClosed || !terminal.Snapshot().Steps[0].Complete || len(terminal.Snapshot().Steps[0].Rows) != 0 || terminal.Snapshot().Reader.TotalRows != total {
		t.Fatal("large finite result failed terminal correctness", err)
	}
	ack(t, inbox)
	if status, _ := runtime.Inspect(); status.Active != 1 {
		t.Fatal("completed reader retained actual-work reservation")
	}
	if got := query(t, owner, inbox, "SELECT nextval('reader_once')").Snapshot().Steps[0].Rows[0][0]; got != int64(70001) {
		t.Fatal("reader secretly re-executed its SELECT")
	}
	t.Logf("real native result: %d ordered exact rows, %d chunks, max rows/chunk %d; no RSS guarantee", total, chunks, selected.ReaderChunkRows)
}

func TestReaderObservedEOFAgainstTotalBoundary(t *testing.T) {
	for _, rowCount := range []int{0, 4, 5} {
		t.Run(fmt.Sprint(rowCount), func(t *testing.T) {
			selected := testSettings()
			selected.ReaderChunkRows, selected.ReaderTotalRows = 2, 4
			owner, inbox, _ := testOwner(t, selected, 0)
			reader, err := owner.Client().Read(context.Background(), fmt.Sprintf("SELECT range FROM range(%d)", rowCount))
			if err != nil {
				t.Fatal(err)
			}
			total := 0
			for {
				value, readErr := reader.Next(context.Background())
				progress := value.Snapshot()
				if !value.HasData() || progress.Reader == nil {
					t.Fatal("accepted read lost progress", readErr)
				}
				total += len(progress.Steps[0].Rows)
				ack(t, inbox)
				if progress.Reader.Closed {
					if rowCount == 5 {
						if !errors.Is(readErr, ErrLimit) || progress.Steps[0].Complete || !progress.Steps[0].Limited || total != 4 {
							t.Fatal("over-bound prefix misreported as complete", readErr)
						}
					} else if readErr != nil || !progress.Steps[0].Complete || total != rowCount || progress.Steps[0].Rows == nil {
						t.Fatal("actual EOF or successful empty read lost", readErr)
					}
					break
				}
				if progress.Steps[0].Complete || readErr != nil {
					t.Fatal("chunk boundary became EOF", readErr)
				}
			}
			terminal, err := reader.Result(context.Background())
			if (rowCount == 5) != errors.Is(err, ErrLimit) || len(terminal.Snapshot().Steps[0].Rows) != 0 {
				t.Fatal("terminal evidence disagreed or retained historical rows", err)
			}
			ack(t, inbox)
		})
	}
}

func TestReaderSaturationRefusesBeforeAdvanceAndStillCloses(t *testing.T) {
	for _, seal := range []bool{false, true} {
		t.Run(fmt.Sprint(seal), func(t *testing.T) {
			selected := testSettings()
			selected.Connections, selected.ReaderChunkRows = 1, 1
			owner, inbox, runtime := testOwner(t, selected, 3)
			reader, err := owner.Client().Read(context.Background(), "SELECT range FROM range(5)")
			if err != nil {
				t.Fatal(err)
			}
			first, err := reader.Next(context.Background())
			if err != nil || first.Snapshot().Steps[0].Rows[0][0] != int64(0) {
				t.Fatal(err)
			}
			delivery := receive(t, inbox)
			if status, _ := inbox.Inspect(); status.Outstanding != 3 || status.Claimed != 1 {
				t.Fatal("claimed evidence did not remain charged")
			}
			if value, err := reader.Next(context.Background()); !errors.Is(err, adapters.ErrEvidence) || value.HasData() {
				t.Fatal("saturated evidence admitted hidden rows", err)
			}
			if err := delivery.Ack(); err != nil {
				t.Fatal(err)
			}
			second, err := reader.Next(context.Background())
			if err != nil || second.Snapshot().Steps[0].Rows[0][0] != int64(1) || second.Snapshot().Reader.Offset != 1 {
				t.Fatal("rejected Next advanced native rows", err)
			}
			if seal {
				if err := inbox.Seal(); err != nil {
					t.Fatal(err)
				}
			}
			terminal, err := reader.Close(context.Background())
			if err != nil || !terminal.Snapshot().Reader.Closed || terminal.Snapshot().Steps[0].Complete || terminal.Snapshot().Reader.TotalRows != 2 {
				t.Fatal("saturated early Close depended on admission or invented EOF", err)
			}
			if err := owner.Close(context.Background()); err != nil || !owner.ShutdownComplete() {
				t.Fatal("saturated source cleanup failed", err)
			}
			for range 3 {
				ack(t, inbox)
			}
			if state, _ := runtime.Inspect(); state.Active != 0 || state.WorkBytes != 0 {
				t.Fatal("saturated cleanup retained work")
			}
		})
	}
}

func TestReaderAbandonmentAndOwnerLifetime(t *testing.T) {
	selected := testSettings()
	selected.ReaderLifetime = 50 * time.Millisecond
	owner, inbox, _ := testOwner(t, selected, 0)
	reader, err := owner.Client().Read(context.Background(), "SELECT range FROM range(10)")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	terminal, err := reader.Result(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || !terminal.Snapshot().ConnectionClosed || !terminal.Snapshot().Reader.Closed || terminal.Snapshot().Steps[0].Complete {
		t.Fatal("abandoned reader was not reclaimed with incomplete evidence", err)
	}
	ack(t, inbox)
	reader, err = owner.Client().Read(context.Background(), "SELECT range FROM range(10)")
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		t.Fatal("owner failed to join abandoned reader", err)
	}
	terminal, err = reader.Result(ctx)
	if !errors.Is(err, context.Canceled) || !terminal.Snapshot().Reader.Closed || terminal.Snapshot().Steps[0].Complete {
		t.Fatal("owner shutdown lost reader cancellation facts", err)
	}
	ack(t, inbox)
	ack(t, inbox)
}

func TestReaderOversizedScalarFailsWithoutSuccessfulChunk(t *testing.T) {
	selected := testSettings()
	selected.ReaderChunkBytes = 1024
	owner, inbox, _ := testOwner(t, selected, 0)
	reader, err := owner.Client().Read(context.Background(), "SELECT repeat('x',1048576)")
	if err != nil {
		t.Fatal(err)
	}
	value, err := reader.Next(context.Background())
	if !errors.Is(err, ErrLimit) || value.Snapshot().Steps[0].Complete || !value.Snapshot().Steps[0].Limited || len(value.Snapshot().Steps[0].Rows) != 0 {
		t.Fatal("oversized decoder value was published as a successful chunk", err)
	}
	ack(t, inbox)
	if _, err := reader.Result(context.Background()); !errors.Is(err, ErrLimit) {
		t.Fatal("terminal lost oversized scalar failure", err)
	}
	ack(t, inbox)
}
