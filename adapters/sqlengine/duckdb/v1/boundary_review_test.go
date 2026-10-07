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
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestReviewedConfigurationPreservesExplicitZero(t *testing.T) {
	defaults := Settings{Name: "strict-config", Path: filepath.Join(t.TempDir(), "not-created", "database.duckdb"), Connections: 3, QueuedCalls: 2}
	schema, err := Configuration(defaults)
	if err != nil || schema.Defaults.Connections != 3 || schema.Defaults.ReaderTotalRows == 0 || schema.Defaults.Timeout == 0 {
		t.Fatal("configuration failed to resolve defaults exactly once", err)
	}
	for _, name := range []string{
		"connections", "threads", "memory_bytes", "timeout_ns", "cleanup_timeout_ns", "max_rows", "max_batch_rows",
		"input_bytes", "result_bytes", "reader_chunk_rows", "reader_chunk_bytes", "reader_total_rows", "reader_total_bytes", "reader_lifetime_ns",
	} {
		for _, encoding := range []configsource.Encoding{configsource.JSON, configsource.YAML} {
			raw := fmt.Sprintf(`{"%s":0}`, name)
			if encoding == configsource.YAML {
				raw = name + ": 0\n"
			}
			_, err := configsource.Prepare(context.Background(), schema, []configsource.Layer{{Kind: configsource.Base, Encoding: encoding, Content: []byte(raw)}})
			if !errors.Is(err, ErrInput) {
				t.Fatalf("explicit zero %s was re-defaulted or lost its error identity", name)
			}
		}
	}
	for _, raw := range []string{`{}`, `{"queued_calls":0,"path":"","reader_total_rows":70001}`} {
		prepared, err := configsource.Prepare(context.Background(), schema, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(raw)}})
		if err != nil {
			t.Fatal("valid absence or explicit disabled setting was refused", err)
		}
		value, err := prepared.ValueCopy()
		if err != nil || value.Connections != 3 || value.Timeout != schema.Defaults.Timeout || value.ReaderChunkBytes != schema.Defaults.ReaderChunkBytes {
			t.Fatal("absent fields failed to inherit resolved defaults", err)
		}
		if raw != `{}` && (value.QueuedCalls != 0 || value.Path != "" || value.ReaderTotalRows != 70001) {
			t.Fatal("legal explicit zero, empty path or positive override was lost")
		}
		policy, err := Recommend(value)
		if err != nil || policy.Runtime.MaxActive != 4 || policy.Runtime.MaxQueued != value.QueuedCalls {
			t.Fatal("strict preparation and admission budget diverged", err)
		}
	}
	if _, err := os.Stat(filepath.Dir(defaults.Path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("strict configuration performed native file I/O", err)
	}
	if Validate(Settings{Name: "direct-literal"}) != nil {
		t.Fatal("strict overlay validation changed documented direct-literal defaults")
	}
}

func TestReviewedReaderCanceledCloseKeepsKnownTerminalResult(t *testing.T) {
	selected := testSettings()
	selected.ReaderChunkRows = 1
	owner, inbox, runtime := testOwner(t, selected, 0)
	before, err := runtime.Inspect()
	if err != nil || before.Active != 1 {
		t.Fatal("source ownership was not charged", err)
	}
	reader, err := owner.Client().Read(context.Background(), "SELECT range FROM range(4)")
	if err != nil {
		t.Fatal(err)
	}
	retained, err := runtime.Inspect()
	if err != nil || retained.Active != 2 || retained.WorkBytes <= before.WorkBytes {
		t.Fatal("retained reader has no independent actual-work charge", err)
	}
	first, err := reader.Next(context.Background())
	if err != nil || first.Snapshot().Reader.TotalRows != 1 || first.Snapshot().Steps[0].Complete {
		t.Fatal("reader did not retain a partial result", err)
	}
	ack(t, inbox)
	cleanup, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("review-close-cancellation")
	cancel(cause)
	_, closeErr := reader.Close(cleanup)
	if !errors.Is(closeErr, context.Canceled) || !errors.Is(closeErr, cause) {
		t.Fatal("cleanup cancellation or its cause disappeared", closeErr)
	}
	terminal, terminalErr := reader.Result(context.Background())
	if !errors.Is(terminalErr, ErrCleanup) || !errors.Is(terminalErr, cause) || !terminal.HasData() {
		t.Fatal("terminal result lost cleanup failure or native progress", terminalErr)
	}
	progress := terminal.Snapshot()
	if progress.Reader == nil || !progress.Reader.Closed || progress.Reader.TotalRows != 1 || !progress.ConnectionClosed || progress.Steps[0].Complete || len(progress.Steps[0].Rows) != 0 {
		t.Fatal("cleanup manufactured EOF, retained row history or lost release facts")
	}
	again, againErr := reader.Close(context.Background())
	if !errors.Is(againErr, ErrCleanup) || !errors.Is(againErr, cause) || !again.HasData() || !reflect.DeepEqual(again.Snapshot(), progress) {
		t.Fatal("repeated cleanup lost its known terminal result", againErr)
	}
	delivery := receive(t, inbox)
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := receipt.Snapshot()
	evidence, present := snapshot.ValueCopy()
	if !present || snapshot.Primary() != nil || !errors.Is(snapshot.Cleanup(), cause) || !reflect.DeepEqual(evidence.Snapshot(), progress) {
		t.Fatal("independent cleanup evidence changed phase or progress")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	after, err := runtime.Inspect()
	if err != nil || after.Active != before.Active || after.WorkBytes != before.WorkBytes {
		t.Fatal("cleanup did not release exactly the reader reservation", err)
	}
}

func TestReviewedReaderConcurrentUseRefusesBeforeAdvance(t *testing.T) {
	selected := testSettings()
	selected.ReaderChunkRows = 1
	owner, inbox, _ := testOwner(t, selected, 0)
	reader, err := owner.Client().Read(context.Background(), "SELECT range FROM range(3)")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := inbox.Inspect()
	reader.session.family.gate.Lock()
	_, nextErr := reader.Next(context.Background())
	_, closeErr := reader.Close(context.Background())
	reader.session.family.gate.Unlock()
	if !errors.Is(nextErr, ErrState) || !errors.Is(closeErr, ErrState) {
		t.Fatal("overlapping reader use did not refuse before native work")
	}
	after, _ := inbox.Inspect()
	if before != after {
		t.Fatal("concurrent-use refusal consumed evidence capacity")
	}
	first, err := reader.Next(context.Background())
	if err != nil || first.Snapshot().Steps[0].Rows[0][0] != int64(0) || first.Snapshot().Reader.Chunk != 1 {
		t.Fatal("refused concurrent operation advanced native rows", err)
	}
	ack(t, inbox)
	if _, err := reader.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
}

func TestReviewedIgnoredReturnsAndRetryRetainEvidence(t *testing.T) {
	selected := testSettings()
	policy, err := Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := adapters.NewObserver(1)
	if err != nil {
		t.Fatal(err)
	}
	owner, openErr := Open(context.Background(), selected, Dependencies{Runtime: runtime, Evidence: inbox, Observer: observer})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if owner != nil {
			if err := owner.Close(ctx); err != nil {
				t.Error(err)
			}
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		for state, _ := inbox.Inspect(); state.Outstanding > 0; state, _ = inbox.Inspect() {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	if openErr != nil {
		t.Fatal(openErr)
	}
	execute(t, owner, inbox, "CREATE SEQUENCE retry_evidence")
	client, err := owner.Client().WithID("opaque-review-correlation")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(context.Background(), context.Background(), "SELECT nextval('retry_evidence')"); err != nil {
		t.Fatal(err)
	}
	delivery := receive(t, inbox)
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := receipt.Snapshot()
	value, present := snapshot.ValueCopy()
	if !present || value.Snapshot().Steps[0].Rows[0][0] != int64(1) || value.Attribution().ID != "opaque-review-correlation" || value.Attempts().Exact || value.Attempts().Observed != 0 {
		t.Fatal("ignored result lost exact data, attribution or attempt unknownness")
	}
	before, _ := inbox.Inspect()
	if err := delivery.Retry(); err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); !errors.Is(err, adapters.ErrReleased) {
		t.Fatal("old evidence claim could erase retried custody", err)
	}
	retried := receive(t, inbox)
	retriedReceipt, err := retried.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	retriedSnapshot, _ := retriedReceipt.Snapshot()
	retriedValue, present := retriedSnapshot.ValueCopy()
	after, _ := inbox.Inspect()
	if !present || before.Outstanding != after.Outstanding || before.Bytes != after.Bytes || retriedSnapshot.Info() != snapshot.Info() || !reflect.DeepEqual(retriedValue.Snapshot(), value.Snapshot()) {
		t.Fatal("retry recreated facts or lost evidence reservation")
	}
	if err := retried.Ack(); err != nil {
		t.Fatal(err)
	}
	if got := query(t, owner, inbox, "SELECT nextval('retry_evidence')").Snapshot().Steps[0].Rows[0][0]; got != int64(2) {
		t.Fatal("receiver retry re-executed native SQL")
	}
	observation, err := observer.Inspect()
	if err != nil || observation.Dropped == 0 {
		t.Fatal("saturation control did not actually drop optional observation", err)
	}
}
