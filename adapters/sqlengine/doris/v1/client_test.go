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

package doris

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type reader struct{ t testing.TB }

func (test reader) inspect(receipt *adapters.Receipt[Result], err error) adapters.Snapshot[Result] {
	t := test.t
	t.Helper()
	if err != nil || receipt == nil {
		t.Fatal("public admission", err)
	}
	value, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func valueOf(t testing.TB, snapshot adapters.Snapshot[Result]) Result {
	t.Helper()
	value, present := snapshot.ValueCopy()
	if !present {
		t.Fatal("missing public evidence")
	}
	return value
}
func (test reader) checked(receipt *adapters.Receipt[Result], err error) Result {
	t := test.t
	t.Helper()
	snapshot := test.inspect(receipt, err)
	if snapshot.Err() != nil {
		t.Fatal(snapshot.Err())
	}
	return valueOf(t, snapshot)
}

func TestDirectSQLAndCopiedEvidence(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newSQLPeer(t, secure)
		owner, inbox, _ := testOwner(t, peer.options(), 0)
		if peer.queries.Load() != 0 {
			t.Fatal("construction performed readiness I/O")
		}
		profile, err := owner.Client().Profile(testContext(t))
		if err != nil || profile.ServiceMode.Value != "doris" {
			t.Fatal("profile", err)
		}
		profile.Options[0].Value = "changed"
		again, _ := owner.Client().Profile(testContext(t))
		if again.Options[0].Value == "changed" {
			t.Fatal("profile aliased")
		}
		client, err := owner.Client().WithID("opaque-业务")
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"invalid\nvalue", strings.Repeat("x", 257), string([]byte{255})} {
			if _, err := client.WithID(bad); err == nil {
				t.Fatal("bad correlation")
			}
		}
		value := (reader{t}).checked(client.Query(testContext(t), "SELECT exact"))
		row := value.RowsCopy()[0].ValuesCopy()
		if row[0] != nil || row[1] == nil || string(row[2]) != "18446744073709551615" || string(row[3]) != "12345678901234567890.00100" {
			t.Fatal("lossy rows")
		}
		row[2][0] = 'X'
		columns := value.ColumnsCopy()
		columns[0].Name = "mutated"
		if string(value.RowsCopy()[0].ValuesCopy()[2]) != "18446744073709551615" || value.ColumnsCopy()[0].Name == "mutated" {
			t.Fatal("aliased evidence")
		}
		if !value.Complete() || !value.Dispatched() || value.SQLAcknowledged() || value.Attribution().ID != "opaque-业务" || value.Attribution().Operation != "database.doris.query" {
			t.Fatal("attribution/effects")
		}
		delivery := receive(t, inbox)
		receipt, err := delivery.Receipt()
		if err != nil {
			t.Fatal(err)
		}
		saved, _ := receipt.Snapshot()
		copy := valueOf(t, saved)
		if copy.Attribution() != value.Attribution() {
			t.Fatal("custody diverged")
		}
		original := peer.queries.Load()
		sinkErr := errors.New("sink-refusal")
		sink := func(adapters.Snapshot[Result]) error { return sinkErr }
		if err := sink(saved); !errors.Is(err, sinkErr) {
			t.Fatal("sink", err)
		}
		if err := delivery.Retry(); err != nil {
			t.Fatal(err)
		}
		retry := receive(t, inbox)
		if err := retry.Ack(); err != nil {
			t.Fatal(err)
		}
		if peer.queries.Load() != original {
			t.Fatal("evidence redelivery redispatched SQL")
		}
		executed := (reader{t}).checked(client.Exec(testContext(t), "INSERT authorized"))
		if !executed.SQLAcknowledged() || !executed.Complete() {
			t.Fatal("missing acknowledgement")
		}
		if affected, known := executed.RowsAffected(); !known || affected != 2 {
			t.Fatal("aggregate affected rows")
		}
		ack(t, inbox)
		partial := (reader{t}).inspect(client.Query(testContext(t), "SELECT partial"))
		native, ok := InspectError(partial.Err())
		if !ok || native.Number != 1077 || !errors.Is(partial.Err(), ErrSQL) || len(valueOf(t, partial).RowsCopy()) != 1 || valueOf(t, partial).Complete() {
			t.Fatal("native partial evidence")
		}
		ack(t, inbox)
		lost := (reader{t}).inspect(client.Exec(testContext(t), "INSERT lost"))
		if lost.Err() == nil || valueOf(t, lost).SQLAcknowledged() || !valueOf(t, lost).Dispatched() {
			t.Fatal("lost reply certified")
		}
		ack(t, inbox)
	}
}

func TestPublicCursorPagesAndSaturatedShutdown(t *testing.T) {
	peer := newSQLPeer(t, false)
	options := peer.options()
	options.Active = 1
	options.MaxRows = 1
	options.MaxPageRows = 2
	owner, inbox, runtime := testOwner(t, options, 3)
	setup, cancel := context.WithCancel(testContext(t))
	cursor, root, err := owner.Client().QueryCursor(setup, testContext(t), "SELECT pages 5")
	if err != nil || cursor == nil {
		t.Fatal(err)
	}
	cancel()
	rootInfo, _ := root.Snapshot()
	var total int
	for page := 0; page < 3; page++ {
		value := (reader{t}).checked(cursor.Next(testContext(t)))
		for _, row := range value.RowsCopy() {
			if string(row.ValuesCopy()[0]) != strconv.Itoa(total) {
				t.Fatal("order")
			}
			total++
		}
		if value.Attribution().Parent != rootInfo.Info().Sequence || value.Attribution().Operation != "database.doris.cursor-next" || value.Complete() != (page == 2) {
			t.Fatal("page identity/EOF")
		}
		if page < 2 {
			// Owner + cursor + page fill all evidence. Rejection must not consume
			// another row, and Close must not need another result slot.
			if _, err := cursor.Next(testContext(t)); !errors.Is(err, adapters.ErrEvidence) {
				t.Fatal("page evidence not bounded", err)
			}
		}
		ack(t, inbox)
	}
	if total != 5 {
		t.Fatal("result truncated")
	}
	final, err := root.WaitReleased(testContext(t))
	if err != nil || final.Err() != nil || !valueOf(t, final).Complete() || valueOf(t, final).RowsRead() != 5 {
		t.Fatal("terminal", err, final.Err())
	}
	if err := cursor.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	abandoned, _, err := owner.Client().QueryCursor(testContext(t), testContext(t), "SELECT pages 5")
	if err != nil || abandoned == nil {
		t.Fatal(err)
	}
	(reader{t}).checked(abandoned.Next(testContext(t)))
	status, _ := inbox.Inspect()
	if status.Outstanding != 3 {
		t.Fatal("expected saturation")
	}
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("shutdown at saturation", err)
	}
	terminal, err := abandoned.Receipt().WaitReleased(testContext(t))
	if err != nil || !errors.Is(terminal.Err(), context.Canceled) || valueOf(t, terminal).Complete() {
		t.Fatal("abandonment certified")
	}
	usage, _ := runtime.Inspect()
	if usage.Active != 0 || usage.WorkBytes != 0 {
		t.Fatal("work reservation leaked")
	}
}

func TestPublicCloseTimeoutKeepsResponsibility(t *testing.T) {
	peer := newSQLPeer(t, false)
	o := peer.options()
	o.Active = 1
	owner, _, runtime := testOwner(t, o, 0)
	cursor, root, err := owner.Client().QueryCursor(testContext(t), testContext(t), "SELECT page-stall")
	if err != nil || cursor == nil {
		t.Fatal(err)
	}
	cursor.group.gate.Lock()
	wait, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = owner.Close(wait)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
		cursor.group.gate.Unlock()
		t.Fatal("incomplete release hidden", err)
	}
	before, _ := runtime.Inspect()
	if before.Active != 2 {
		cursor.group.gate.Unlock()
		t.Fatal("work lost during timeout")
	}
	cursor.group.gate.Unlock()
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("continuation", err)
	}
	terminal, err := root.WaitReleased(testContext(t))
	if err != nil || !errors.Is(terminal.Err(), context.Canceled) {
		t.Fatal("original evidence lost", err)
	}
}
