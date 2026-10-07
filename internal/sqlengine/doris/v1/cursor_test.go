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
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func childID(name string) fault.Correlation { return fault.Correlation{Call: name, Parent: "cursor"} }

func consumeCursorTest(t *testing.T, fixture fixture, query string) (invocation.Result[Result], int) {
	t.Helper()
	cursor, receipt, err := fixture.client.QueryCursor(context.Background(), context.Background(), correlation("cursor"), query)
	if err != nil || cursor == nil {
		t.Fatalf("setup: %v", err)
	}
	root, err := fixture.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var rowCount int
	for index := 0; ; index++ {
		if index > 2000 {
			t.Fatal("unbounded pages")
		}
		page, err := cursor.Next(context.Background(), childID(strconv.Itoa(index)))
		value := observe(t, page, err)
		rowCount += len(value.Outcome.Value.RowsCopy())
		drain(t, fixture.inbox, 1)
		if value.Err() != nil || value.Outcome.Value.Complete() {
			break
		}
	}
	result, err := receipt.WaitReleased(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Release(); err != nil {
		t.Fatal(err)
	}
	return result, rowCount
}

func TestCursorExactTotalLimits(t *testing.T) {
	peer := newSQLPeer(t, false)
	options := peer.options()
	options.MaxPageRows = 7
	options.MaxCursorRows = 257
	first := bindFixture(t, options, 2)
	result, rowCount := consumeCursorTest(t, first, "SELECT pages 257")
	if result.Err() != nil || !result.Outcome.Value.Complete() || rowCount != 257 {
		t.Fatal("exact row cap", result.Err(), rowCount)
	}
	responseBytes := result.Outcome.Value.ResponseBytes()
	if responseBytes < 1024 {
		t.Fatal("test needs configurable byte cap")
	}
	for _, delta := range []int{0, -1} {
		t.Run("bytes"+strconv.Itoa(delta), func(t *testing.T) {
			options.MaxResponseBytes = responseBytes + delta
			fixture := bindFixture(t, options, 2)
			result, rowCount := consumeCursorTest(t, fixture, "SELECT pages 257")
			if delta == 0 {
				if result.Err() != nil || !result.Outcome.Value.Complete() || rowCount != 257 {
					t.Fatal("exact response cap", result.Err(), rowCount)
				}
			} else if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() {
				t.Fatal("one-byte-short cap certified EOF", result.Err())
			}
		})
	}
}

func TestCursorPagesAndLifetime(t *testing.T) {
	for _, secure := range []bool{false, true} {
		for _, count := range []int{0, 1, 4, 5, 257} {
			peer := newSQLPeer(t, secure)
			options := peer.options()
			options.MaxRows = 1
			options.MaxPageRows = 2
			options.MaxPageBytes = 1024
			fixture := bindFixture(t, options, 2)
			setup, endSetup := context.WithCancel(context.Background())
			cursor, receipt, err := fixture.client.QueryCursor(setup, context.Background(), correlation("cursor"), "SELECT pages "+strconv.Itoa(count))
			if err != nil || cursor == nil {
				t.Fatal("setup", err)
			}
			endSetup()
			root, err := fixture.inbox.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var seen []string
			for page := 0; ; page++ {
				got, err := cursor.Next(context.Background(), childID("page-"+strconv.Itoa(page)))
				value := observe(t, got, err)
				if value.Err() != nil {
					t.Fatal(value.Err())
				}
				data := value.Outcome.Value
				if len(data.ColumnsCopy()) != 1 || len(data.RowsCopy()) > 2 {
					t.Fatal("page bound or metadata")
				}
				for _, row := range data.RowsCopy() {
					seen = append(seen, string(row.ValuesCopy()[0]))
				}
				drain(t, fixture.inbox, 1)
				if data.Complete() {
					if len(seen) != count {
						t.Fatal("premature EOF", len(seen))
					}
					break
				}
				if page > count {
					t.Fatal("no EOF")
				}
			}
			for index, value := range seen {
				if value != strconv.Itoa(index) {
					t.Fatal("order changed")
				}
			}
			value, err := receipt.WaitReleased(context.Background())
			if err != nil || value.Err() != nil || !value.Outcome.Value.Complete() || value.Outcome.Value.RowsRead() != count || value.Outcome.Value.ResponseBytes() == 0 {
				t.Fatal("terminal facts", err, value.Err())
			}
			if err := cursor.Close(context.Background()); err != nil {
				t.Fatal("idempotent close", err)
			}
			if err := root.Release(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCursorRejectsBoundsAndMalformedResults(t *testing.T) {
	for _, test := range []struct {
		name, query string
		configure   func(*OptionsV1)
		want        error
	}{
		{"rows", "SELECT pages 5", func(o *OptionsV1) { o.MaxCursorRows = 3; o.MaxPageRows = 1 }, ErrLimit},
		{"response", "SELECT page-bytes", func(o *OptionsV1) { o.MaxResponseBytes = 1024; o.MaxPageRows = 1 }, ErrLimit},
		{"row-size", "SELECT large", func(o *OptionsV1) { o.MaxPageBytes = 1024 }, ErrLimit},
		{"false-eof", "SELECT false-eof", func(o *OptionsV1) {}, ErrProtocol},
		{"malformed", "SELECT malformed-row", func(o *OptionsV1) {}, ErrProtocol},
		{"late-error", "SELECT partial", func(o *OptionsV1) {}, ErrSQL},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer := newSQLPeer(t, false)
			options := peer.options()
			test.configure(&options)
			fixture := bindFixture(t, options, 2)
			cursor, root, err := fixture.client.QueryCursor(context.Background(), context.Background(), correlation("cursor"), test.query)
			if cursor == nil || err != nil {
				t.Fatal("setup", err)
			}
			record, err := fixture.inbox.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			total := 0
			for page := 0; ; page++ {
				receipt, err := cursor.Next(context.Background(), childID("page-"+strconv.Itoa(page)))
				result := observe(t, receipt, err)
				total += len(result.Outcome.Value.RowsCopy())
				drain(t, fixture.inbox, 1)
				if result.Err() != nil {
					if !errors.Is(result.Err(), test.want) || result.Outcome.Value.Complete() {
						t.Fatal("wrong rejection", result.Err())
					}
					break
				}
				if page > 10 || result.Outcome.Value.Complete() {
					t.Fatal("limit/error lost")
				}
			}
			result, err := root.WaitReleased(context.Background())
			if err != nil || !errors.Is(result.Err(), test.want) || result.Outcome.Value.Complete() {
				t.Fatal("terminal lost error", result.Err())
			}
			if test.name == "rows" && total != 3 {
				t.Fatal("page rows reset aggregate", total)
			}
			if test.name == "late-error" && total != 1 {
				t.Fatal("partial row lost")
			}
			if err := record.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCursorBytePagesAndDetachedCopies(t *testing.T) {
	peer := newSQLPeer(t, false)
	o := peer.options()
	o.MaxPageBytes = 1024
	o.MaxResultBytes = 1024
	f := bindFixture(t, o, 2)
	cursor, root, err := f.client.QueryCursor(context.Background(), context.Background(), correlation("cursor"), "SELECT page-bytes")
	if err != nil || cursor == nil {
		t.Fatal(err)
	}
	record, _ := f.inbox.Next(context.Background())
	for page := 0; page < 5; page++ {
		receipt, err := cursor.Next(context.Background(), childID("page-"+strconv.Itoa(page)))
		result := observe(t, receipt, err)
		if result.Err() != nil || len(result.Outcome.Value.RowsCopy()) != 2 || result.Outcome.Value.Complete() != (page == 4) {
			t.Fatal("byte page", result.Err())
		}
		row := result.Outcome.Value.RowsCopy()[0].ValuesCopy()
		row[0][0] = 'z'
		if result.Outcome.Value.RowsCopy()[0].ValuesCopy()[0][0] != 'x' {
			t.Fatal("aliased")
		}
		drain(t, f.inbox, 1)
	}
	result, _ := root.WaitReleased(context.Background())
	if result.Outcome.Value.RowsRead() != 10 || !result.Outcome.Value.Complete() {
		t.Fatal("totals")
	}
	if err := record.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestCursorCancellationSaturationAndClose(t *testing.T) {
	peer := newSQLPeer(t, false)
	o := peer.options()
	o.Active = 1
	f := bindFixture(t, o, 2)
	cursor, root, err := f.client.QueryCursor(context.Background(), context.Background(), correlation("cursor"), "SELECT page-stall")
	if err != nil || cursor == nil {
		t.Fatal(err)
	}
	record, _ := f.inbox.Next(context.Background())
	done := make(chan invocationResult, 1)
	go func() { r, e := cursor.Next(context.Background(), childID("page")); done <- invocationResult{r, e} }()
	deadline := time.Now().Add(time.Second)
	for cursor.state.mu.TryLock() {
		cursor.state.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("page did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := cursor.Next(context.Background(), childID("overlap")); !errors.Is(err, ErrState) {
		t.Fatal("overlap allowed", err)
	}
	if _, err := f.client.Exec(context.Background(), correlation("full"), "INSERT no"); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("evidence not full", err)
	}
	wait, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cursor.Close(wait); !errors.Is(err, context.Canceled) {
		t.Fatal("abandonment not retained", err)
	}
	select {
	case got := <-done:
		value := observe(t, got.receipt, got.err)
		if !errors.Is(value.Err(), context.Canceled) || value.Outcome.Value.Complete() {
			t.Fatal("canceled page complete")
		}
	case <-wait.Done():
		t.Fatal("Rows.Close drained")
	}
	drain(t, f.inbox, 1)
	value, err := root.WaitReleased(wait)
	if err != nil || value.Outcome.Value.Complete() || !value.Outcome.Value.Dispatched() {
		t.Fatal("abandonment facts")
	}
	if err := record.Release(); err != nil {
		t.Fatal(err)
	}
	if err := f.assembly.Close(wait); err != nil {
		t.Fatal("retained native lease", err)
	}
}

func TestCursorSetupCancellationAndLifetimeExpiry(t *testing.T) {
	for _, setup := range []bool{true, false} {
		peer := newSQLPeer(t, false)
		o := peer.options()
		o.CursorTimeout = 40 * time.Millisecond
		f := bindFixture(t, o, 2)
		query := "SELECT page-stall"
		if setup {
			query = "SELECT stalled"
		}
		cursor, root, err := f.client.QueryCursor(context.Background(), context.Background(), correlation("cursor"), query)
		if err != nil || root == nil {
			t.Fatal(err)
		}
		if setup && cursor != nil {
			t.Fatal("failed setup leaked cursor")
		}
		wait, cancel := context.WithTimeout(context.Background(), time.Second)
		value, err := root.WaitReleased(wait)
		cancel()
		if err != nil || !errors.Is(value.Err(), context.DeadlineExceeded) || value.Outcome.Value.Complete() {
			t.Fatal("unbounded lifetime", err, value.Err())
		}
		drain(t, f.inbox, 1)
	}
}

func TestCursorPageCancellationKeepsPartialRows(t *testing.T) {
	peer := newSQLPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	cursor, root, err := fixture.client.QueryCursor(context.Background(), context.Background(), correlation("cursor"), "SELECT page-partial-stall")
	if cursor == nil || err != nil {
		t.Fatal(err)
	}
	rootDelivery, _ := fixture.inbox.Next(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	receipt, err := cursor.Next(ctx, childID("page"))
	cancel()
	page := observe(t, receipt, err)
	if !errors.Is(page.Err(), context.DeadlineExceeded) || page.Outcome.Value.Complete() || len(page.Outcome.Value.RowsCopy()) != 1 {
		t.Fatal("partial cancellation evidence", page.Err())
	}
	drain(t, fixture.inbox, 1)
	terminal, err := root.WaitReleased(context.Background())
	if err != nil || !errors.Is(terminal.Err(), context.DeadlineExceeded) || terminal.Outcome.Value.Complete() {
		t.Fatal("terminal cancellation evidence", err)
	}
	if err := rootDelivery.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestCursorSetupBudgetAndExactValues(t *testing.T) {
	peer := newSQLPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	setup, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	cursor, receipt, err := fixture.client.QueryCursor(setup, context.Background(), correlation("cursor"), "SELECT stalled")
	cancel()
	if cursor != nil || err != nil || receipt == nil {
		t.Fatal("setup ownership", err)
	}
	result, err := receipt.WaitReleased(context.Background())
	if err != nil || !errors.Is(result.Err(), context.DeadlineExceeded) {
		t.Fatal("setup budget lost", result.Err())
	}
	drain(t, fixture.inbox, 1)
	cursor, receipt, err = fixture.client.QueryCursor(context.Background(), context.Background(), correlation("cursor"), "SELECT exact")
	if cursor == nil || err != nil {
		t.Fatal(err)
	}
	root, _ := fixture.inbox.Next(context.Background())
	page, err := cursor.Next(context.Background(), childID("page"))
	value := observe(t, page, err)
	values := value.Outcome.Value.RowsCopy()[0].ValuesCopy()
	if value.Err() != nil || !value.Outcome.Value.Complete() || values[0] != nil || values[1] == nil ||
		string(values[2]) != "18446744073709551615" || string(values[3]) != "12345678901234567890.00100" ||
		values[5][2] != 255 || len(value.Outcome.Value.ColumnsCopy()) != 7 {
		t.Fatal("cursor value/metadata loss", value.Err())
	}
	drain(t, fixture.inbox, 1)
	if _, err := receipt.WaitReleased(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := root.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparationAuthoritativeResolvedBudgets(t *testing.T) {
	o := OptionsV1{Name: "doris", SQLAddress: "127.0.0.1:1", Database: "test", User: "user", Plaintext: true}
	layer := resource.Layer{Kind: resource.Local, Content: []byte("active: 2\nmax_page_rows: 999\nmax_page_bytes: 1048576\nmax_rows: 3")}
	prepared, err := PrepareV1(o, layer)
	if err != nil {
		t.Fatal(err)
	}
	resolved := prepared.OptionsCopy()
	if resolved.MaxRows != 3 || resolved.MaxPageRows != 999 || resolved.Active != 2 {
		t.Fatal("overlay not resolved")
	}
	want := defaults(resolved)
	budget := prepared.Reservation()
	if budget.WorkBytes != want.reservation() || budget.EvidenceBytes != want.evidenceReservation() || budget.Limits != want.limits() {
		t.Fatal("reservation drift")
	}
	selected := resource.WithLimits(prepared.Selection(), budget.Limits)
	a, err := resource.Assemble(context.Background(), context.Background(), "resolved", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	owned, _, err := resource.Bind(a, selected)
	if err != nil || !reflect.DeepEqual(owned.owner.settings, want) {
		t.Fatal("selection differs from description", err)
	}
	for _, bad := range []string{"max_page_rows: 0", "max_page_bytes: 0", "max_cursor_rows: 0", "cursor_timeout_ns: 0"} {
		if _, err := PrepareV1(o, resource.Layer{Kind: resource.Local, Content: []byte(bad)}); err == nil {
			t.Fatal("strict zero defaulted", bad)
		}
	}
}
