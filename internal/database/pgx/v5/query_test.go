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

package pgx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/http/httptrace"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func queryResult(t testing.TB, database *Database, id, sql string, args ...any) invocation.Result[Result] {
	t.Helper()
	receipt, err := database.Query(context.Background(), correlation(id), sql, args...)
	return operationResult(t, receipt, err)
}
func TestQueryNativeResultAndImmutableCopies(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 4)
	result := queryResult(t, fixture.database, "cells", "SELECT cells")
	if result.Err() != nil || !result.Outcome.Present || !result.Outcome.Value.Complete() || result.Outcome.Value.RowsRead() != 1 {
		t.Fatal("complete cells missing", result.Err())
	}
	row, err := result.Outcome.Value.First()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{nil, {}, []byte("value-canary")}
	if !reflect.DeepEqual(row.ValuesCopy(), want) {
		t.Fatal("NULL, empty and present cells conflated")
	}
	copied := row.ValuesCopy()
	copied[1] = []byte("mutated")
	copied[2][0] = 'X'
	columns := result.Outcome.Value.ColumnsCopy()
	columns[0].Name = "changed"
	rows := result.Outcome.Value.RowsCopy()
	rows[0] = Row{}
	if !reflect.DeepEqual(row.ValuesCopy(), want) || result.Outcome.Value.ColumnsCopy()[0].Name != "value" {
		t.Fatal("caller copies changed immutable evidence")
	}
	conformance.Runtime(t, result.Outcome.Value, new(Result), "value-canary")
	conformance.Runtime(t, row, new(Row), "value-canary")
	empty := queryResult(t, fixture.database, "empty", "SELECT empty")
	if empty.Err() != nil || !empty.Outcome.Value.Complete() || empty.Outcome.Value.RowsCopy() == nil || len(empty.Outcome.Value.ColumnsCopy()) != 1 {
		t.Fatal("empty successful result lost field metadata")
	}
	if _, err := empty.Outcome.Value.First(); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("native no-rows identity lost")
	}
	parameterized := queryResult(t, fixture.database, "parameters", "SELECT $1::text", "parameter-canary")
	first, err := parameterized.Outcome.Value.First()
	if err != nil || string(first.ValuesCopy()[0]) != "parameter-canary" {
		t.Fatal("native parameter binding failed", parameterized.Err())
	}
	if peer.connects.Load() != 1 || peer.queries.Load() != 3 {
		t.Fatal("pool reuse or attempt oracle changed")
	}
	drain(t, fixture.inbox, 3)
}
func TestQueryPartialLateErrorAndIndependentEvidence(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	for _, test := range []struct{ id, sql, state string }{{"partial", "SELECT partial", "22012"}, {"late", "SELECT late", "23514"}} {
		result := queryResult(t, fixture.database, test.id, test.sql)
		var native *pgconn.PgError
		if !errors.As(result.Err(), &native) || native.Code != test.state || result.Outcome.Value.Complete() ||
			result.Outcome.Value.RowsRead() != 1 || len(result.Outcome.Value.RowsCopy()) != 1 {
			t.Fatal("late native failure or partial data lost")
		}
		conformance.Private(t, result.Err(), "native-cause-canary", "private-detail-canary", "late-cause-canary")
		if _, err := result.Outcome.Value.First(); !errors.Is(err, ErrState) {
			t.Fatal("incomplete query became complete first-row success")
		}
	}
	if fixture.inbox.Usage().Outstanding != 2 {
		t.Fatal("handled errors erased independent evidence")
	}
	if receipt, err := fixture.database.Query(context.Background(), correlation("saturated"), "SELECT cells"); receipt != nil || !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("full inbox admitted native work")
	}
	if peer.queries.Load() != 2 {
		t.Fatal("evidence saturation reached server")
	}
	drain(t, fixture.inbox, 2)
}
func TestQueryBoundsPreserveDrainFailure(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.MaxRows = 1
	fixture := bindFixture(t, options, 1)
	result := queryResult(t, fixture.database, "bounded", "SELECT over")
	var native *pgconn.PgError
	if !errors.Is(result.Outcome.Primary, ErrLimit) || !errors.As(result.Outcome.Cleanup, &native) || native.Code != "22012" ||
		result.Outcome.Value.Complete() || len(result.Outcome.Value.RowsCopy()) != 1 || result.Outcome.Value.RowsRead() != 2 {
		t.Fatal("result limit replaced native drainage evidence")
	}
	drain(t, fixture.inbox, 1)
}
func TestProtocolMessageAndResultByteLimits(t *testing.T) {
	for _, wire := range []bool{false, true} {
		peer := newProtocolPeer(t, false)
		options := peer.options()
		if wire {
			options.MaxMessageBytes = 1024
		} else {
			options.MaxResultBytes = 1024
		}
		fixture := bindFixture(t, options, 1)
		result := queryResult(t, fixture.database, "large", "SELECT large")
		if result.Err() == nil || result.Outcome.Value.Complete() {
			t.Fatal("oversize result was accepted")
		}
		if wire {
			var native *pgproto3.ExceededMaxBodyLenErr
			if !errors.As(result.Err(), &native) {
				t.Fatal("wire-size native cause lost", result.Err())
			}
		} else if !errors.Is(result.Err(), ErrLimit) {
			t.Fatal("retained-byte limit was not enforced")
		}
		drain(t, fixture.inbox, 1)
	}
}
func TestQueryCancellationJoinsNativeConnection(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 1)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("caller-cancellation-canary")
	done := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, err := fixture.database.Query(ctx, correlation("cancel"), "SELECT wait")
		if err != nil {
			t.Error(err)
		}
		done <- receipt
	}()
	select {
	case <-peer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("query did not enter native peer")
	}
	cancel(cause)
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("native cancellation/cleanup did not finish")
	}
	result := operationResult(t, receipt, nil)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) {
		t.Fatal("cancellation causes were discarded")
	}
	if fixture.database.owner.native.Stat().TotalResources() != 0 {
		t.Fatal("canceled connection retained or replaced before cleanup")
	}
	drain(t, fixture.inbox, 1)
}

func TestConcurrentCallsRemainBoundedAndCorrectlyAttributed(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.MaxConnections, options.QueuedCalls = 4, 8
	options.Timeout = 3 * time.Second
	fixture := bindFixture(t, options, 16)
	var sequence atomic.Uint64
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 16 {
				id := fmt.Sprintf("call-%d", sequence.Add(1))
				result := queryResult(t, fixture.database, id, "SELECT $1::text", id)
				row, err := result.Outcome.Value.First()
				if err != nil || result.Err() != nil || result.Context.Correlation.Call != id || string(row.ValuesCopy()[0]) != id {
					t.Error("concurrent direct result crossed call attribution")
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				record, err := fixture.inbox.Next(ctx)
				if err != nil {
					cancel()
					t.Error(err)
					return
				}
				evidence, err := record.Receipt().WaitReleased(ctx)
				cancel()
				if err != nil {
					t.Error(err)
					return
				}
				evidenceRow, err := evidence.Outcome.Value.First()
				if err != nil || string(evidenceRow.ValuesCopy()[0]) != evidence.Context.Correlation.Call {
					t.Error("independent evidence crossed call attribution")
					return
				}
				if err := record.Release(); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	workers.Wait()
	if sequence.Load() != 128 || peer.queries.Load() != 128 || peer.connects.Load() > 4 ||
		fixture.assembly.Snapshot().Sources[0].Usage != (resource.Usage{}) || fixture.inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("concurrent workload exceeded bounds or retained ownership")
	}
}
func TestCallerTransportTraceHooksAreNotInvoked(t *testing.T) {
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 1)
	var calls atomic.Int32
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		ConnectStart: func(string, string) { calls.Add(1) }, ConnectDone: func(string, string, error) { calls.Add(1) },
	})
	receipt, err := fixture.database.Query(ctx, correlation("trace"), "SELECT cells")
	if result := operationResult(t, receipt, err); result.Err() != nil || calls.Load() != 0 {
		t.Fatal("native dialing exposed a caller transport hook", result.Err())
	}
	drain(t, fixture.inbox, 1)
}

func TestArgumentReservationBoundsNativeTextEncoding(t *testing.T) {
	for _, test := range []struct {
		value       any
		reservation int
	}{
		{int64(math.MaxInt64), 32}, {math.SmallestNonzeroFloat64, 512}, {float32(math.SmallestNonzeroFloat32), 64},
	} {
		var native sdk.ExtendedQueryBuilder
		if err := native.Build(pgtype.NewMap(), nil, []any{test.value}); err != nil {
			t.Fatal(err)
		}
		if len(native.ParamValues[0]) > test.reservation {
			t.Fatal("native scalar exceeds its declared text reservation")
		}
		oversized := []any{strings.Repeat("x", MaxArgumentBytes-8), test.value}
		if err := validStatement("SELECT $1,$2", oversized); !errors.Is(err, ErrLimit) {
			t.Fatal("argument contract: Go memory width was mistaken for native text bytes")
		}
		bounded := []any{strings.Repeat("x", MaxArgumentBytes-test.reservation), test.value}
		if err := validStatement("SELECT $1,$2", bounded); err != nil {
			t.Fatal("conservative exact reservation was rejected")
		}
	}
}

func TestExecBoundsAndColumnLimits(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.MaxRows = 1
	options.MaxResultBytes = 1024
	fixture := bindFixture(t, options, 1)
	for _, test := range []struct {
		sql     string
		args    []any
		limited bool
		late    bool
	}{
		{"SELECT $1::text", []any{strings.Repeat("x", 1019)}, false, false},
		{"SELECT large", nil, true, false}, {"SELECT over", nil, true, true},
		{"SELECT columns64", nil, false, false}, {"SELECT columns65", nil, true, false},
	} {
		receipt, err := fixture.database.Exec(context.Background(), correlation("exec-bound"), test.sql, test.args...)
		result := operationResult(t, receipt, err)
		if errors.Is(result.Outcome.Primary, ErrLimit) != test.limited || result.Outcome.Value.Complete() == test.limited ||
			result.Outcome.Value.RowsCopy() != nil {
			t.Fatal("Exec bounded consumption contract was bypassed")
		}
		if test.late {
			var native *pgconn.PgError
			if !errors.As(result.Outcome.Cleanup, &native) || native.Code != "22012" || result.Outcome.Value.RowsRead() != 2 {
				t.Fatal("Exec dropped limit witness or native drain failure")
			}
		}
		drain(t, fixture.inbox, 1)
	}
	for _, count := range []int{64, 65} {
		result := queryResult(t, fixture.database, "query-columns", fmt.Sprintf("SELECT columns%d", count))
		if errors.Is(result.Err(), ErrLimit) != (count == 65) || result.Outcome.Value.Complete() != (count == 64) {
			t.Fatal("Query column bound was not enforced")
		}
		drain(t, fixture.inbox, 1)
	}
}
func TestRetainedResultFitsIndependentStructuralAccounting(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.MaxRows, options.MaxResultBytes, options.MaxMessageBytes = 8192, 1024, 2048
	options.Timeout = 10 * time.Second
	fixture := bindFixture(t, options, 1)
	result := queryResult(t, fixture.database, "wide", "SELECT wide_nulls")
	data := result.Outcome.Value.data
	if result.Err() != nil || !result.Outcome.Value.Complete() || len(data.rows) != 8192 || len(data.columns) != 64 {
		t.Fatal("structural accounting workload changed")
	}
	retained := int64(reflect.TypeFor[resultData]().Size()) +
		int64(cap(data.rows))*int64(reflect.TypeFor[Row]().Size()) +
		int64(cap(data.columns))*int64(reflect.TypeFor[Column]().Size()) +
		int64(len(data.command)+len(data.serverVersion))
	for _, column := range data.columns {
		retained += int64(len(column.Name))
	}
	for _, row := range data.rows {
		retained += int64(cap(row.cells)) * int64(reflect.TypeFor[cell]().Size())
		for _, cell := range row.cells {
			if !cell.null {
				t.Fatal("NULL fixture changed")
			}
			retained += int64(len(cell.text))
		}
	}
	cellOnly := int64(options.MaxResultBytes+2*options.MaxMessageBytes) + int64(options.MaxRows)*64*int64(reflect.TypeFor[cell]().Size()) + 64<<10
	if retained > fixture.inbox.Usage().ReservedBytes || retained <= cellOnly {
		t.Fatal("retained result exceeds reservation, or defective cell-only control no longer distinguishes the workload")
	}
	drain(t, fixture.inbox, 1)
}
func TestRowGrowthStructuralAllowance(t *testing.T) {
	var rows []Row
	width := int64(reflect.TypeFor[Row]().Size())
	for count := 1; count <= 65536; count++ {
		old := cap(rows)
		rows = append(rows, Row{})
		retained := int64(cap(rows)) * width
		peak := retained
		if cap(rows) != old {
			peak += int64(old) * width
		}
		if retained > int64(count)*2*24+64<<10 || peak > int64(count)*4*24+64<<10 {
			t.Fatal("toolchain row growth exceeds declared structural allowances")
		}
	}
}
