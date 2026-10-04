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

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestEvidenceAndFinalizationAtSaturation(t *testing.T) {
	peer := newProtocolPeer(t, false)
	owner, inbox, _ := testOwner(t, peer.options(), 3)
	ctx := context.Background()
	statement, err := owner.Client().Prepare(ctx, "SELECT cells")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := statement.Query(ctx); err != nil {
		t.Fatal(err)
	}
	before := peer.queries.Load()
	if _, err := statement.Query(ctx); !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("evidence rejection", err)
	}
	delivery := receive(t, inbox)
	if err := delivery.Retry(); err != nil {
		t.Fatal(err)
	}
	if peer.queries.Load() != before {
		t.Fatal("delivery retry dispatched SQL")
	}
	ack(t, inbox)
	if _, err := statement.Query(ctx); err != nil {
		t.Fatal("native custody was not drained", err)
	}
	if _, err := owner.Client().Ping(ctx); err == nil {
		t.Fatal("root saturation not enforced")
	}
	if _, err := statement.Close(ctx); err != nil {
		t.Fatal("close required fresh admission", err)
	}
	ack(t, inbox)
	ack(t, inbox)
}

func TestStatementEffectsAndRepresentations(t *testing.T) {
	peer := newProtocolPeer(t, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	ctx := context.Background()
	for _, text := range []string{"18446744073709551615", "12345678901234567890.00100", "2026-10-04 01:02:03.123456+00"} {
		value, err := owner.Client().Query(ctx, "SELECT $1::text", text)
		if err != nil {
			t.Fatal(err)
		}
		row, err := value.First()
		if err != nil || string(row.ValuesCopy()[0]) != text || value.ColumnsCopy()[0].OID != 25 {
			t.Fatal("exact positional text changed")
		}
		ack(t, inbox)
	}
	value, err := owner.Client().Query(ctx, "SELECT cells")
	if err != nil {
		t.Fatal(err)
	}
	columns := value.ColumnsCopy()
	if len(columns) != 3 || columns[0].Name != columns[1].Name || len(value.RowsCopy()[0].ValuesCopy()) != len(columns) {
		t.Fatal("duplicate positional columns collapsed")
	}
	ack(t, inbox)
	verifyExec := func(value Result, err error) {
		t.Helper()
		if err != nil || !value.Complete() || value.RowsCopy() != nil || value.RowsAffected() != 1 || value.CommandTag() != "INSERT 0 1" || value.TransactionOutcome() != TransactionUnobserved {
			t.Fatal("statement acknowledgement changed", err)
		}
		ack(t, inbox)
	}
	verifyExec(owner.Client().Exec(ctx, "INSERT INTO fixture VALUES ($1)", "ordinary"))
	statement, err := owner.Client().Prepare(ctx, "INSERT INTO fixture VALUES ($1)")
	if err != nil {
		t.Fatal(err)
	}
	verifyExec(statement.Exec(ctx, "prepared"))
	if _, err = statement.Close(ctx); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	transaction, err := owner.Client().Begin(ctx, txOptions())
	if err != nil {
		t.Fatal(err)
	}
	verifyExec(transaction.Exec(ctx, "INSERT INTO fixture VALUES ($1)", "transaction"))
	prepared, err := transaction.Prepare(ctx, "INSERT INTO fixture VALUES ($1)")
	if err != nil {
		t.Fatal(err)
	}
	verifyExec(prepared.Exec(ctx, "transaction-prepared"))
	if _, err = transaction.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	ack(t, inbox)
}

func TestResultBoundRetainsIndependentDrainFailure(t *testing.T) {
	peer := newProtocolPeer(t, false)
	settings := peer.options()
	settings.MaxRows = 1
	owner, inbox, _ := testOwner(t, settings, 0)
	value, err := owner.Client().Query(context.Background(), "SELECT over")
	native, found := InspectError(err)
	if !errors.Is(err, ErrLimit) || !found || native.Code != "22012" || value.Complete() || value.RowsRead() != 2 || len(value.RowsCopy()) != 1 {
		t.Fatal("row bound lost partial data or later native failure", err)
	}
	if _, err := value.First(); !errors.Is(err, ErrState) {
		t.Fatal("partial result became complete first row", err)
	}
	ack(t, inbox)
}
