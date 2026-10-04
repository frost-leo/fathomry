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

package mysql

import (
	"context"
	"errors"
	"testing"
)

func TestSequentialPreparedFinalization(t *testing.T) {
	peer := newPeer(t, false, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	transaction, err := owner.Client().Begin(context.Background(), txOptions())
	if err != nil {
		t.Fatal(err)
	}
	for iteration := range 512 {
		statement, err := transaction.Prepare(context.Background(), "SELECT cells")
		if err != nil {
			t.Fatalf("prepare %d: %v", iteration, err)
		}
		if _, err := statement.Close(context.Background()); err != nil {
			t.Fatalf("close %d: %v", iteration, err)
		}
		if _, err := transaction.Query(context.Background(), "SELECT cells"); err != nil {
			t.Fatalf("sequential query %d: %v", iteration, err)
		}
		ack(t, inbox)
		ack(t, inbox)
	}
	if _, err := transaction.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
}

func TestTransactionPreparationSlots(t *testing.T) {
	peer := newPeer(t, false, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	ctx := context.Background()
	transaction, err := owner.Client().Begin(ctx, txOptions())
	if err != nil {
		t.Fatal(err)
	}
	statements := make([]*Statement, MaxPreparedStatements)
	for index := range statements {
		statements[index], err = transaction.Prepare(ctx, "SELECT ?")
		if err != nil {
			t.Fatal(err)
		}
	}
	if statement, err := transaction.Prepare(ctx, "SELECT ?"); statement != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("transaction preparation bound exceeded", err)
	}
	ack(t, inbox)
	for _, statement := range statements {
		copy := *statement
		if _, err = copy.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if snapshot, _ := statement.Receipt().Snapshot(); !snapshot.Info().Released {
			t.Fatal("copied statement split ownership")
		}
		ack(t, inbox)
	}
	for range 80 {
		statement, err := transaction.Prepare(ctx, "SELECT ?")
		if err != nil {
			t.Fatal("released preparation slots accumulated", err)
		}
		if _, err = statement.Close(ctx); err != nil {
			t.Fatal(err)
		}
		ack(t, inbox)
	}
	if _, err = transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
}
