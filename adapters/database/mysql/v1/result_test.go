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

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestEvidenceAndFinalizationAtSaturation(t *testing.T) {
	peer := newPeer(t, false, false)
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

func TestExactOrdinaryAndPreparedResults(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		name := "ordinary"
		if prepared {
			name = "prepared"
		}
		t.Run(name, func(t *testing.T) {
			peer := newPeer(t, false, false)
			owner, inbox, _ := testOwner(t, peer.options(), 0)
			ctx := context.Background()
			for _, example := range []struct{ query, want, kind string }{
				{"SELECT unsigned", "18446744073709551615", "UNSIGNED BIGINT"},
				{"SELECT decimal", "12345678901234567890.00100", "DECIMAL"},
				{"SELECT zero_date", "0000-00-00 00:00:00", "DATETIME"},
				{"SELECT json", "{\"exact\":18446744073709551615}", "JSON"},
			} {
				var value Result
				var err error
				if prepared {
					statement, prepareErr := owner.Client().Prepare(ctx, example.query)
					if prepareErr != nil {
						t.Fatal(prepareErr)
					}
					value, err = statement.Query(ctx)
					ack(t, inbox)
					if _, err := statement.Close(ctx); err != nil {
						t.Fatal(err)
					}
					ack(t, inbox)
				} else {
					value, err = owner.Client().Query(ctx, example.query)
					ack(t, inbox)
				}
				if err != nil {
					t.Fatal(err)
				}
				row, err := value.First()
				if err != nil || string(row.ValuesCopy()[0]) != example.want || value.ColumnsCopy()[0].DatabaseType != example.kind {
					t.Fatal("native exact value or metadata changed")
				}
				if _, known := value.LastInsertID(); known {
					t.Fatal("query invented insert metadata")
				}
			}
		})
	}
}

func TestParseTimeRepresentation(t *testing.T) {
	peer := newPeer(t, false, false)
	settings := peer.options()
	settings.ParseTime = true
	owner, inbox, _ := testOwner(t, settings, 0)
	value, err := owner.Client().Query(context.Background(), "SELECT zero_date")
	if err != nil {
		t.Fatal(err)
	}
	row, err := value.First()
	if err != nil || string(row.ValuesCopy()[0]) != "0001-01-01T00:00:00Z" {
		t.Fatal("configured native time representation changed")
	}
	ack(t, inbox)
}

func TestStatementEffectMetadata(t *testing.T) {
	peer := newPeer(t, false, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	ctx := context.Background()
	verify := func(value Result, err error) {
		t.Helper()
		affected, affectedKnown := value.RowsAffected()
		inserted, insertedKnown := value.LastInsertID()
		if err != nil || !value.Complete() || value.RowsCopy() != nil || !affectedKnown || affected != 1 || !insertedKnown || inserted != 7 || value.TransactionOutcome() != TransactionUnobserved {
			t.Fatal("native affected/insert metadata changed", err)
		}
		ack(t, inbox)
	}
	verify(owner.Client().Exec(ctx, "INSERT INTO fixture VALUES (?)", "ordinary"))
	statement, err := owner.Client().Prepare(ctx, "INSERT INTO fixture VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	verify(statement.Exec(ctx, "prepared"))
	if _, err = statement.Close(ctx); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	transaction, err := owner.Client().Begin(ctx, txOptions())
	if err != nil {
		t.Fatal(err)
	}
	verify(transaction.Exec(ctx, "INSERT INTO fixture VALUES (?)", "transaction"))
	prepared, err := transaction.Prepare(ctx, "INSERT INTO fixture VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	verify(prepared.Exec(ctx, "transaction-prepared"))
	if _, err = transaction.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	ack(t, inbox)
}

func TestResultBoundRetainsIndependentDrainFailure(t *testing.T) {
	peer := newPeer(t, false, false)
	settings := peer.options()
	settings.MaxResultBytes = 1024
	settings.MaxPacketBytes = 8192
	owner, inbox, _ := testOwner(t, settings, 0)
	value, err := owner.Client().Query(context.Background(), "SELECT drain_error")
	native, found := InspectError(err)
	if !errors.Is(err, ErrLimit) || !found || native.Number != 1213 || value.Complete() || value.RowsRead() != 1 || len(value.RowsCopy()) != 0 {
		t.Fatal("result bound lost independent drain failure", err)
	}
	if _, err := value.First(); !errors.Is(err, ErrState) {
		t.Fatal("partial result became complete first row", err)
	}
	ack(t, inbox)
}
