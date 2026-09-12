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
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	sdk "github.com/go-sql-driver/mysql"
)

func TestPreparedQueriesAndOwnedTLS(t *testing.T) {
	for _, mode := range []struct {
		name      string
		tls, full bool
	}{{"plain", false, false}, {"tls-fast", true, false}, {"tls-full", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			peer := newPeer(t, mode.tls, mode.full)
			f := bindFixture(t, peer.options(), 1)
			for _, test := range []struct {
				query string
				args  []any
				want  [][]byte
			}{
				{"SELECT cells", nil, [][]byte{nil, {}, []byte("value-canary")}},
				{"SELECT ?", []any{"parameter-canary"}, [][]byte{[]byte("parameter-canary")}},
				{"SELECT ?", []any{nil}, [][]byte{nil}},
				{"SELECT unsigned", nil, [][]byte{[]byte("18446744073709551615")}},
				{"SELECT decimal", nil, [][]byte{[]byte("12345678901234567890.00100")}},
				{"SELECT zero_date", nil, [][]byte{[]byte("0000-00-00 00:00:00")}},
				{"SELECT json", nil, [][]byte{[]byte("{\"exact\":18446744073709551615}")}},
			} {
				receipt, err := f.db.Query(context.Background(), correlation("query"), test.query, test.args...)
				result := observe(t, receipt, err)
				if result.Err() != nil {
					t.Fatalf("query failed (%s): %v", test.query, result.Err())
				}
				row, err := result.Outcome.Value.First()
				if err != nil || !reflect.DeepEqual(row.ValuesCopy(), test.want) {
					t.Fatalf("native cell representation changed for %s", test.query)
				}
				values := row.ValuesCopy()
				for _, cell := range values {
					if len(cell) > 0 {
						cell[0] = 'X'
					}
				}
				if !reflect.DeepEqual(row.ValuesCopy(), test.want) {
					t.Fatal("copy mutated independent evidence")
				}
				conformance.Runtime(t, result.Outcome.Value, new(Result), "value-canary", "parameter-canary")
				drain(t, f.inbox, 1)
			}
			receipt, err := f.db.Query(context.Background(), correlation("empty"), "SELECT empty")
			result := observe(t, receipt, err)
			if result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.RowsCopy() == nil {
				t.Fatal("empty query lost completeness")
			}
			if _, err = result.Outcome.Value.First(); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("no-row identity lost")
			}
			drain(t, f.inbox, 1)
			if mode.tls && peer.secure.Load() != 1 {
				t.Fatal("queries did not reuse the verified TLS connection")
			}
		})
	}
}
func TestStatementBoundsAndPartialEvidence(t *testing.T) {
	peer := newPeer(t, true, false)
	options := peer.options()
	options.MaxRows = 1
	options.MaxResultBytes = 1024
	options.MaxPacketBytes = 8192
	f := bindFixture(t, options, 1)
	for _, test := range []struct {
		query string
		kind  error
		rows  int
	}{
		{"SELECT over", ErrLimit, 2}, {"SELECT wide", ErrLimit, 0}, {"SELECT large", ErrLimit, 1}, {"SELECT huge_header", ErrLimit, 0}, {"SELECT partial", ErrQuery, 1},
	} {
		receipt, err := f.db.Query(context.Background(), correlation("bounded"), test.query)
		result := observe(t, receipt, err)
		if !errors.Is(result.Err(), test.kind) || result.Outcome.Value.Complete() || result.Outcome.Value.RowsRead() != test.rows {
			t.Fatalf("wrong bounded/partial outcome for %s: %v, rows=%d", test.query, result.Err(), result.Outcome.Value.RowsRead())
		}
		if test.query == "SELECT partial" {
			var native *sdk.MySQLError
			if !errors.As(result.Err(), &native) || native.Number != 1213 || string(native.SQLState[:]) != "40001" {
				t.Fatal("original native code/state lost")
			}
			conformance.Private(t, result.Err(), "native-error-canary")
		}
		drain(t, f.inbox, 1)
	}
	receipt, err := f.db.Query(context.Background(), correlation("replacement"), "SELECT cells")
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("failed connection was not replaced", result.Err())
	}
	drain(t, f.inbox, 1)
}
func TestExecUsesPreparedSingleConnection(t *testing.T) {
	peer := newPeer(t, true, true)
	f := bindFixture(t, peer.options(), 1)
	receipt, err := f.db.Exec(context.Background(), correlation("exec"), "INSERT INTO fixture VALUES (?)", "payload")
	result := observe(t, receipt, err)
	affected, known := result.Outcome.Value.RowsAffected()
	id, idKnown := result.Outcome.Value.LastInsertID()
	if result.Err() != nil || !result.Outcome.Value.Complete() || !known || affected != 1 || !idKnown || id != 7 || peer.queries.Load() != 1 {
		t.Fatal("native execution evidence changed", result.Err())
	}
	drain(t, f.inbox, 1)
}
