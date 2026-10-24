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

package catalog

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

func TestPublicationTransactionAdmission(t *testing.T) {
	if _, err := postgresPublication(nil, "public"); !errors.Is(err, ErrInput) {
		t.Fatal("nil transaction accepted")
	}
	if _, err := mysqlPublication(nil, "control"); !errors.Is(err, ErrInput) {
		t.Fatal("nil transaction accepted")
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		for _, target := range []string{"", "nul\x00", "line\n", "information_schema"} {
			if _, err := publicationTarget(dialect, target); !errors.Is(err, ErrInput) {
				t.Fatal("unsafe target accepted", dialect)
			}
		}
		for _, target := range []string{"control", "控制库", "old", "a\"b", "a`b"} {
			if _, err := publicationTarget(dialect, target); err != nil {
				t.Fatal("safe identifier rejected", dialect, err)
			}
		}
	}
	pg, err := postgresPublication(new(postgres.Transaction), "a\"b")
	if err != nil || pg.target != "\"a\"\"b\"" || pg.parameters(3) != "$1,$2,$3" || pg.predicate([]string{"id", "code"}) != "id=$1 AND code=$2" {
		t.Fatal("PostgreSQL SQL ownership", err)
	}
	my, err := mysqlPublication(new(mysql.Transaction), "a`b")
	if err != nil || my.target != "`a``b`" || my.parameters(3) != "?,?,?" || my.lastParameter(2) != "?" {
		t.Fatal("MySQL SQL ownership", err)
	}
	if _, err := publicationTarget("mysql", "mysql"); err == nil {
		t.Fatal("system database admitted")
	}
	if _, err := publicationTarget("mysql", "emoji😀"); err == nil {
		t.Fatal("unsupported MySQL identifier admitted")
	}
	for _, target := range []string{"pg_temp", strings.Repeat("x", 64)} {
		if _, err := publicationTarget("postgresql", target); err == nil {
			t.Fatal("unsupported PostgreSQL namespace")
		}
	}
	pg.calls = maxPublicationCalls
	if err := pg.admit(context.Background()); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded SQL work")
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("private-cancel")
	cancel(cause)
	if err := pg.admit(ctx); !errors.Is(err, ErrCanceled) || !errors.Is(err, cause) {
		t.Fatal("cancellation cause lost")
	}
	if _, err := pg.register(context.Background(), nil); !errors.Is(err, ErrHandle) {
		t.Fatal("unprepared registration accepted")
	}
}
func TestPublicationDecodingRefusesUnprovenRows(t *testing.T) {
	pg := &publicationTx{pg: new(postgres.Transaction)}
	actual, err := pg.binary([]byte("\\x0001ff"), 3)
	if err != nil || !bytes.Equal(actual, []byte{0, 1, 255}) {
		t.Fatal("bytea identity changed", err)
	}
	for _, raw := range [][]byte{[]byte("bad"), []byte("\\xzz"), []byte("\\x00010203")} {
		if _, err := pg.binary(raw, 3); !errors.Is(err, ErrStored) {
			t.Fatal("bad binary accepted", err)
		}
	}
	for _, raw := range []string{"", "0", "-1", "01", "+1", "9223372036854775808"} {
		if _, err := rowID([][]byte{[]byte(raw)}, 0); !errors.Is(err, ErrStored) {
			t.Fatal("invalid row identity admitted", err)
		}
	}
	if value, err := rowID([][]byte{[]byte("9223372036854775807")}, 0); err != nil || value != 9223372036854775807 {
		t.Fatal(err)
	}
	if _, err := rowID(nil, 0); !errors.Is(err, ErrStored) {
		t.Fatal("missing row ID fabricated")
	}
	for _, raw := range []string{"t", "1", "f", "0"} {
		if _, err := enabled([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := enabled(nil); !errors.Is(err, ErrStored) {
		t.Fatal("NULL became false")
	}
	if (publicationChanges{}).changed() || !(publicationChanges{definitionCreated: true}).changed() || !(publicationChanges{resources: 1}).changed() {
		t.Fatal("tentative change accounting")
	}
}

func TestPublicationBudgetFitsAdmittedAggregate(t *testing.T) {
	input := &registration{info: Preparation{Modes: declaration.MaxModes, Resources: MaxResources}, contents: make([]declaration.Content, declaration.MaxWorkflows+declaration.MaxModes), category: &initialCategory{}, tags: make([]tag, MaxTags)}
	if publicationCalls(input) >= maxPublicationCalls {
		t.Fatal("admitted maximum cannot fit SQL-call budget")
	}
	prepared, err := Prepare(context.Background(), registrationInput(t))
	if err != nil {
		t.Fatal(err)
	}
	tx := &publicationTx{pg: new(postgres.Transaction), target: "\"public\"", calls: maxPublicationCalls - publicationCalls(prepared.state)}
	if result, err := tx.register(context.Background(), prepared); !errors.Is(err, ErrLimit) || result.changed() || tx.used {
		t.Fatal("known excessive work reached mutation", err)
	}
}
