//go:build schema_service

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

package fathomry_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io/fs"
	"testing"
	"time"

	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
)

func postgresTransaction(t *testing.T, fixture sqlFixture, isolation postgres.Isolation) *postgres.Transaction {
	t.Helper()
	setup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	transaction, err := fixture.postgresClient.Begin(setup, postgres.TxOptions{Isolation: isolation, Access: postgres.ReadWrite})
	cancel()
	if err != nil || transaction == nil {
		t.Fatal("PostgreSQL transaction unavailable", err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, _ = transaction.Rollback(cleanup)
		snapshot, err := transaction.Receipt().WaitReleased(cleanup)
		if err != nil || !snapshot.Info().Released || snapshot.Cleanup() != nil {
			t.Error("PostgreSQL transaction cleanup incomplete", err, snapshot.Cleanup())
		}
	})
	return transaction
}

func postgresScalar(t *testing.T, transaction *postgres.Transaction, query string, args ...any) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	value, err := transaction.Query(ctx, query, args...)
	if err != nil || !value.Complete() {
		t.Fatal("retained PostgreSQL read failed", err)
	}
	row, err := value.First()
	if err != nil {
		t.Fatal(err)
	}
	cells := row.ValuesCopy()
	if len(cells) != 1 || cells[0] == nil {
		t.Fatal("expected one non-NULL PostgreSQL cell")
	}
	return string(cells[0])
}

func postgresLockFixture(t *testing.T) (sqlFixture, sqlFixture, string, int64) {
	t.Helper()
	first, second := sqlPostgres(t), sqlPostgres(t)
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	name := "fathomry_transaction_" + hex.EncodeToString(random[:])
	first.target = `"` + name + `"`
	second.target = first.target
	first.execute(t, "CREATE SCHEMA "+first.target)
	t.Cleanup(func() {
		first.execute(t, "DROP SCHEMA "+first.target+" CASCADE")
		if first.scalar(t, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=$1", name) != "0" {
			t.Error("owned transaction fixture survived cleanup")
		}
	})
	return first, second, name, int64(binary.BigEndian.Uint64(random[:]))
}

func TestPostgresTransactionalMigrationOwnership(t *testing.T) {
	first, second, name, lock := postgresLockFixture(t)
	owner := postgresTransaction(t, first, postgres.ReadCommitted)
	contender := postgresTransaction(t, second, postgres.ReadCommitted)
	if postgresScalar(t, owner, "SELECT pg_try_advisory_xact_lock($1)", lock) != "t" ||
		postgresScalar(t, contender, "SELECT pg_try_advisory_xact_lock($1)", lock) != "f" {
		t.Fatal("transaction lock did not exclude the competing connection")
	}
	apply := func(transaction *postgres.Transaction) {
		t.Helper()
		for _, step := range readManifest(t, first.assets).Migrations[0].Steps {
			raw, err := fs.ReadFile(first.assets, step.Path)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			value, err := transaction.Exec(ctx, first.statement(string(raw)))
			cancel()
			if err != nil || !value.Complete() {
				t.Fatal("initial asset is not transaction-compatible", step.ID, err)
			}
		}
	}
	apply(owner)
	tables := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=$1 AND table_type='BASE TABLE'"
	if postgresScalar(t, owner, tables, name) != "9" || postgresScalar(t, contender, tables, name) != "0" {
		t.Fatal("uncommitted catalog visibility is incorrect")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if value, err := owner.Rollback(ctx); err != nil || value.TransactionOutcome() != postgres.RollbackAcknowledged {
		t.Fatal("transactional schema rollback not acknowledged", err)
	}
	if postgresScalar(t, contender, "SELECT pg_try_advisory_xact_lock($1)", lock) != "t" ||
		postgresScalar(t, contender, tables, name) != "0" {
		t.Fatal("rollback retained its lock or left partial tables")
	}
	apply(contender)
	if value, err := contender.Commit(ctx); err != nil || value.TransactionOutcome() != postgres.CommitAcknowledged {
		t.Fatal("transactional schema commit not acknowledged", err)
	}
	if first.scalar(t, tables, name) != "9" ||
		first.scalar(t, "SELECT pg_try_advisory_xact_lock($1)", lock) != "t" {
		t.Fatal("committed catalog or lock release is unavailable")
	}
	verifyStoredComments(t, first)
	t.Log("all 25 initial assets rolled back together, then committed with retained transaction locking; setup cancellation did not revoke the transaction")
}

func TestPostgresMigrationLockDoesNotRefreshSnapshot(t *testing.T) {
	first, second, _, lock := postgresLockFixture(t)
	first.execute(t, "CREATE TABLE "+first.target+".checkpoint (revision BIGINT NOT NULL)")
	first.execute(t, "INSERT INTO "+first.target+".checkpoint VALUES (0)")
	for _, isolation := range []postgres.Isolation{postgres.RepeatableRead, postgres.ReadCommitted} {
		t.Run(string(isolation), func(t *testing.T) {
			first.execute(t, "UPDATE "+first.target+".checkpoint SET revision=0")
			writer := postgresTransaction(t, first, postgres.ReadCommitted)
			reader := postgresTransaction(t, second, isolation)
			if postgresScalar(t, writer, "SELECT pg_try_advisory_xact_lock($1)", lock) != "t" {
				t.Fatal("writer did not acquire lock")
			}
			query := "SELECT revision FROM " + first.target + ".checkpoint"
			if postgresScalar(t, reader, query) != "0" ||
				postgresScalar(t, reader, "SELECT pg_try_advisory_xact_lock($1)", lock) != "f" {
				t.Fatal("initial snapshot or exclusion is incorrect")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := writer.Exec(ctx, "UPDATE "+first.target+".checkpoint SET revision=1"); err != nil {
				t.Fatal(err)
			}
			if value, err := writer.Commit(ctx); err != nil || value.TransactionOutcome() != postgres.CommitAcknowledged {
				t.Fatal("writer commit not acknowledged", err)
			}
			if postgresScalar(t, reader, "SELECT pg_try_advisory_xact_lock($1)", lock) != "t" {
				t.Fatal("reader did not acquire the released lock")
			}
			want := "1"
			if isolation == postgres.RepeatableRead {
				want = "0"
			}
			if postgresScalar(t, reader, query) != want {
				t.Fatal("snapshot baseline changed")
			}
		})
	}
}
