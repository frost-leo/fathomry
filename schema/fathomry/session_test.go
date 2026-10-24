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
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
)

func mysqlSession(t *testing.T, fixture sqlFixture, lifetime context.Context) *mysql.Session {
	t.Helper()
	retained, err := fixture.mysqlClient.Retain(lifetime, mysql.SessionOptions{Timeout: time.Minute})
	if err != nil || retained == nil {
		t.Fatal("retained MySQL session unavailable", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = retained.Close(ctx)
		snapshot, err := retained.Receipt().WaitReleased(ctx)
		if err != nil || snapshot.Cleanup() != nil || !snapshot.Info().Released {
			t.Error("session did not positively retire", err, snapshot.Cleanup())
		}
	})
	return retained
}

func sessionScalar(t *testing.T, retained *mysql.Session, query string, args ...any) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	value, err := retained.Query(ctx, query, args...)
	if err != nil || !value.Complete() {
		t.Fatal("session query failed", err)
	}
	row, err := value.First()
	if err != nil {
		t.Fatal(err)
	}
	cells := row.ValuesCopy()
	if len(cells) != 1 || cells[0] == nil {
		t.Fatal("expected exactly one non-NULL session value")
	}
	return string(cells[0])
}

func sessionExec(t *testing.T, retained *mysql.Session, statement string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	value, err := retained.Exec(ctx, statement, args...)
	if err != nil || !value.Complete() || value.TransactionOutcome() != mysql.TransactionUnobserved {
		t.Fatal("session statement failed or fabricated transaction evidence", err)
	}
}

func sessionName(t *testing.T) string {
	t.Helper()
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	return "fathomry_155_" + hex.EncodeToString(random[:])
}

func TestMySQLRetainedMigrationSession(t *testing.T) {
	document := maximumSpecification(t)
	first := sqlMySQL(t, mysqlDocumentSettings)
	second := sqlMySQL(t)
	if !strings.HasPrefix(first.scalar(t, "SELECT VERSION()"), "8.4.") {
		t.Fatal("MySQL8.4 required")
	}
	name := sessionName(t)
	table, marker := "catalog_test."+name, "catalog_test."+name+"_ddl"
	first.execute(t, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	t.Cleanup(func() {
		first.execute(t, "DROP TABLE IF EXISTS "+marker)
		first.execute(t, "DROP TABLE "+table)
	})
	retained := mysqlSession(t, first, context.Background())
	contender := mysqlSession(t, second, context.Background())
	identity := sessionScalar(t, retained, "SELECT CONNECTION_ID()")
	if sessionScalar(t, retained, "SELECT GET_LOCK(?,0)", name) != "1" {
		t.Fatal("migration lock not acquired")
	}
	if sessionScalar(t, contender, "SELECT GET_LOCK(?,0)", name) != "0" {
		t.Fatal("competing session bypassed exclusion")
	}
	sessionExec(t, retained, "START TRANSACTION")
	sessionExec(t, retained, "INSERT INTO "+table+" VALUES (1)")
	if sessionScalar(t, contender, "SELECT COUNT(*) FROM "+table) != "0" {
		t.Fatal("uncommitted control was visible")
	}
	sessionExec(t, retained, "CREATE TABLE "+marker+" (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	if sessionScalar(t, contender, "SELECT COUNT(*) FROM "+table) != "1" {
		t.Fatal("DDL implicit commit control did not occur")
	}
	if sessionScalar(t, retained, "SELECT CONNECTION_ID()") != identity || sessionScalar(t, contender, "SELECT IS_USED_LOCK(?)", name) != identity {
		t.Fatal("DDL lost the original connection/lock")
	}
	if sessionScalar(t, contender, "SELECT GET_LOCK(?,0)", name) != "0" {
		t.Fatal("DDL released session-scoped exclusion")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	value, err := retained.Query(ctx, "SELECT ?", document)
	if err != nil {
		t.Fatal("retained document transport failed", err)
	}
	row, err := value.First()
	if err != nil {
		t.Fatal(err)
	}
	checkDocumentBytes(t, "mysql", row.ValuesCopy()[0], document)
	if err := retained.Close(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := retained.Receipt().Snapshot()
	closed, _ := snapshot.ValueCopy()
	if closed.HasData() || closed.TransactionOutcome() != mysql.TransactionUnobserved {
		t.Fatal("local close became a commit acknowledgement")
	}
	if sessionScalar(t, contender, "SELECT GET_LOCK(?,1)", name) != "1" {
		t.Fatal("retired session kept its remote lock")
	}
	if sessionScalar(t, contender, "SELECT RELEASE_LOCK(?)", name) != "1" {
		t.Fatal("explicit release was not acknowledged")
	}
	if first.scalar(t, "SELECT CONNECTION_ID()") == identity {
		t.Fatal("closed session was reused from the pool")
	}
	t.Log("same session and lock survived real implicit-commit DDL; retirement, competing acquisition and 4 MiB transport passed")
}

func TestMySQLSessionCancellationReleasesRemoteLock(t *testing.T) {
	first, second := sqlMySQL(t), sqlMySQL(t)
	lifetime, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("owned-session-stop")
	retained := mysqlSession(t, first, lifetime)
	contender := mysqlSession(t, second, context.Background())
	name := sessionName(t)
	if sessionScalar(t, retained, "SELECT GET_LOCK(?,0)", name) != "1" {
		t.Fatal("lock not acquired")
	}
	cancel(cause)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	snapshot, err := retained.Receipt().WaitReleased(ctx)
	if err != nil || !errors.Is(snapshot.Primary(), cause) {
		t.Fatal("cancellation ownership/cause lost", err, snapshot.Primary())
	}
	if sessionScalar(t, contender, "SELECT GET_LOCK(?,1)", name) != "1" {
		t.Fatal("canceled session retained its lock")
	}
	if sessionScalar(t, contender, "SELECT RELEASE_LOCK(?)", name) != "1" {
		t.Fatal("contender release failed")
	}
}

func TestMySQLSessionDoesNotReconnectAfterServerKill(t *testing.T) {
	first, second := sqlMySQL(t), sqlMySQL(t)
	retained := mysqlSession(t, first, context.Background())
	contender := mysqlSession(t, second, context.Background())
	identity := sessionScalar(t, retained, "SELECT CONNECTION_ID()")
	number, err := strconv.ParseUint(identity, 10, 32)
	if err != nil || number == 0 {
		t.Fatal("invalid owned connection identity")
	}
	sessionExec(t, contender, "KILL CONNECTION "+strconv.FormatUint(number, 10))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := retained.Query(ctx, "SELECT 1"); err == nil {
		t.Fatal("killed session became a successful replacement")
	}
	if _, err := retained.Query(ctx, "SELECT 1"); !errors.Is(err, mysql.ErrState) {
		t.Fatal("dead retained session tried new native work", err)
	}
	if err := retained.Close(ctx); err != nil {
		t.Fatal("dead session was not retired", err)
	}
}
