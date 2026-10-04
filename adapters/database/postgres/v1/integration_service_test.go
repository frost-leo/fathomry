//go:build postgres_service

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
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	sdk "github.com/jackc/pgx/v5"
)

type serviceConfig struct {
	Settings                Settings `json:"settings"`
	AllowCreateTestDatabase bool     `json:"allow_create_test_database"`
	ExpectedVersion         string   `json:"expected_version"`
	RecoveryFile            string   `json:"recovery_file"`
}

func serviceConfiguration(t *testing.T, variable string) serviceConfig {
	t.Helper()
	path := os.Getenv(variable)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("explicit private service fixture unavailable")
	}
	defer file.Close()
	metadata, err := file.Stat()
	if err != nil || !metadata.Mode().IsRegular() || metadata.Mode().Perm()&0077 != 0 || metadata.Size() > 128<<10 {
		t.Fatal("invalid private fixture permissions or size")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 128<<10+1))
	decoder.DisallowUnknownFields()
	var config serviceConfig
	if decoder.Decode(&config) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) ||
		!config.AllowCreateTestDatabase || config.ExpectedVersion == "" || config.RecoveryFile == "" ||
		config.Settings.Plaintext || config.Settings.RootCAPEM == "" {
		t.Fatal("explicit authorization, expected version, recovery file and verified TLS are required")
	}
	if err := Validate(config.Settings); err != nil {
		t.Fatal("private service settings are invalid")
	}
	return config
}
func serviceName(t *testing.T, recovery, prefix string) string {
	t.Helper()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("fixture identity unavailable")
	}
	name := prefix + hex.EncodeToString(nonce[:])
	file, err := os.OpenFile(recovery, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal("exclusive recovery record unavailable")
	}
	_, writeErr := file.WriteString(name + "\n")
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal("recovery identity could not be retained")
	}
	return name
}

// Cleanup authority requires acknowledged creation. One early absent read cannot
// prove an unacknowledged remote CREATE will not finish later.
func cleanupServiceFixture(ctx context.Context, owned bool, recovery string, drop func(context.Context) error, absent func(context.Context) (bool, error)) error {
	if !owned {
		return errors.New("creation outcome unconfirmed; preserve private recovery record")
	}
	if err := drop(ctx); err != nil {
		return err
	}
	gone, err := absent(ctx)
	if err != nil {
		return err
	}
	if !gone {
		return errors.New("fixture absence unconfirmed; preserve private recovery record")
	}
	return os.Remove(recovery)
}

func TestServiceFixtureRecovery(t *testing.T) {
	for _, test := range []struct {
		name                                        string
		owned, dropFails, absent, readFails, remove bool
	}{
		{name: "unknown_create_even_if_initially_absent", absent: true},
		{name: "unknown_drop", owned: true, dropFails: true, absent: true},
		{name: "still_present", owned: true},
		{name: "oracle_unavailable", owned: true, absent: true, readFails: true},
		{name: "confirmed_cleanup", owned: true, absent: true, remove: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recovery")
			if err := os.WriteFile(path, []byte("private-fixture"), 0600); err != nil {
				t.Fatal(err)
			}
			dropped, inspected := false, false
			err := cleanupServiceFixture(context.Background(), test.owned, path, func(context.Context) error {
				dropped = true
				if test.dropFails {
					return errors.New("drop unconfirmed")
				}
				return nil
			}, func(context.Context) (bool, error) {
				inspected = true
				if test.readFails {
					return false, errors.New("oracle unavailable")
				}
				return test.absent, nil
			})
			if (err == nil) != test.remove || dropped != test.owned || inspected != (test.owned && !test.dropFails) {
				t.Fatal("cleanup authority or terminal evidence changed")
			}
			_, statErr := os.Stat(path)
			if errors.Is(statErr, os.ErrNotExist) != test.remove {
				t.Fatal("private recovery responsibility was lost")
			}
		})
	}
}

func serviceExec(t *testing.T, client *Client, inbox *adapters.Inbox[Result], ctx context.Context, query string, args ...any) Result {
	t.Helper()
	value, err := client.Exec(ctx, query, args...)
	ack(t, inbox)
	if err != nil {
		t.Fatal("public service execution failed", err)
	}
	return value
}
func serviceQuery(t *testing.T, client *Client, inbox *adapters.Inbox[Result], ctx context.Context, query string, args ...any) Result {
	t.Helper()
	value, err := client.Query(ctx, query, args...)
	ack(t, inbox)
	if err != nil {
		t.Fatal("public service query failed", err)
	}
	return value
}

func serviceOracle(t *testing.T, ctx context.Context, settings Settings) *sdk.Conn {
	t.Helper()
	config, err := sdk.ParseConfig("host=/fathomry-parser-only port=5432 user=fixture password=unused dbname=fixture sslmode=disable")
	if err != nil {
		t.Fatal("independent reader configuration failed")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(settings.RootCAPEM)) {
		t.Fatal("independent reader trust unavailable")
	}
	config.Host, config.Port, config.Database = settings.Address, settings.Port, settings.Database
	config.User, config.Password = settings.User, settings.Password
	config.Fallbacks = nil
	config.TLSConfig = &tls.Config{RootCAs: roots, ServerName: settings.ServerName, MinVersion: tls.VersionTLS12}
	config.ConnectTimeout = 5 * time.Second
	config.DefaultQueryExecMode = sdk.QueryExecModeExec
	connection, err := sdk.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("independent verified-TLS reader connection failed")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if connection.Close(ctx) != nil {
			t.Error("independent reader cleanup failed")
		}
	})
	return connection
}

func TestPublicPostgreSQLService(t *testing.T) {
	config := serviceConfiguration(t, "FATHOMRY_POSTGRES_ADAPTER_CONFIG")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, inbox, _ := testOwner(t, config.Settings, 0)
	oracleAdmin := serviceOracle(t, ctx, config.Settings)
	var version string
	var encrypted bool
	if err := oracleAdmin.QueryRow(ctx, "SELECT current_setting('server_version'), COALESCE((SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()),false)").Scan(&version, &encrypted); err != nil || version != config.ExpectedVersion || !encrypted {
		t.Fatal("actual version or TLS differs from authorized fixture")
	}
	name := serviceName(t, config.RecoveryFile, "gh102_pg_")
	var count int
	if err := oracleAdmin.QueryRow(ctx, "SELECT count(*) FROM pg_database WHERE datname=$1", name).Scan(&count); err != nil || count != 0 {
		t.Fatal("exclusive fixture absence unconfirmed")
	}
	owned := false
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		err := cleanupServiceFixture(cleanup, owned, config.RecoveryFile, func(cleanup context.Context) error {
			value, err := admin.Client().Exec(cleanup, "DROP DATABASE "+name)
			ack(t, inbox)
			if err != nil || !value.Complete() {
				return errors.New("owned fixture DROP unconfirmed; preserve private recovery record")
			}
			return nil
		}, func(cleanup context.Context) (bool, error) {
			if err := oracleAdmin.QueryRow(cleanup, "SELECT count(*) FROM pg_database WHERE datname=$1", name).Scan(&count); err != nil {
				return false, errors.New("independent absence check failed; preserve private recovery record")
			}
			return count == 0, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("Unique PostgreSQL fixture absence independently verified after cleanup.")
	})
	created, err := admin.Client().Exec(ctx, "CREATE DATABASE "+name)
	owned = created.Complete() && created.CommandTag() == "CREATE DATABASE"
	ack(t, inbox)
	if err != nil || !owned {
		t.Fatal("fixture creation unconfirmed; recovery record retained")
	}
	settings := config.Settings
	settings.Database = name
	owner, records, _ := testOwner(t, settings, 0)
	client := owner.Client()
	oracle := serviceOracle(t, ctx, settings)
	if value, err := client.Ping(ctx); err != nil || !value.Complete() {
		t.Fatal("public readiness failed", err)
	}
	ack(t, records)
	selected := serviceQuery(t, client, records, ctx, "SELECT current_database()")
	selectedRow, err := selected.First()
	if err != nil || string(selectedRow.ValuesCopy()[0]) != name {
		t.Fatal("public connection did not select its exclusive fixture")
	}
	serviceExec(t, client, records, ctx, "CREATE TABLE entries (id bigint PRIMARY KEY,payload text NOT NULL,note text)")
	value := serviceExec(t, client, records, ctx, "INSERT INTO entries VALUES ($1,$2,$3)", int64(1), "initial", nil)
	if value.RowsAffected() != 1 {
		t.Fatal("affected-row metadata lost")
	}
	result := serviceQuery(t, client, records, ctx, "SELECT payload,note,''::text FROM entries WHERE id=$1", int64(1))
	row, err := result.First()
	if err != nil || string(row.ValuesCopy()[0]) != "initial" || row.ValuesCopy()[1] != nil || row.ValuesCopy()[2] == nil {
		t.Fatal("read or NULL/empty distinction failed")
	}
	exact := serviceQuery(t, client, records, ctx, "SELECT 18446744073709551615::numeric,12345678901234567890.00100::numeric,'2026-01-02 03:04:05.123456+00'::timestamptz")
	exactRow, err := exact.First()
	if err != nil || string(exactRow.ValuesCopy()[0]) != "18446744073709551615" || string(exactRow.ValuesCopy()[1]) != "12345678901234567890.00100" || string(exactRow.ValuesCopy()[2]) != "2026-01-02 03:04:05.123456+00" {
		t.Fatal("exact numeric/time representation changed")
	}

	setup, stopSetup := context.WithCancel(ctx)
	statement, err := client.Prepare(setup, "SELECT payload FROM entries WHERE id=$1")
	if err != nil {
		t.Fatal("standalone preparation failed", err)
	}
	stopSetup()
	for range 2 {
		value, err := statement.Query(ctx, int64(1))
		if err != nil || !value.Complete() {
			t.Fatal("retained preparation failed", err)
		}
		ack(t, records)
	}
	if _, err := statement.Close(ctx); err != nil {
		t.Fatal("preparation cleanup failed", err)
	}
	ack(t, records)

	readPayload := func(expected string) {
		t.Helper()
		var actual string
		if err := oracle.QueryRow(ctx, "SELECT payload FROM entries WHERE id=1").Scan(&actual); err != nil || actual != expected {
			t.Fatal("independent effect read-back failed")
		}
	}
	insidePayload := func(transaction *Transaction, expected string) {
		t.Helper()
		value, err := transaction.Query(ctx, "SELECT payload FROM entries WHERE id=1")
		ack(t, records)
		if err != nil {
			t.Fatal("transaction query failed", err)
		}
		row, err := value.First()
		if err != nil || string(row.ValuesCopy()[0]) != expected {
			t.Fatal("transaction-local effect read-back failed")
		}
	}
	setup, stopSetup = context.WithCancel(ctx)
	transaction, err := client.Begin(setup, TxOptions{Isolation: ReadCommitted, Access: ReadWrite})
	if err != nil {
		t.Fatal(err)
	}
	stopSetup()
	prepared, err := transaction.Prepare(ctx, "UPDATE entries SET payload=$1 WHERE id=$2")
	if err != nil {
		t.Fatal(err)
	}
	if value, err := prepared.Exec(ctx, "committed", int64(1)); err != nil || value.RowsAffected() != 1 {
		t.Fatal("transaction preparation failed", err)
	}
	ack(t, records)
	insidePayload(transaction, "committed")
	readPayload("initial")
	committed, err := transaction.Commit(ctx)
	if err != nil || committed.TransactionOutcome() != CommitAcknowledged {
		t.Fatal("commit evidence failed", err)
	}
	ack(t, records)
	ack(t, records)
	readPayload("committed")
	transaction, err = client.Begin(ctx, TxOptions{Isolation: ReadCommitted, Access: ReadWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(ctx, "UPDATE entries SET payload=$1 WHERE id=1", "discarded"); err != nil {
		t.Fatal(err)
	}
	ack(t, records)
	insidePayload(transaction, "discarded")
	readPayload("committed")
	if value, err := transaction.Rollback(ctx); err != nil || value.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("rollback evidence failed", err)
	}
	ack(t, records)
	readPayload("committed")

	transaction, err = client.Begin(ctx, TxOptions{Isolation: ReadCommitted, Access: ReadWrite})
	if err != nil {
		t.Fatal(err)
	}
	point, err := transaction.Savepoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(ctx, "UPDATE entries SET payload=$1 WHERE id=1", "savepoint-discarded"); err != nil {
		t.Fatal(err)
	}
	ack(t, records)
	insidePayload(transaction, "savepoint-discarded")
	readPayload("committed")
	if value, err := point.Rollback(ctx); err != nil || value.SavepointOutcome() != SavepointRolledBack {
		t.Fatal("savepoint rollback failed", err)
	}
	ack(t, records)
	insidePayload(transaction, "committed")
	point, err = transaction.Savepoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if value, err := point.Release(ctx); err != nil || value.SavepointOutcome() != SavepointReleased {
		t.Fatal("savepoint release failed", err)
	}
	ack(t, records)
	point, err = transaction.Savepoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	ack(t, records)
	ack(t, records)
	pointSnapshot, _ := point.Receipt().Snapshot()
	pointResult, _ := pointSnapshot.ValueCopy()
	if pointResult.SavepointOutcome() != SavepointParentEnded {
		t.Fatal("parent savepoint settlement missing")
	}
	readPayload("committed")

	transaction, err = client.Begin(ctx, TxOptions{Isolation: ReadCommitted, Access: ReadWrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(ctx, "INSERT INTO entries VALUES (1,'duplicate',NULL)"); err == nil {
		t.Fatal("duplicate violation hidden")
	}
	ack(t, records)
	aborted, err := transaction.Commit(ctx)
	if !errors.Is(err, sdk.ErrTxCommitRollback) || aborted.TransactionOutcome() != CommitRolledBack {
		t.Fatal("aborted commit became an acknowledged commit", err)
	}
	ack(t, records)
	readPayload("committed")
	partial, err := client.Query(ctx, "SELECT 100/(17-n) FROM generate_series(1,32) AS n")
	ack(t, records)
	if server, ok := InspectError(err); !ok || server.Code != "22012" || partial.Complete() || partial.RowsRead() != 16 {
		t.Fatal("partial output or server error lost")
	}
	if _, err := client.Stats(ctx); err != nil {
		t.Fatal(err)
	}
	if profile, err := client.Profile(ctx); err != nil || profile.Protocol.Value != "postgresql-3.0" {
		t.Fatal("effective profile missing", err)
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		t.Fatal("actual source cleanup unconfirmed", err)
	}
	t.Logf("PostgreSQL %s: verified TLS; public readiness, parameterized I/O, exact values, preparation, commit/rollback, savepoints, aborted commit and independent read-back passed.", version)
}
