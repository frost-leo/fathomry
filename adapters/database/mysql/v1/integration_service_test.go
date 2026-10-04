//go:build mysql_service

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
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	sdk "github.com/go-sql-driver/mysql"
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

func serviceOracle(t *testing.T, ctx context.Context, settings Settings) *sql.DB {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(settings.RootCAPEM)) {
		t.Fatal("independent reader trust unavailable")
	}
	trust := &tls.Config{RootCAs: roots, ServerName: settings.ServerName, MinVersion: tls.VersionTLS12}
	if settings.ServerCertificateSHA256 != "" {
		pin, err := hex.DecodeString(settings.ServerCertificateSHA256)
		if err != nil || len(pin) != sha256.Size {
			t.Fatal("independent reader pin invalid")
		}
		trust.InsecureSkipVerify = true
		trust.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("reader certificate missing")
			}
			leaf := state.PeerCertificates[0]
			digest := sha256.Sum256(leaf.Raw)
			if string(digest[:]) != string(pin) {
				return errors.New("reader certificate pin mismatch")
			}
			intermediates := x509.NewCertPool()
			for _, certificate := range state.PeerCertificates[1:] {
				intermediates.AddCert(certificate)
			}
			_, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
			return err
		}
	}
	config := sdk.NewConfig()
	config.Net, config.Addr, config.DBName = settings.Network, settings.Address, settings.Database
	config.User, config.Passwd = settings.User, settings.Password
	config.TLS = trust
	config.Timeout, config.ReadTimeout, config.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
	config.Logger = &sdk.NopLogger{}
	connector, err := sdk.NewConnector(config)
	if err != nil {
		t.Fatal("independent reader configuration failed")
	}
	database := sql.OpenDB(connector)
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if database.Close() != nil {
			t.Error("independent reader cleanup failed")
		}
	})
	if database.PingContext(ctx) != nil {
		t.Fatal("independent verified-TLS reader connection failed")
	}
	return database
}

func TestPublicMySQLService(t *testing.T) {
	config := serviceConfiguration(t, "FATHOMRY_MYSQL_ADAPTER_CONFIG")
	if config.Settings.Network != "unix" {
		t.Fatal("this authorized write fixture requires a Unix socket")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, inbox, _ := testOwner(t, config.Settings, 0)
	oracleAdmin := serviceOracle(t, ctx, config.Settings)
	var version, engine string
	if err := oracleAdmin.QueryRowContext(ctx, "SELECT VERSION(),@@default_storage_engine").Scan(&version, &engine); err != nil || version != config.ExpectedVersion || engine != "InnoDB" {
		t.Fatal("actual version/engine differs from authorized fixture")
	}
	var key, cipher string
	if err := oracleAdmin.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&key, &cipher); err != nil || cipher == "" {
		t.Fatal("actual oracle TLS missing")
	}
	name := serviceName(t, config.RecoveryFile, "gh102_my_")
	var count int
	if err := oracleAdmin.QueryRowContext(ctx, "SELECT count(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?", name).Scan(&count); err != nil || count != 0 {
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
			if err := oracleAdmin.QueryRowContext(cleanup, "SELECT count(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?", name).Scan(&count); err != nil {
				return false, errors.New("independent absence check failed; preserve private recovery record")
			}
			return count == 0, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("Unique MySQL fixture absence independently verified after cleanup.")
	})
	created, err := admin.Client().Exec(ctx, "CREATE DATABASE "+name)
	owned = created.Complete()
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
	selected := serviceQuery(t, client, records, ctx, "SELECT DATABASE()")
	selectedRow, err := selected.First()
	if err != nil || string(selectedRow.ValuesCopy()[0]) != name {
		t.Fatal("public connection did not select its exclusive fixture")
	}
	serviceExec(t, client, records, ctx, "CREATE TABLE entries (id bigint PRIMARY KEY AUTO_INCREMENT,payload varchar(128) NOT NULL,note text) ENGINE=InnoDB")
	value := serviceExec(t, client, records, ctx, "INSERT INTO entries (payload,note) VALUES (?,?)", "initial", nil)
	if affected, known := value.RowsAffected(); !known || affected != 1 {
		t.Fatal("affected-row metadata lost")
	}
	id, known := value.LastInsertID()
	if !known || id != 1 {
		t.Fatal("insert identity/presence lost")
	}
	result := serviceQuery(t, client, records, ctx, "SELECT payload,note,'' FROM entries WHERE id=?", int64(1))
	row, err := result.First()
	if err != nil || string(row.ValuesCopy()[0]) != "initial" || row.ValuesCopy()[1] != nil || row.ValuesCopy()[2] == nil {
		t.Fatal("read or NULL/empty distinction failed")
	}
	exact := serviceQuery(t, client, records, ctx, "SELECT CAST(18446744073709551615 AS UNSIGNED),CAST('12345678901234567890.00100' AS DECIMAL(25,5)),CAST('2026-01-02 03:04:05.123456' AS DATETIME(6))")
	exactRow, err := exact.First()
	if err != nil || string(exactRow.ValuesCopy()[0]) != "18446744073709551615" || string(exactRow.ValuesCopy()[1]) != "12345678901234567890.00100" || string(exactRow.ValuesCopy()[2]) != "2026-01-02 03:04:05.123456" {
		t.Fatal("exact numeric/time representation changed")
	}
	tlsRows := serviceQuery(t, client, records, ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'")
	tlsRow, err := tlsRows.First()
	if err != nil || len(tlsRow.ValuesCopy()[1]) == 0 {
		t.Fatal("public connection TLS missing")
	}

	setup, stopSetup := context.WithCancel(ctx)
	statement, err := client.Prepare(setup, "SELECT payload FROM entries WHERE id=?")
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
		if err := oracle.QueryRowContext(ctx, "SELECT payload FROM entries WHERE id=1").Scan(&actual); err != nil || actual != expected {
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
	transaction, err := client.Begin(ctx, TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := transaction.Prepare(ctx, "UPDATE entries SET payload=? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	value, err = prepared.Exec(ctx, "committed", int64(1))
	if affected, known := value.RowsAffected(); err != nil || !known || affected != 1 {
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
	transaction, err = client.Begin(ctx, TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(ctx, "UPDATE entries SET payload=? WHERE id=1", "discarded"); err != nil {
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

	transaction, err = client.Begin(ctx, TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(ctx, "INSERT INTO entries VALUES (1,'duplicate',NULL)"); err == nil {
		t.Fatal("duplicate violation hidden")
	} else if server, ok := InspectError(err); !ok || server.Number != 1062 {
		t.Fatal("native violation identity lost")
	}
	ack(t, records)
	if _, err := transaction.Exec(ctx, "UPDATE entries SET payload=? WHERE id=1", "after-statement-error"); err != nil {
		t.Fatal("statement-only failure ended transaction", err)
	}
	ack(t, records)
	insidePayload(transaction, "after-statement-error")
	readPayload("committed")
	if value, err := transaction.Commit(ctx); err != nil || value.TransactionOutcome() != CommitAcknowledged {
		t.Fatal("commit after statement-only error failed", err)
	}
	ack(t, records)
	readPayload("after-statement-error")

	transaction, err = client.Begin(ctx, TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(ctx, "UPDATE entries SET payload=? WHERE id=1", "ddl-committed"); err != nil {
		t.Fatal(err)
	}
	ack(t, records)
	insidePayload(transaction, "ddl-committed")
	readPayload("after-statement-error")
	if _, err := transaction.Exec(ctx, "CREATE TABLE implicit_end (id bigint PRIMARY KEY) ENGINE=InnoDB"); err != nil {
		t.Fatal("owned DDL failed", err)
	}
	ack(t, records)
	readPayload("ddl-committed")
	if _, err := transaction.Commit(ctx); !errors.Is(err, ErrState) {
		t.Fatal("server-ended transaction accepted commit", err)
	}
	if value, err := transaction.Rollback(ctx); err != nil || value.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("local finalization after implicit commit failed", err)
	}
	ack(t, records)
	readPayload("ddl-committed")

	lifetime, stopLifetime := context.WithCancel(ctx)
	transaction, err = client.Begin(lifetime, TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(ctx, "UPDATE entries SET payload=? WHERE id=1", "canceled-write"); err != nil {
		t.Fatal(err)
	}
	ack(t, records)
	insidePayload(transaction, "canceled-write")
	readPayload("ddl-committed")
	stopLifetime()
	snapshot, err := transaction.Receipt().WaitReleased(ctx)
	observed, _ := snapshot.ValueCopy()
	if err != nil || !errors.Is(snapshot.Primary(), context.Canceled) || observed.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("native lifetime rollback evidence missing", err)
	}
	ack(t, records)
	readPayload("ddl-committed")
	if _, err := client.Stats(ctx); err != nil {
		t.Fatal(err)
	}
	if profile, err := client.Profile(ctx); err != nil || profile.Protocol.Value != "mysql-41" {
		t.Fatal("effective profile missing", err)
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		t.Fatal("actual source cleanup unconfirmed", err)
	}
	t.Logf("MySQL %s: verified TLS/InnoDB; public readiness, parameterized I/O, exact values, preparation, commit/rollback, statement-only errors, DDL implicit commit, lifetime rollback and independent read-back passed.", version)
}
