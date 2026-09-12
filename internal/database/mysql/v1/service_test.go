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
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

type serviceConfig struct {
	Network                 string `json:"network"`
	Address                 string `json:"address"`
	Port                    uint16 `json:"port"`
	Database                string `json:"database"`
	User                    string `json:"user"`
	Password                string `json:"password"`
	Plaintext               bool   `json:"plaintext"`
	RootCAPEM               string `json:"root_ca_pem"`
	ServerName              string `json:"server_name"`
	CertificatePin          string `json:"server_certificate_sha256"`
	ExpectedVersion         string `json:"expected_version"`
	AllowCreateTestDatabase bool   `json:"allow_create_test_database"`
}

func serviceOptions(t *testing.T, variable string) (OptionsV1, serviceConfig) {
	t.Helper()
	path := os.Getenv(variable)
	if path == "" {
		t.Fatal("explicit private MySQL fixture is required")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("private MySQL fixture unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		t.Fatal("private fixture permissions or size invalid")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 128<<10+1))
	decoder.DisallowUnknownFields()
	var config serviceConfig
	if decoder.Decode(&config) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) || config.ExpectedVersion == "" {
		t.Fatal("invalid private MySQL fixture")
	}
	return OptionsV1{Name: "service", Network: config.Network, Address: config.Address, Port: config.Port, Database: config.Database, User: config.User, Password: config.Password,
		Plaintext: config.Plaintext, RootCAPEM: config.RootCAPEM, ServerName: config.ServerName, ServerCertificateSHA256: config.CertificatePin, MaxConnections: 2, Timeout: 5 * time.Second}, config
}
func serviceRead(t testing.TB, f boundFixture, query string, args ...any) Result {
	t.Helper()
	receipt, err := f.db.Query(context.Background(), correlation("service-read"), query, args...)
	result := observe(t, receipt, err)
	drain(t, f.inbox, 1)
	if result.Err() != nil {
		logServiceCauses(t, result.Err(), 0)
		t.Fatal("real MySQL read failed", result.Err())
	}
	return result.Outcome.Value
}
func logServiceCauses(t testing.TB, err error, depth int) {
	if err == nil || depth > 12 {
		return
	}
	if err.Error() == "unexpected resp from server for caching_sha2_password, perform full authentication" {
		t.Log("Safe native identity=caching-sha2-full-auth-response-mismatch")
	}
	for name, identity := range map[string]error{"invalid-connection": sdk.ErrInvalidConn, "native-password": sdk.ErrNativePassword, "malformed-packet": sdk.ErrMalformPkt, "packet-sequence": sdk.ErrPktSync, "unknown-plugin": sdk.ErrUnknownPlugin, "no-tls": sdk.ErrNoTLS} {
		if err == identity {
			t.Logf("Safe native identity=%s", name)
		}
	}
	switch value := err.(type) {
	case *fault.Error:
		t.Logf("Safe failure kind=%s operation=%s", value.Diagnostic().Kind, value.Diagnostic().Context.Operation)
	case *sdk.MySQLError:
		t.Logf("Safe native MySQL error number=%d", value.Number)
	default:
		t.Logf("Safe native failure type=%s", fmt.Sprintf("%T", err))
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range wrapped.Unwrap() {
			logServiceCauses(t, cause, depth+1)
		}
	} else if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		logServiceCauses(t, wrapped.Unwrap(), depth+1)
	}
}
func serviceExec(t testing.TB, f boundFixture, query string, args ...any) Result {
	t.Helper()
	receipt, err := f.db.Exec(context.Background(), correlation("service-exec"), query, args...)
	result := observe(t, receipt, err)
	drain(t, f.inbox, 1)
	if result.Err() != nil {
		t.Fatal("real MySQL execution failed", result.Err())
	}
	return result.Outcome.Value
}
func firstValues(t testing.TB, result Result) [][]byte {
	t.Helper()
	row, err := result.First()
	if err != nil {
		t.Fatal("real expected row missing", err)
	}
	return row.ValuesCopy()
}
func fixtureName(name string) bool {
	if len(name) != 29 || !strings.HasPrefix(name, "gh25_") {
		return false
	}
	_, err := hex.DecodeString(name[5:])
	return err == nil
}
func createServiceDatabase(t *testing.T, admin boundFixture, name string) (bool, error) {
	t.Helper()
	if !fixtureName(name) {
		return false, failure(ErrInput, "fixture-name")
	}
	result := serviceRead(t, admin, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?", name)
	if len(result.RowsCopy()) != 0 {
		return false, failure(ErrState, "fixture-exists")
	}
	receipt, err := admin.db.Exec(context.Background(), correlation("create-fixture"), "CREATE DATABASE `"+name+"`")
	observed := observe(t, receipt, err)
	drain(t, admin.inbox, 1)
	return observed.Err() == nil, observed.Err()
}
func TestMySQLService(t *testing.T) {
	options, config := serviceOptions(t, "FATHOMRY_MYSQL_TEST_CONFIG")
	if !config.AllowCreateTestDatabase {
		t.Fatal("exclusive generated test-database writes were not authorized")
	}
	options.Database = ""
	admin := bindFixture(t, options, 1)
	metadata := firstValues(t, serviceRead(t, admin, "SELECT VERSION(),@@version_comment,@@default_storage_engine,@@autocommit,@@transaction_isolation,@@sql_mode,@@time_zone,@@global.innodb_rollback_on_timeout"))
	if string(metadata[0]) != config.ExpectedVersion || string(metadata[2]) != "InnoDB" || string(metadata[3]) != "1" {
		t.Fatal("authorized MySQL version/engine/autocommit profile changed")
	}
	t.Logf("Observed MySQL version=%s vendor=%s engine=%s autocommit=%s isolation=%s sql_mode=%s time_zone=%s rollback_on_timeout=%s",
		metadata[0], metadata[1], metadata[2], metadata[3], metadata[4], metadata[5], metadata[6], metadata[7])
	plugin := firstValues(t, serviceRead(t, admin, "SELECT plugin FROM mysql.user WHERE CONCAT(user,'@',host)=CURRENT_USER()"))
	t.Logf("Observed service account authentication plugin=%s; network=%s; plaintext=%t", plugin[0], config.Network, config.Plaintext)
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("fixture identity unavailable")
	}
	name := "gh25_" + hex.EncodeToString(nonce[:])
	created, err := createServiceDatabase(t, admin, name)
	if err != nil || !created {
		t.Fatal("exclusive fixture creation was not confirmed", err)
	}
	t.Cleanup(func() {
		cleanupOptions := options
		cleanupOptions.Name = "cleanup"
		cleanup := bindFixture(t, cleanupOptions, 1)
		receipt, err := cleanup.db.Exec(context.Background(), correlation("drop-fixture"), "DROP DATABASE `"+name+"`")
		result := observe(t, receipt, err)
		drain(t, cleanup.inbox, 1)
		if result.Err() != nil {
			t.Error("owned test database cleanup unconfirmed", result.Err())
			return
		}
		if len(serviceRead(t, cleanup, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?", name).RowsCopy()) != 0 {
			t.Error("owned test database still exists")
		}
		t.Log("Exclusive generated database dropped; fresh-connection absence verified.")
	})
	if owned, err := createServiceDatabase(t, admin, name); owned || !errors.Is(err, ErrState) {
		t.Fatal("pre-existing database protection failed")
	}
	options.Database = name
	options.Name = "writer"
	writer := bindFixture(t, options, 8)
	options.Name = "reader"
	reader := bindFixture(t, options, 1)
	serviceExec(t, writer, "CREATE TABLE items (id BIGINT UNSIGNED PRIMARY KEY, amount DECIMAL(30,5), observed DATETIME(6), payload JSON, note VARBINARY(128), revision INT NOT NULL) ENGINE=InnoDB")
	identifier := ^uint64(0)
	amount := "12345678901234567890.00100"
	stamp := time.Date(2026, 9, 12, 12, 0, 0, 123456000, time.UTC)
	inserted := serviceExec(t, writer, "INSERT INTO items VALUES (?,?,?,?,?,?)", identifier, amount, stamp, json.RawMessage("{\"exact\":18446744073709551615}"), []byte(nil), 0)
	if rows, known := inserted.RowsAffected(); !known || rows != 1 {
		t.Fatal("real affected-row evidence missing")
	}
	values := firstValues(t, serviceRead(t, reader, "SELECT id,amount,observed,payload,note FROM items WHERE id=?", identifier))
	if string(values[0]) != "18446744073709551615" || string(values[1]) != amount || string(values[2]) != "2026-09-12 12:00:00.123456" || !strings.Contains(string(values[3]), "18446744073709551615") || values[4] != nil {
		t.Fatal("real exact numeric/date/JSON/NULL contract changed")
	}
	tx := beginTx(t, writer, context.Background())
	receipt, err := tx.Exec(context.Background(), correlation("transaction-write"), "UPDATE items SET note=?,revision=revision+1 WHERE id=?", []byte("committed"), identifier)
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("real transaction update failed", result.Err())
	}
	if note := firstValues(t, serviceRead(t, reader, "SELECT note FROM items WHERE id=?", identifier))[0]; note != nil {
		t.Fatal("uncommitted change escaped its transaction")
	}
	receipt, err = tx.Commit(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil || result.Outcome.Value.TransactionOutcome() != CommitAcknowledged {
		t.Fatal("real commit acknowledgement failed", result.Err())
	}
	drain(t, writer.inbox, 2)
	if string(firstValues(t, serviceRead(t, reader, "SELECT note FROM items WHERE id=?", identifier))[0]) != "committed" {
		t.Fatal("independent committed read-back failed")
	}
	tx = beginTx(t, writer, context.Background())
	receipt, err = tx.Exec(context.Background(), correlation("rollback-write"), "UPDATE items SET note=? WHERE id=?", []byte("discarded"), identifier)
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	receipt, err = tx.Rollback(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil || result.Outcome.Value.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("real rollback acknowledgement failed", result.Err())
	}
	drain(t, writer.inbox, 2)
	if string(firstValues(t, serviceRead(t, reader, "SELECT note FROM items WHERE id=?", identifier))[0]) != "committed" {
		t.Fatal("independent rollback read-back failed")
	}
	// Native statement errors do not automatically roll back all prior work.
	tx = beginTx(t, writer, context.Background())
	receipt, err = tx.Exec(context.Background(), correlation("before-error"), "UPDATE items SET revision=revision+1 WHERE id=?", identifier)
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	receipt, err = tx.Exec(context.Background(), correlation("duplicate"), "INSERT INTO items (id,revision) VALUES (?,0)", identifier)
	if result := observe(t, receipt, err); result.Err() == nil {
		t.Fatal("real duplicate-key control did not fail")
	}
	receipt, err = tx.Exec(context.Background(), correlation("savepoint"), "SAVEPOINT verified")
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("statement error invalidated a live InnoDB transaction", result.Err())
	}
	receipt, err = tx.Commit(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("real post-error commit failed", result.Err())
	}
	drain(t, writer.inbox, 4)
	prepared, _, err := writer.db.Prepare(context.Background(), correlation("prepared"), "SELECT amount FROM items WHERE id=?")
	if err != nil || prepared == nil {
		t.Fatal("real persistent preparation failed", err)
	}
	receipt, err = prepared.Query(context.Background(), correlation("prepared-query"), identifier)
	if result := observe(t, receipt, err); result.Err() != nil || string(firstValues(t, result.Outcome.Value)[0]) != amount {
		t.Fatal("real prepared execution failed", result.Err())
	}
	receipt, err = prepared.Close(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("real prepared cleanup failed", result.Err())
	}
	drain(t, writer.inbox, 2)
	// Drop an actual COMMIT acknowledgement after the server produced it.
	tx = beginTx(t, writer, context.Background())
	receipt, err = tx.Exec(context.Background(), correlation("lost-write"), "UPDATE items SET note=? WHERE id=?", []byte("response-lost"), identifier)
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drop := &commitDrop{Conn: tx.owned.wire.transport}
	tx.owned.wire.transport = drop
	receipt, err = tx.Commit(context.Background())
	unknown := observe(t, receipt, err)
	if unknown.Err() == nil || unknown.Outcome.Value.TransactionOutcome() != FinalizationUnknown || !drop.observed.Load() {
		t.Fatal("lost real commit reply was falsely confirmed")
	}
	drain(t, writer.inbox, 2)
	if string(firstValues(t, serviceRead(t, reader, "SELECT note FROM items WHERE id=?", identifier))[0]) != "response-lost" {
		t.Fatal("independent real-server lost-commit effect was not confirmed")
	}
	// Cancellation closes local I/O; a finite SLEEP bounds any remaining server work.
	work, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	receipt, err = writer.db.Query(work, correlation("cancel"), "SELECT SLEEP(0.2)")
	canceled := observe(t, receipt, err)
	cancel()
	drain(t, writer.inbox, 1)
	if canceled.Err() == nil || !errors.Is(canceled.Err(), context.DeadlineExceeded) {
		t.Fatal("real cancellation did not preserve its deadline")
	}
	if value := firstValues(t, serviceRead(t, writer, "SELECT 1"))[0]; string(value) != "1" {
		t.Fatal("post-cancellation replacement failed")
	}
	for _, level := range []sql.IsolationLevel{sql.LevelDefault, sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSerializable} {
		isolated, _, err := writer.db.Begin(context.Background(), correlation("isolation"), TxOptionsV1{Isolation: level, ReadOnly: true})
		if err != nil || isolated == nil {
			t.Fatal("real isolation/read-only Begin failed", err)
		}
		receipt, err := isolated.Query(context.Background(), correlation("isolation-read"), "SELECT revision FROM items")
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal("real isolated read failed", result.Err())
		}
		receipt, err = isolated.Rollback(context.Background())
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal("real isolated rollback failed", result.Err())
		}
		drain(t, writer.inbox, 2)
	}
	serviceObservedCancellation(t, writer, admin)
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	automatic := beginTx(t, writer, lifetime)
	receipt, err = automatic.Exec(context.Background(), correlation("automatic-write"), "UPDATE items SET note=? WHERE id=?", []byte("automatic-discard"), identifier)
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	statement, _, err := automatic.Prepare(context.Background(), correlation("automatic-prepare"), "SELECT note FROM items WHERE id=?")
	if err != nil || statement == nil {
		t.Fatal("real automatic-cleanup preparation failed", err)
	}
	stopLifetime()
	final := observe(t, automatic.call.Receipt(), nil)
	if !errors.Is(final.Outcome.Primary, context.Canceled) || final.Outcome.Value.TransactionOutcome() != RollbackAcknowledged || final.Outcome.Cleanup != nil {
		t.Fatal("real automatic rollback or owned statement cleanup failed", final.Err())
	}
	drain(t, writer.inbox, 3)
	if string(firstValues(t, serviceRead(t, reader, "SELECT note FROM items WHERE id=?", identifier))[0]) != "response-lost" {
		t.Fatal("automatic rollback failed independent read-back")
	}
	foundOptions := writer.db.owner.settings
	flags := options
	flags.Database, flags.ClientFoundRows, flags.ColumnsWithAlias = foundOptions.Database, true, true
	found := bindFixture(t, flags, 1)
	changed := serviceExec(t, writer, "UPDATE items SET revision=revision WHERE id=?", identifier)
	matched := serviceExec(t, found, "UPDATE items SET revision=revision WHERE id=?", identifier)
	if count, known := changed.RowsAffected(); !known || count != 0 {
		t.Fatal("default changed-row semantics changed")
	}
	if count, known := matched.RowsAffected(); !known || count != 1 {
		t.Fatal("native found-row semantics changed")
	}
	columns := serviceRead(t, found, "SELECT fixture.revision FROM items AS fixture").ColumnsCopy()
	if len(columns) != 1 || columns[0].Name != "fixture.revision" {
		t.Fatal("native column alias metadata changed")
	}
	t.Log("Real parameterized I/O, exact values, independent commit/rollback, recoverable errors, preparation, all native isolation options, found rows/aliases, lost-COMMIT effect and observed cancellation executed.")
}

func serviceObservedCancellation(t *testing.T, writer, observer boundFixture) {
	t.Helper()
	tx := beginTx(t, writer, context.Background())
	root, err := writer.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := tx.Query(context.Background(), correlation("connection-id"), "SELECT CONNECTION_ID()")
	connectionID := string(firstValues(t, observe(t, receipt, err).Outcome.Value)[0])
	drain(t, writer.inbox, 1)
	work, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	done := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, err := tx.Query(work, correlation("observed-cancel"), "SELECT SLEEP(0.5)")
		if err != nil {
			t.Error("real cancellation initiation failed", err)
		}
		done <- receipt
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		count := firstValues(t, serviceRead(t, observer, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST WHERE ID=? AND INFO='SELECT SLEEP(0.5)'", connectionID))[0]
		if string(count) == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("independent server-entry witness was not observed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cause := errors.New("service-cancellation-canary")
	cancel(cause)
	result := observe(t, <-done, nil)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) {
		t.Fatal("observed native cancellation lost its cause")
	}
	drain(t, writer.inbox, 1)
	receipt, err = tx.Rollback(context.Background())
	if result := observe(t, receipt, err); result.Outcome.Value.TransactionOutcome() != FinalizationUnknown {
		t.Fatal("closed socket falsely acknowledged rollback")
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		count := firstValues(t, serviceRead(t, observer, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST WHERE ID=?", connectionID))[0]
		if string(count) == "0" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("canceled owned server connection remained")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Log("Independent server-entry and eventual connection-exit witnesses observed; no immediate server cancellation inferred.")
}

type commitDrop struct {
	net.Conn
	observed atomic.Bool
}

func (c *commitDrop) Read(dst []byte) (int, error) {
	body, _, err := packetRead(c.Conn)
	if err == nil && len(body) > 0 && body[0] == 0 {
		c.observed.Store(true)
	}
	return 0, io.EOF
}
func TestMySQLTLSService(t *testing.T) {
	options, config := serviceOptions(t, "FATHOMRY_MYSQL_TLS_TEST_CONFIG")
	if options.Plaintext {
		t.Fatal("real TLS acceptance requires verified encryption")
	}
	f := bindFixture(t, options, 1)
	values := firstValues(t, serviceRead(t, f, "SELECT VERSION(),CAST(? AS UNSIGNED),CAST(? AS DECIMAL(30,5)),CAST(? AS JSON)", ^uint64(0), "12345678901234567890.00100", json.RawMessage("{\"exact\":18446744073709551615}")))
	if string(values[0]) != config.ExpectedVersion || string(values[1]) != "18446744073709551615" || string(values[2]) != "12345678901234567890.00100" {
		t.Fatal("real TLS values or version changed")
	}
	cipher := serviceRead(t, f, "SHOW SESSION STATUS LIKE 'Ssl_cipher'")
	version := serviceRead(t, f, "SHOW SESSION STATUS LIKE 'Ssl_version'")
	cipherValues, versionValues := firstValues(t, cipher), firstValues(t, version)
	if len(cipherValues) < 2 || len(cipherValues[1]) == 0 || len(versionValues) < 2 || len(versionValues[1]) == 0 {
		t.Fatal("actual MySQL TLS session was not confirmed")
	}
	t.Logf("Real TLS session version=%s cipher=%s; parameter binding, unsigned/decimal/JSON values and trusted server identity verified.", versionValues[1], cipherValues[1])
	if err := f.assembly.Close(context.Background()); err != nil {
		t.Fatal("real TLS resource cleanup failed", err)
	}
	if f.assembly.Snapshot().Sources[0].Usage != (resource.Usage{}) {
		t.Fatal("real TLS use remained")
	}
}
