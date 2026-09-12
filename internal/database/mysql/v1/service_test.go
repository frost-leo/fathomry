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
	"bytes"
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
	"os/exec"
	"strconv"
	"strings"
	"sync"
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

func ownServiceCall(t *testing.T, inbox *invocation.Inbox[Result], receipt *invocation.Receipt[Result], finish func(context.Context) (*invocation.Receipt[Result], error)) *invocation.DeliveryRecord[Result] {
	t.Helper()
	var record *invocation.DeliveryRecord[Result]
	t.Cleanup(func() {
		if !finishServiceCall(t, receipt, finish) {
			return
		}
		if record != nil {
			if err := record.Release(); err != nil {
				t.Error(err)
			}
		}
	})
	var err error
	record, err = inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return record
}
func finishServiceCall(t *testing.T, receipt *invocation.Receipt[Result], finish func(context.Context) (*invocation.Receipt[Result], error)) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := finish(ctx)
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Error("service-cleanup-failure: retained handle", err)
	}
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Error("service-cleanup-failure: active handle", err)
		return false
	}
	if result.Outcome.Cleanup != nil {
		t.Error("service-cleanup-failure: independent evidence", result.Outcome.Cleanup)
	}
	return true
}
func beginServiceTransaction(t *testing.T, fixture boundFixture, ctx context.Context) *Transaction {
	t.Helper()
	tx, receipt, err := fixture.db.Begin(ctx, correlation("transaction"), TxOptionsV1{Isolation: sql.LevelReadCommitted})
	if err != nil || tx == nil {
		t.Fatal("real transaction establishment failed", err)
	}
	t.Cleanup(func() { finishServiceCall(t, receipt, tx.Rollback) })
	return tx
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
func registerServiceDatabaseCleanup(t *testing.T, options OptionsV1, name string) {
	t.Helper()
	t.Cleanup(func() {
		options.Name, options.Database = "cleanup", ""
		cleanup := bindFixture(t, options, 1)
		receipt, err := cleanup.db.Exec(context.Background(), correlation("drop-fixture"), "DROP DATABASE `"+name+"`")
		result := observe(t, receipt, err)
		drain(t, cleanup.inbox, 1)
		if result.Err() != nil {
			t.Error("owned test database cleanup unconfirmed", result.Err())
		}
		if err := cleanup.assembly.Close(context.Background()); err != nil {
			t.Error("cleanup source release failed", err)
		}
		options.Name = "cleanup-check"
		check := bindFixture(t, options, 1)
		if len(serviceRead(t, check, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?", name).RowsCopy()) != 0 {
			t.Error("owned test database still exists")
			return
		}
		t.Log("Exclusive generated database absence verified through a fresh connection after DROP.")
	})
}
func createServiceDatabase(t *testing.T, admin boundFixture, options OptionsV1, name string) (bool, error) {
	t.Helper()
	if !fixtureName(name) {
		return false, failure(ErrInput, "fixture-name")
	}
	result := serviceRead(t, admin, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?", name)
	if len(result.RowsCopy()) != 0 {
		return false, failure(ErrState, "fixture-exists")
	}
	receipt, err := admin.db.Exec(context.Background(), correlation("create-fixture"), "CREATE DATABASE `"+name+"`")
	if err != nil {
		return false, err
	}
	snapshot, _ := receipt.Result()
	created := snapshot.Outcome.Primary == nil && snapshot.Outcome.Value.Complete()
	if created {
		registerServiceDatabaseCleanup(t, options, name)
	}
	observed := observe(t, receipt, err)
	drain(t, admin.inbox, 1)
	return created, observed.Err()
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
	if !options.Plaintext {
		cipher := firstValues(t, serviceRead(t, admin, "SHOW SESSION STATUS LIKE 'Ssl_cipher'"))
		version := firstValues(t, serviceRead(t, admin, "SHOW SESSION STATUS LIKE 'Ssl_version'"))
		if len(cipher) < 2 || len(cipher[1]) == 0 || len(version) < 2 || len(version[1]) == 0 {
			t.Fatal("write gate did not establish actual TLS")
		}
		t.Logf("Actual write-gate TLS version=%s cipher=%s", version[1], cipher[1])
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("fixture identity unavailable")
	}
	name := "gh25_" + hex.EncodeToString(nonce[:])
	created, err := createServiceDatabase(t, admin, options, name)
	if err != nil || !created {
		t.Fatal("exclusive fixture creation failed; acknowledged ownership remains registered for cleanup", err)
	}
	if owned, err := createServiceDatabase(t, admin, options, name); owned || !errors.Is(err, ErrState) {
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
	tx := beginServiceTransaction(t, writer, context.Background())
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
	tx = beginServiceTransaction(t, writer, context.Background())
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
	tx = beginServiceTransaction(t, writer, context.Background())
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
	prepared, preparation, err := writer.db.Prepare(context.Background(), correlation("prepared"), "SELECT amount FROM items WHERE id=?")
	if err != nil || prepared == nil {
		t.Fatal("real persistent preparation failed", err)
	}
	preparedRoot := ownServiceCall(t, writer.inbox, preparation, prepared.Close)
	for range 2 {
		receipt, err = prepared.Query(context.Background(), correlation("prepared-query"), identifier)
		if result := observe(t, receipt, err); result.Err() != nil || string(firstValues(t, result.Outcome.Value)[0]) != amount {
			t.Fatal("real prepared reuse failed", result.Err())
		}
	}
	receipt, err = prepared.Close(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("real prepared cleanup failed", result.Err())
	}
	drain(t, writer.inbox, 2)
	if err := preparedRoot.Release(); err != nil {
		t.Fatal(err)
	}
	// Drop an actual COMMIT acknowledgement after the server produced it.
	tx = beginServiceTransaction(t, writer, context.Background())
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
		isolated, isolation, err := writer.db.Begin(context.Background(), correlation("isolation"), TxOptionsV1{Isolation: level, ReadOnly: true})
		if err != nil || isolated == nil {
			t.Fatal("real isolation/read-only Begin failed", err)
		}
		root := ownServiceCall(t, writer.inbox, isolation, isolated.Rollback)
		receipt, err := isolated.Query(context.Background(), correlation("isolation-read"), "SELECT revision FROM items")
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal("real isolated read failed", result.Err())
		}
		receipt, err = isolated.Exec(context.Background(), correlation("read-only-write"), "UPDATE items SET revision=revision+1 WHERE id=?", identifier)
		denied := observe(t, receipt, err)
		var native *sdk.MySQLError
		if !errors.As(denied.Err(), &native) || native.Number != 1792 {
			t.Fatal("read-only transaction admitted a write", denied.Err())
		}
		receipt, err = isolated.Rollback(context.Background())
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal("real isolated rollback failed", result.Err())
		}
		drain(t, writer.inbox, 2)
		if err = root.Release(); err != nil {
			t.Fatal(err)
		}
	}
	serviceObservedCancellation(t, writer, admin)
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	automatic := beginServiceTransaction(t, writer, lifetime)
	receipt, err = automatic.Exec(context.Background(), correlation("automatic-write"), "UPDATE items SET note=? WHERE id=?", []byte("automatic-discard"), identifier)
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	statement, _, err := automatic.Prepare(context.Background(), correlation("automatic-prepare"), "SELECT note FROM items WHERE id=?")
	if err != nil || statement == nil {
		t.Fatal("real automatic-cleanup preparation failed", err)
	}
	receipt, err = statement.Query(context.Background(), correlation("automatic-prepared-query"), identifier)
	if result := observe(t, receipt, err); result.Err() != nil || string(firstValues(t, result.Outcome.Value)[0]) != "automatic-discard" {
		t.Fatal("transactional native preparation did not execute", result.Err())
	}
	stopLifetime()
	final := observe(t, automatic.call.Receipt(), nil)
	if !errors.Is(final.Outcome.Primary, context.Canceled) || final.Outcome.Value.TransactionOutcome() != RollbackAcknowledged || final.Outcome.Cleanup != nil {
		t.Fatal("real automatic rollback or owned statement cleanup failed", final.Err())
	}
	drain(t, writer.inbox, 4)
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
	serviceCoreCoverage(t, options, writer, reader, identifier)
	serviceFailureCleanupControl(t, options.Database)
	t.Log("Real parameterized I/O, exact values, independent commit/rollback, recoverable errors, preparation, all native isolation options, found rows/aliases, lost-COMMIT effect and observed cancellation executed.")
}

func serviceObservedCancellation(t *testing.T, writer, observer boundFixture) {
	t.Helper()
	tx := beginServiceTransaction(t, writer, context.Background())
	root, err := writer.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := tx.Query(context.Background(), correlation("connection-id"), "SELECT CONNECTION_ID()")
	connectionID := string(firstValues(t, observe(t, receipt, err).Outcome.Value)[0])
	drain(t, writer.inbox, 1)
	work, cancel := context.WithCancelCause(context.Background())
	var worker sync.WaitGroup
	defer func() { cancel(nil); worker.Wait() }()
	done := make(chan *invocation.Receipt[Result], 1)
	worker.Go(func() {
		receipt, err := tx.Query(work, correlation("observed-cancel"), "SELECT SLEEP(0.5)")
		if err != nil {
			t.Error("real cancellation initiation failed", err)
		}
		done <- receipt
	})
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
	f := bindFixture(t, options, 3)
	values := firstValues(t, serviceRead(t, f, "SELECT VERSION(),CAST(? AS UNSIGNED),CAST(? AS DECIMAL(30,5)),CAST(? AS JSON)", ^uint64(0), "12345678901234567890.00100", json.RawMessage("{\"exact\":18446744073709551615}")))
	if string(values[0]) != config.ExpectedVersion || string(values[1]) != "18446744073709551615" || string(values[2]) != "12345678901234567890.00100" {
		t.Fatal("real TLS values or version changed")
	}
	var document map[string]json.Number
	if json.Unmarshal(values[3], &document) != nil || len(document) != 1 || document["exact"].String() != "18446744073709551615" {
		t.Fatal("real TLS JSON number lost exactness")
	}
	cipher := serviceRead(t, f, "SHOW SESSION STATUS LIKE 'Ssl_cipher'")
	version := serviceRead(t, f, "SHOW SESSION STATUS LIKE 'Ssl_version'")
	cipherValues, versionValues := firstValues(t, cipher), firstValues(t, version)
	if len(cipherValues) < 2 || len(cipherValues[1]) == 0 || len(versionValues) < 2 || len(versionValues[1]) == 0 {
		t.Fatal("actual MySQL TLS session was not confirmed")
	}
	t.Logf("Real TLS session version=%s cipher=%s; parameter binding, unsigned/decimal/JSON values and trusted server identity verified.", versionValues[1], cipherValues[1])
	serviceLongParameters(t, f)
	if err := f.assembly.Close(context.Background()); err != nil {
		t.Fatal("real TLS resource cleanup failed", err)
	}
	if f.assembly.Snapshot().Sources[0].Usage != (resource.Usage{}) {
		t.Fatal("real TLS use remained")
	}
}

func serviceLongParameters(t *testing.T, fixture boundFixture) {
	t.Helper()
	statement, preparation, err := fixture.db.Prepare(context.Background(), correlation("long-prepare"), "SELECT OCTET_LENGTH(?), ?")
	if err != nil || statement == nil {
		t.Fatal("large-parameter preparation failed", err)
	}
	root := ownServiceCall(t, fixture.inbox, preparation, statement.Close)
	defer func() {
		receipt, err := statement.Close(context.Background())
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Error("large-parameter cleanup failed", result.Err())
		}
		if err = root.Release(); err != nil {
			t.Error(err)
		}
	}()
	receipt, err := statement.Query(context.Background(), correlation("invalid-year"), make([]byte, 800<<10), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))
	if receipt != nil || !errors.Is(err, ErrInput) {
		t.Fatal("invalid date reached native parameter dispatch")
	}
	for _, size := range []int{800 << 10, 790 << 10, 2, 800 << 10} {
		receipt, err := statement.Query(context.Background(), correlation("long-reuse"), make([]byte, size), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		result := observe(t, receipt, err)
		drain(t, fixture.inbox, 1)
		if result.Err() != nil {
			t.Fatal("large-parameter reuse failed", result.Err())
		}
		values := firstValues(t, result.Outcome.Value)
		if string(values[0]) != strconv.Itoa(size) || string(values[1]) != "2026-01-01" {
			t.Fatal("old native long-parameter data survived reuse")
		}
	}
	t.Log("Real long-data dispatch/reuse and invalid-date preflight preserve each call's exact parameter length.")
}

func serviceCoreCoverage(t *testing.T, options OptionsV1, writer, reader boundFixture, identifier uint64) {
	t.Helper()
	receipt, err := writer.db.Ping(context.Background(), correlation("native-ping"))
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal("real native Ping failed", result.Err())
	}
	drain(t, writer.inbox, 1)
	serviceLongParameters(t, writer)
	serviceExec(t, writer, "CREATE TABLE generated_ids (id BIGINT AUTO_INCREMENT PRIMARY KEY, note VARBINARY(8)) ENGINE=InnoDB")
	statement, preparation, err := writer.db.Prepare(context.Background(), correlation("insert-prepare"), "INSERT INTO generated_ids (note) VALUES (?)")
	if err != nil || statement == nil {
		t.Fatal("real prepared insert failed", err)
	}
	root := ownServiceCall(t, writer.inbox, preparation, statement.Close)
	var previous int64
	for _, value := range [][]byte{{}, nil, []byte("next")} {
		receipt, err := statement.Exec(context.Background(), correlation("insert-reuse"), value)
		result := observe(t, receipt, err)
		drain(t, writer.inbox, 1)
		id, known := result.Outcome.Value.LastInsertID()
		if result.Err() != nil || !known || id <= previous {
			t.Fatal("real native insert-ID/reuse failed", result.Err())
		}
		previous = id
		stored := firstValues(t, serviceRead(t, reader, "SELECT note FROM generated_ids WHERE id=?", id))[0]
		if string(stored) != string(value) || (stored == nil) != (value == nil) {
			t.Fatal("empty and NULL bytes were conflated")
		}
	}
	receipt, err = statement.Close(context.Background())
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	timeOptions := options
	timeOptions.Name, timeOptions.ParseTime = "parsed-time", true
	parsed := bindFixture(t, timeOptions, 1)
	stamp := time.Date(2026, 9, 12, 12, 0, 0, 123456000, time.UTC)
	values := firstValues(t, serviceRead(t, parsed, "SELECT CAST(? AS DATETIME(6)),CAST(? AS TIME(6)),CAST(? AS BINARY),CAST(? AS BINARY)", stamp, "-123:45:56.123456", []byte{}, []byte(nil)))
	if string(values[0]) != stamp.Format(time.RFC3339Nano) || string(values[1]) != "-123:45:56.123456" || values[2] == nil || len(values[2]) != 0 || values[3] != nil {
		t.Fatal("real binary time/empty/NULL representation changed")
	}
	columns := serviceRead(t, reader, "SELECT amount,revision,note FROM items WHERE id=?", identifier).ColumnsCopy()
	if len(columns) != 3 || !columns[0].PrecisionKnown || columns[0].Precision != 30 || columns[0].Scale != 5 || !columns[0].NullableKnown || !columns[0].Nullable || columns[1].Nullable {
		t.Fatal("real precision/scale/nullability metadata changed")
	}
	for _, level := range []sql.IsolationLevel{sql.LevelReadCommitted, sql.LevelRepeatableRead} {
		tx, snapshot, err := writer.db.Begin(context.Background(), correlation("snapshot"), TxOptionsV1{Isolation: level, ReadOnly: true})
		if err != nil || tx == nil {
			t.Fatal("snapshot transaction failed", err)
		}
		root := ownServiceCall(t, writer.inbox, snapshot, tx.Rollback)
		read := func() string {
			receipt, err := tx.Query(context.Background(), correlation("snapshot-read"), "SELECT revision FROM items WHERE id=?", identifier)
			result := observe(t, receipt, err)
			drain(t, writer.inbox, 1)
			if result.Err() != nil {
				t.Fatal(result.Err())
			}
			return string(firstValues(t, result.Outcome.Value)[0])
		}
		before := read()
		serviceExec(t, reader, "UPDATE items SET revision=revision+1 WHERE id=?", identifier)
		after := read()
		if (before == after) != (level == sql.LevelRepeatableRead) {
			t.Fatal("real statement/snapshot isolation behavior changed")
		}
		receipt, err := tx.Rollback(context.Background())
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		if err = root.Release(); err != nil {
			t.Fatal(err)
		}
	}
	servicePoolCoverage(t, options)
	t.Log("Real Ping, reusable prepared Query/Exec, generated IDs, binary ParseTime/TIME, metadata, read-only refusal and statement/snapshot isolation verified.")
}

func servicePoolCoverage(t *testing.T, options OptionsV1) {
	t.Helper()
	options.Name, options.MaxConnections = "pool-core", 2
	fixture := bindFixture(t, options, 4)
	var transactions []*Transaction
	var roots []*invocation.DeliveryRecord[Result]
	var identifiers []string
	for index := range 2 {
		tx, pinned, err := fixture.db.Begin(context.Background(), correlation("pool-pin-"+strconv.Itoa(index)), TxOptionsV1{ReadOnly: true})
		if err != nil || tx == nil {
			t.Fatal("real native pool pin failed", err)
		}
		transactions = append(transactions, tx)
		root := ownServiceCall(t, fixture.inbox, pinned, tx.Rollback)
		roots = append(roots, root)
		receipt, err := tx.Query(context.Background(), correlation("pool-id"), "SELECT CONNECTION_ID()")
		result := observe(t, receipt, err)
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
		identifiers = append(identifiers, string(firstValues(t, result.Outcome.Value)[0]))
		drain(t, fixture.inbox, 1)
	}
	stats := fixture.db.Stats()
	if identifiers[0] == identifiers[1] || stats.InUse != 2 || stats.OpenConnections != 2 || stats.MaxOpenConnections != 2 {
		t.Fatal("real native pool sharing or counters changed")
	}
	if _, err := fixture.db.Ping(context.Background(), correlation("pool-full")); !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("real pool saturation admitted excess work")
	}
	done := make(chan *invocation.Receipt[Result], 2)
	work, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	for index, tx := range transactions {
		workers.Go(func() {
			receipt, err := tx.Query(work, correlation("pool-parallel-"+strconv.Itoa(index)), "SELECT SLEEP(0.2)")
			if err != nil {
				t.Error("real parallel native query rejected", err)
			}
			done <- receipt
		})
	}
	observing := options
	observing.Name = "pool-observer"
	observer := bindFixture(t, observing, 1)
	deadline := time.Now().Add(2 * time.Second)
	for {
		count := firstValues(t, serviceRead(t, observer, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST WHERE ID IN (?,?) AND INFO='SELECT SLEEP(0.2)'", identifiers[0], identifiers[1]))[0]
		if string(count) == "2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("independent observer did not witness overlapping pool work")
		}
		time.Sleep(time.Millisecond)
	}
	for range 2 {
		if result := observe(t, <-done, nil); result.Err() != nil {
			t.Fatal(result.Err())
		}
	}
	workers.Wait()
	drain(t, fixture.inbox, 2)
	for index, tx := range transactions {
		receipt, err := tx.Rollback(context.Background())
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		if err = roots[index].Release(); err != nil {
			t.Fatal(err)
		}
	}
	if fixture.db.Stats().InUse != 0 {
		t.Fatal("real native pool retained use")
	}
	for _, idle := range []bool{false, true} {
		expiring := options
		expiring.Name, expiring.MaxConnections = "expiring", 1
		if idle {
			expiring.MaxIdleTime = 10 * time.Millisecond
		} else {
			expiring.MaxLifetime = 10 * time.Millisecond
		}
		pool := bindFixture(t, expiring, 1)
		first := string(firstValues(t, serviceRead(t, pool, "SELECT CONNECTION_ID()"))[0])
		if idle {
			deadline := time.Now().Add(3 * time.Second)
			for pool.db.Stats().MaxIdleTimeClosed == 0 {
				if time.Now().After(deadline) {
					t.Fatal("real native idle retirement did not finish")
				}
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			time.Sleep(20 * time.Millisecond)
		}
		second := string(firstValues(t, serviceRead(t, pool, "SELECT CONNECTION_ID()"))[0])
		stats := pool.db.Stats()
		if first == second || idle && stats.MaxIdleTimeClosed == 0 || !idle && stats.MaxLifetimeClosed == 0 {
			t.Fatal("real native expiration did not replace the connection")
		}
	}
	t.Log("Real same-pool concurrent connections, maximum admission/Stats and idle/lifetime replacement verified.")
}

func serviceFailureCleanupControl(t *testing.T, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMySQLServiceCleanupChild$", "-test.v", "-test.timeout=25s")
	command.Env = append(os.Environ(), "FATHOMRY_MYSQL_CLEANUP_CHILD="+database, "GORACE=atexit_sleep_ms=0")
	output, err := command.CombinedOutput()
	var failed *exec.ExitError
	if !errors.As(err, &failed) || failed.ExitCode() != 1 || ctx.Err() != nil ||
		!bytes.Contains(output, []byte("intentional retained-scope assertion failure")) ||
		!bytes.Contains(output, []byte("retained service scopes settled after failure")) ||
		bytes.Contains(output, []byte("cleanup failed")) || bytes.Contains(output, []byte("service-cleanup-failure")) ||
		bytes.Contains(output, []byte("panic:")) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatal("real service failure-path cleanup control did not satisfy its oracle")
	}
	t.Log("An intentionally failed real-service child finalized its transaction/statements and independent evidence before exit.")
}

func TestMySQLServiceCleanupChild(t *testing.T) {
	database := os.Getenv("FATHOMRY_MYSQL_CLEANUP_CHILD")
	if database == "" {
		return
	}
	if !fixtureName(database) {
		t.Fatal("invalid parent-owned fixture")
	}
	options, _ := serviceOptions(t, "FATHOMRY_MYSQL_TEST_CONFIG")
	options.Database = database
	fixture := bindFixture(t, options, 3)
	t.Cleanup(func() {
		if fixture.db.Stats().InUse != 0 || fixture.inbox.Usage() != (invocation.InboxUsage{}) || fixture.assembly.Snapshot().Sources[0].Usage != (resource.Usage{}) {
			t.Error("service failure cleanup retained active ownership")
			return
		}
		t.Log("retained service scopes settled after failure")
	})
	tx, receipt, err := fixture.db.Begin(context.Background(), correlation("cleanup-child"), TxOptionsV1{ReadOnly: true})
	if err != nil || tx == nil {
		t.Fatal("real cleanup control Begin failed", err)
	}
	ownServiceCall(t, fixture.inbox, receipt, tx.Rollback)
	statement, prepared, err := tx.Prepare(context.Background(), correlation("cleanup-child-prepare"), "SELECT revision FROM items")
	if err != nil || statement == nil {
		t.Fatal("real cleanup control Prepare failed", err)
	}
	ownServiceCall(t, fixture.inbox, prepared, statement.Close)
	receipt, err = statement.Query(context.Background(), correlation("cleanup-child-query"))
	if result := observe(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, fixture.inbox, 1)
	t.Fatal("intentional retained-scope assertion failure")
}
