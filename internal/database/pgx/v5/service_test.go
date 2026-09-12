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

package pgx

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type serviceConfig struct {
	Address                 string `json:"address"`
	Port                    uint16 `json:"port"`
	User                    string `json:"user"`
	Password                string `json:"password"`
	RootCAPEM               string `json:"root_ca_pem"`
	ServerName              string `json:"server_name"`
	ExpectedVersionNumber   int    `json:"expected_version_number"`
	AllowCreateTestDatabase bool   `json:"allow_create_test_database"`
}

func loadServiceConfig(t *testing.T) serviceConfig {
	t.Helper()
	path := os.Getenv("FATHOMRY_POSTGRES_TEST_CONFIG")
	if path == "" {
		t.Fatal("explicit private PostgreSQL fixture is required")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("private PostgreSQL fixture unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		t.Fatal("invalid private fixture permissions or size")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 128<<10+1))
	decoder.DisallowUnknownFields()
	var config serviceConfig
	if decoder.Decode(&config) != nil {
		t.Fatal("invalid private PostgreSQL fixture")
	}
	if !errors.Is(decoder.Decode(new(any)), io.EOF) || !config.AllowCreateTestDatabase || config.ExpectedVersionNumber == 0 ||
		config.RootCAPEM == "" || config.ServerName == "" {
		t.Fatal("explicit test-database authorization, expected service version and trusted TLS inputs are required")
	}
	return config
}
func TestPostgreSQLService(t *testing.T) {
	fixture := loadServiceConfig(t)
	options := OptionsV1{Name: "service", Address: fixture.Address, Port: fixture.Port, Database: "postgres", User: fixture.User,
		Password: fixture.Password, RootCAPEM: fixture.RootCAPEM, ServerName: fixture.ServerName, ParserHome: os.Getenv("HOME"), MaxConnections: 2}
	nativeOptions, err := nativeConfig(defaults(options))
	if err != nil {
		t.Fatal(err)
	}
	maintenanceOptions := nativeOptions.Copy()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := connect(ctx, nativeOptions, options.RootCAPEM)
	if err != nil {
		t.Fatal("authorized maintenance connection failed", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if admin.close(cleanup) != nil {
			t.Error("maintenance connection cleanup failed")
		}
	})
	var version int
	var create, encrypted, recovery, superuser bool
	err = admin.native.QueryRow(ctx, "SELECT current_setting('server_version_num')::int,rolcreatedb,rolsuper,pg_is_in_recovery(),COALESCE((SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()),false) FROM pg_roles WHERE rolname=current_user").Scan(&version, &create, &superuser, &recovery, &encrypted)
	if err != nil || version != fixture.ExpectedVersionNumber || !create || superuser || recovery || !encrypted {
		t.Fatal("actual service version, TLS or isolated-database permission differs from authorized fixture")
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("test database identity unavailable")
	}
	name := "gh23_" + hex.EncodeToString(nonce[:])
	var exists bool
	if err := admin.native.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", name).Scan(&exists); err != nil || exists {
		t.Fatal("exclusive test database absence was not established")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := cleanupTestDatabase(cleanup, maintenanceOptions, options.RootCAPEM, name); err != nil {
			t.Error("owned test database cleanup failed; no business database was targeted")
			return
		}
		t.Log("Owned isolated database dropped; independent absence check passed.")
	})
	if _, err := admin.native.Exec(ctx, "CREATE DATABASE "+sdk.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("authorized isolated database creation failed")
	}
	options.Database = name
	nativeOptions, err = nativeConfig(defaults(options))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := connect(ctx, nativeOptions, options.RootCAPEM)
	if err != nil {
		t.Fatal("isolated read-back connection failed", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if reader.close(cleanup) != nil {
			t.Error("read-back connection cleanup failed")
		}
	})
	if _, err := reader.native.Exec(ctx, "CREATE TABLE gh23_items (id bigint PRIMARY KEY, payload text NOT NULL, note text, revision integer NOT NULL)"); err != nil {
		t.Fatal("owned metadata-shaped table creation failed")
	}
	bound := bindFixture(t, options, 8)
	receipt, err := bound.database.Exec(ctx, correlation("insert"), "INSERT INTO gh23_items VALUES ($1,$2,$3,$4)", int64(1), "initial", nil, int32(1))
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.RowsAffected() != 1 {
		t.Fatal("parameterized write failed", result.Err())
	}
	drain(t, bound.inbox, 1)
	result := queryResult(t, bound.database, "read", "SELECT payload,note FROM gh23_items WHERE id=$1", int64(1))
	row, firstErr := result.Outcome.Value.First()
	if result.Err() != nil || firstErr != nil || string(row.ValuesCopy()[0]) != "initial" || row.ValuesCopy()[1] != nil {
		t.Fatal("parameterized read or SQL NULL semantics failed")
	}
	drain(t, bound.inbox, 1)
	transaction := beginTransaction(t, bound, "commit")
	receipt, err = transaction.Exec(ctx, correlation("update"), "UPDATE gh23_items SET payload=$1,revision=revision+1 WHERE id=$2", "committed", int64(1))
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("transaction update failed", result.Err())
	}
	receipt, err = transaction.Commit(ctx)
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.TransactionOutcome() != CommitAcknowledged {
		t.Fatal("commit acknowledgement failed")
	}
	drain(t, bound.inbox, 2)
	var payload string
	if err := reader.native.QueryRow(ctx, "SELECT payload FROM gh23_items WHERE id=$1", int64(1)).Scan(&payload); err != nil || payload != "committed" {
		t.Fatal("independent committed read-back failed")
	}
	transaction = beginTransaction(t, bound, "rollback")
	receipt, err = transaction.Exec(ctx, correlation("discard-update"), "UPDATE gh23_items SET payload=$1 WHERE id=$2", "discarded", int64(1))
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.RowsAffected() != 1 {
		t.Fatal("rollback write setup failed")
	}
	inside, insideErr := transaction.Query(ctx, correlation("rollback-visible"), "SELECT payload FROM gh23_items WHERE id=$1", int64(1))
	insideResult := operationResult(t, inside, insideErr)
	insideRow, insideErr := insideResult.Outcome.Value.First()
	if insideResult.Err() != nil || insideErr != nil || string(insideRow.ValuesCopy()[0]) != "discarded" {
		t.Fatal("rollback write was not visible inside its transaction")
	}
	if err := reader.native.QueryRow(ctx, "SELECT payload FROM gh23_items WHERE id=$1", int64(1)).Scan(&payload); err != nil || payload != "committed" {
		t.Fatal("uncommitted rollback write escaped to the independent reader")
	}
	receipt, err = transaction.Rollback(ctx)
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("explicit rollback failed")
	}
	drain(t, bound.inbox, 3)
	if err := reader.native.QueryRow(ctx, "SELECT payload FROM gh23_items WHERE id=$1", int64(1)).Scan(&payload); err != nil || payload != "committed" {
		t.Fatal("independent rollback read-back failed")
	}
	verifyServiceAbortedCommit(t, ctx, bound, reader)
	verifyServiceEmptyAndLimits(t, ctx, options, bound)
	partial := queryResult(t, bound.database, "partial", "SELECT 100 / (17 - n) FROM generate_series(1,32) AS n")
	var pgError *pgconn.PgError
	if !errors.As(partial.Err(), &pgError) || pgError.Code != "22012" || partial.Outcome.Value.Complete() ||
		partial.Outcome.Value.RowsRead() != 16 {
		t.Fatal("real partial output and late server error were not retained")
	}
	drain(t, bound.inbox, 1)
	verifyServiceCancellation(t, ctx, bound, reader)

	tap := newCommandDropProxy(t, defaults(options), "COMMIT")
	proxyOptions := options
	_, port, _ := net.SplitHostPort(tap.listener.Addr().String())
	portNumber, _ := strconv.Atoi(port)
	proxyOptions.Address, proxyOptions.Port, proxyOptions.Name = "127.0.0.1", uint16(portNumber), "commit-drop"
	proxyOptions.Plaintext, proxyOptions.RootCAPEM, proxyOptions.ServerName = true, "", ""
	proxied := bindFixture(t, proxyOptions, 2)
	transaction = beginTransaction(t, proxied, "lost-commit")
	receipt, err = transaction.Exec(ctx, correlation("lost-update"), "UPDATE gh23_items SET payload=$1 WHERE id=$2", "response-lost", int64(1))
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("fault-injection write setup failed")
	}
	receipt, err = transaction.Commit(ctx)
	unknown := operationResult(t, receipt, err)
	if unknown.Err() == nil || unknown.Outcome.Value.TransactionOutcome() != FinalizationUnknown {
		t.Fatal("lost commit reply became confirmed client success")
	}
	select {
	case <-tap.dropped:
	case <-ctx.Done():
		t.Fatal("proxy did not observe and discard the real COMMIT response")
	}
	if err := reader.native.QueryRow(ctx, "SELECT payload FROM gh23_items WHERE id=$1", int64(1)).Scan(&payload); err != nil || payload != "response-lost" {
		t.Fatal("independent real-server oracle did not prove the acknowledged-but-lost commit effect")
	}
	drain(t, proxied.inbox, 2)
	verifyServiceCreateResponseLoss(t, ctx, fixture, maintenanceOptions)
	t.Logf("PostgreSQL version-number=%d; verified TLS and required SCRAM; parameterized I/O, NULL, commit, rollback, partial error, cancellation and lost-COMMIT read-back executed.", version)
}

type commandDropProxy struct {
	listener net.Listener
	dropped  chan struct{}
}

// This bounded proxy uses verified TLS to the authorized real server. Only the
// loopback client leg is plaintext. It discards the selected CommandComplete,
// not a fabricated PostgreSQL response; no SQL or packet contents are logged.
func newCommandDropProxy(t *testing.T, options settings, command string) *commandDropProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("bounded proxy listener failed")
	}
	tap := &commandDropProxy{listener: listener, dropped: make(chan struct{})}
	trust, err := tlsConfig(options)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	stopped := make(chan struct{})
	workers.Go(func() {
		downstream, err := listener.Accept()
		if err != nil {
			return
		}
		listener.Close()
		mu.Lock()
		select {
		case <-stopped:
			mu.Unlock()
			downstream.Close()
			return
		default:
		}
		connections = append(connections, downstream)
		mu.Unlock()
		defer downstream.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(options.Address, strconv.Itoa(int(options.Port))))
		if err != nil {
			return
		}
		defer upstream.Close()
		mu.Lock()
		connections = append(connections, upstream)
		mu.Unlock()
		upstream.SetDeadline(time.Now().Add(30 * time.Second))
		request := []byte{0, 0, 0, 8, 4, 210, 22, 47}
		if _, err := upstream.Write(request); err != nil {
			return
		}
		var answer [1]byte
		if _, err := io.ReadFull(upstream, answer[:]); err != nil || answer[0] != 'S' {
			return
		}
		secure := tls.Client(upstream, trust)
		if secure.HandshakeContext(ctx) != nil {
			return
		}
		copied := make(chan struct{})
		go func() { defer close(copied); _, _ = io.Copy(secure, downstream); secure.Close() }()
		defer func() { downstream.Close(); secure.Close(); <-copied }()
		for {
			var header [5]byte
			if _, err := io.ReadFull(secure, header[:]); err != nil {
				return
			}
			length := int(binary.BigEndian.Uint32(header[1:])) - 4
			if length < 0 || length > 4<<20 {
				return
			}
			body := make([]byte, length)
			if _, err := io.ReadFull(secure, body); err != nil {
				return
			}
			if header[0] == 'C' && string(body) == command+"\x00" {
				close(tap.dropped)
				return
			}
			if _, err := downstream.Write(header[:]); err != nil {
				return
			}
			if _, err := downstream.Write(body); err != nil {
				return
			}
		}
	})
	t.Cleanup(func() {
		close(stopped)
		listener.Close()
		mu.Lock()
		for _, connection := range connections {
			connection.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return tap
}

func cleanupTestDatabase(ctx context.Context, config *sdk.ConnConfig, rootCAPEM, name string) error {
	for attempt := 0; attempt < 20; attempt++ {
		maintenance, err := connect(ctx, config, rootCAPEM)
		if err != nil {
			return err
		}
		var owned bool
		err = maintenance.native.QueryRow(ctx, "SELECT datdba=(SELECT oid FROM pg_roles WHERE rolname=current_user) FROM pg_database WHERE datname=$1", name).Scan(&owned)
		if errors.Is(err, sdk.ErrNoRows) {
			return maintenance.close(ctx)
		}
		if err != nil || !owned {
			_ = maintenance.close(ctx)
			return failure(ErrCleanup, "fixture-ownership", err)
		}
		_, dropErr := maintenance.native.Exec(ctx, "DROP DATABASE "+sdk.Identifier{name}.Sanitize())
		closeErr := maintenance.close(ctx)
		if closeErr != nil {
			return closeErr
		}
		if dropErr != nil {
			var native *pgconn.PgError
			if errors.As(dropErr, &native) && native.Code != "55006" {
				return failure(ErrCleanup, "fixture-drop", dropErr)
			}
			if ctx.Err() != nil {
				return failure(ErrCleanup, "fixture-drop", dropErr, ctx.Err())
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		// Reconnect for independent absence confirmation, including a lost DROP reply.
	}
	return failure(ErrCleanup, "fixture-drop-unconfirmed")
}
func verifyServiceAbortedCommit(t *testing.T, ctx context.Context, bound boundFixture, reader *connection) {
	t.Helper()
	transaction := beginTransaction(t, bound, "aborted")
	receipt, err := transaction.Exec(ctx, correlation("aborted-update"), "UPDATE gh23_items SET payload=$1 WHERE id=$2", "aborted", int64(1))
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.RowsAffected() != 1 {
		t.Fatal("aborted transaction write setup failed")
	}
	receipt, err = transaction.Query(ctx, correlation("aborted-visible"), "SELECT payload FROM gh23_items WHERE id=$1", int64(1))
	result := operationResult(t, receipt, err)
	row, rowErr := result.Outcome.Value.First()
	if result.Err() != nil || rowErr != nil || string(row.ValuesCopy()[0]) != "aborted" {
		t.Fatal("aborted transaction write was not visible before its failure")
	}
	receipt, err = transaction.Exec(ctx, correlation("duplicate"), "INSERT INTO gh23_items VALUES ($1,$2,$3,$4)", int64(1), "duplicate", nil, int32(0))
	result = operationResult(t, receipt, err)
	var native *pgconn.PgError
	if !errors.As(result.Err(), &native) || native.Code != "23505" {
		t.Fatal("real constraint failure was not observed")
	}
	receipt, err = transaction.Commit(ctx)
	result = operationResult(t, receipt, err)
	if !errors.Is(result.Err(), sdk.ErrTxCommitRollback) || result.Outcome.Value.TransactionOutcome() != CommitRolledBack {
		t.Fatal("real aborted commit became success or unknown")
	}
	drain(t, bound.inbox, 4)
	var payload string
	if err := reader.native.QueryRow(ctx, "SELECT payload FROM gh23_items WHERE id=$1", int64(1)).Scan(&payload); err != nil || payload != "committed" {
		t.Fatal("aborted commit changed independently observed data")
	}
	t.Log("Real aborted-COMMIT and independent unchanged-data oracle passed.")
}
func verifyServiceEmptyAndLimits(t *testing.T, ctx context.Context, options OptionsV1, bound boundFixture) {
	t.Helper()
	receipt, err := bound.database.Exec(ctx, correlation("empty-insert"), "INSERT INTO gh23_items VALUES ($1,$2,$3,$4)", int64(2), "", "", int32(0))
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.RowsAffected() != 1 {
		t.Fatal("empty-value insert failed")
	}
	drain(t, bound.inbox, 1)
	result := queryResult(t, bound.database, "empty-value", "SELECT payload,note FROM gh23_items WHERE id=$1", int64(2))
	row, rowErr := result.Outcome.Value.First()
	if result.Err() != nil || rowErr != nil || row.ValuesCopy()[0] == nil || row.ValuesCopy()[1] == nil || len(row.ValuesCopy()[0]) != 0 || len(row.ValuesCopy()[1]) != 0 {
		t.Fatal("real empty string became NULL")
	}
	drain(t, bound.inbox, 1)
	result = queryResult(t, bound.database, "no-rows", "SELECT payload FROM gh23_items WHERE id=$1", int64(999))
	if _, err := result.Outcome.Value.First(); result.Err() != nil || !result.Outcome.Value.Complete() || !errors.Is(err, sdk.ErrNoRows) {
		t.Fatal("real no-row result lost completeness/identity")
	}
	drain(t, bound.inbox, 1)
	receipt, err = bound.database.Exec(ctx, correlation("multiple-statements"), "INSERT INTO gh23_items VALUES (99,'excluded',NULL,0); SELECT 1")
	result = operationResult(t, receipt, err)
	var native *pgconn.PgError
	if !errors.As(result.Err(), &native) || native.Code != "42601" {
		t.Fatal("extended protocol admitted multiple statements")
	}
	drain(t, bound.inbox, 1)
	result = queryResult(t, bound.database, "multiple-absent", "SELECT payload FROM gh23_items WHERE id=99")
	if _, err := result.Outcome.Value.First(); result.Err() != nil || !errors.Is(err, sdk.ErrNoRows) {
		t.Fatal("rejected multi-statement input changed data")
	}
	drain(t, bound.inbox, 1)

	limitedOptions := options
	limitedOptions.Name, limitedOptions.MaxConnections, limitedOptions.MaxRows = "service-limits", 1, 1
	limitedOptions.MaxResultBytes, limitedOptions.MaxMessageBytes = 1024, 4096
	limited := bindFixture(t, limitedOptions, 1)
	for _, keep := range []bool{true, false} {
		invoke := func(id, sql string, args ...any) invocation.Result[Result] {
			var receipt *invocation.Receipt[Result]
			var err error
			if keep {
				receipt, err = limited.database.Query(ctx, correlation(id), sql, args...)
			} else {
				receipt, err = limited.database.Exec(ctx, correlation(id), sql, args...)
			}
			result := operationResult(t, receipt, err)
			drain(t, limited.inbox, 1)
			return result
		}
		for _, test := range []struct {
			size    int
			limited bool
		}{{1023, false}, {1024, true}} {
			result := invoke("bytes", "SELECT repeat('x',$1) AS v", test.size)
			if errors.Is(result.Err(), ErrLimit) != test.limited || result.Outcome.Value.Complete() == test.limited ||
				!keep && result.Outcome.Value.RowsCopy() != nil {
				t.Fatal("real Query/Exec result-byte boundary failed")
			}
		}
		result := invoke("rows", "SELECT n AS v FROM generate_series(1,2) AS n")
		if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.RowsRead() != 2 || result.Outcome.Value.Complete() {
			t.Fatal("real Query/Exec row bound failed")
		}
		for _, count := range []int{64, 65} {
			result := invoke("columns", "SELECT "+strings.Repeat("NULL::text AS v,", count-1)+"NULL::text AS v")
			if errors.Is(result.Err(), ErrLimit) != (count == 65) || result.Outcome.Value.Complete() != (count == 64) {
				t.Fatal("real column bound failed")
			}
		}
		if result := invoke("reuse", "SELECT 1 AS v"); result.Err() != nil || !result.Outcome.Value.Complete() {
			t.Fatal("bounded-result drainage prevented reuse")
		}
	}
	wireOptions := options
	wireOptions.Name, wireOptions.MaxConnections = "service-wire", 1
	wireOptions.MaxMessageBytes = 1024
	wire := bindFixture(t, wireOptions, 1)
	for _, keep := range []bool{true, false} {
		for _, size := range []int{1018, 1019} {
			var receipt *invocation.Receipt[Result]
			var err error
			if keep {
				receipt, err = wire.database.Query(ctx, correlation("wire"), "SELECT repeat('x',$1) AS v", size)
			} else {
				receipt, err = wire.database.Exec(ctx, correlation("wire"), "SELECT repeat('x',$1) AS v", size)
			}
			result := operationResult(t, receipt, err)
			if size == 1018 {
				if result.Err() != nil || !result.Outcome.Value.Complete() {
					t.Fatal("exact real protocol-body bound rejected")
				}
			} else {
				var native *pgproto3.ExceededMaxBodyLenErr
				if !errors.As(result.Err(), &native) || wire.database.owner.native.Stat().TotalResources() != 0 {
					t.Fatal("oversize real protocol connection was not rejected and retired")
				}
			}
			drain(t, wire.inbox, 1)
		}
	}
	if result := queryResult(t, wire.database, "replacement", "SELECT 1"); result.Err() != nil {
		t.Fatal("real protocol failure replacement failed")
	}
	drain(t, wire.inbox, 1)
	t.Log("Real empty/NULL/no-row, single-statement refusal, Query/Exec row/byte/column/protocol bounds and replacement passed.")
}
func verifyServiceCancellation(t *testing.T, ctx context.Context, bound boundFixture, reader *connection) {
	t.Helper()
	pidResult := queryResult(t, bound.database, "pid", "SELECT pg_backend_pid()")
	row, err := pidResult.Outcome.Value.First()
	if pidResult.Err() != nil || err != nil {
		t.Fatal("native cancellation PID was not observed")
	}
	pid, err := strconv.Atoi(string(row.ValuesCopy()[0]))
	if err != nil {
		t.Fatal("invalid native backend identifier")
	}
	drain(t, bound.inbox, 1)
	work, cancel := context.WithCancelCause(ctx)
	cause := errors.New("explicit-service-cancellation")
	type response struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan response, 1)
	go func() {
		receipt, err := bound.database.Query(work, correlation("cancel"), "SELECT pg_sleep(30)")
		done <- response{receipt, err}
	}()
	defer cancel(nil)
	deadline := time.Now().Add(3 * time.Second)
	entered := false
	for time.Now().Before(deadline) {
		if err := reader.native.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active' AND wait_event='PgSleep' AND query=$2)", pid, "SELECT pg_sleep(30)").Scan(&entered); err != nil {
			t.Fatal("independent query-entry observation failed")
		}
		if entered {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel(cause)
	var outcome response
	select {
	case outcome = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("real cancellation did not return and clean up")
	}
	result := operationResult(t, outcome.receipt, outcome.err)
	if !entered || !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) {
		t.Fatal("real cancellation lacked entry or original cause")
	}
	drain(t, bound.inbox, 1)
	deadline = time.Now().Add(3 * time.Second)
	exited := false
	for time.Now().Before(deadline) {
		if err := reader.native.QueryRow(ctx, "SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)", pid).Scan(&exited); err != nil {
			t.Fatal("independent backend-exit observation failed")
		}
		if exited {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !exited {
		t.Fatal("real canceled backend did not disappear")
	}
	if result := queryResult(t, bound.database, "after-cancel", "SELECT 1"); result.Err() != nil {
		t.Fatal("post-cancellation connection replacement failed")
	}
	drain(t, bound.inbox, 1)
	t.Log("Real cancellation independently observed PgSleep entry, caller cause, backend exit and replacement.")
}
func verifyServiceCreateResponseLoss(t *testing.T, ctx context.Context, fixture serviceConfig, maintenanceOptions *sdk.ConnConfig) {
	t.Helper()
	options := OptionsV1{Name: "create-drop", Address: fixture.Address, Port: fixture.Port, Database: "postgres",
		User: fixture.User, Password: fixture.Password, RootCAPEM: fixture.RootCAPEM, ServerName: fixture.ServerName, ParserHome: os.Getenv("HOME")}
	proxy := newCommandDropProxy(t, defaults(options), "CREATE DATABASE")
	_, port, _ := net.SplitHostPort(proxy.listener.Addr().String())
	number, _ := strconv.Atoi(port)
	options.Address, options.Port, options.Plaintext, options.RootCAPEM, options.ServerName = "127.0.0.1", uint16(number), true, "", ""
	config, err := nativeConfig(defaults(options))
	if err != nil {
		t.Fatal(err)
	}
	creator, err := connect(ctx, config, "")
	if err != nil {
		t.Fatal("creation-response-loss setup failed", err)
	}
	defer creator.close(ctx)
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	name := "gh23_" + hex.EncodeToString(nonce[:])
	var exists bool
	if err := creator.native.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", name).Scan(&exists); err != nil || exists {
		t.Fatal("creation fault target absence was not established")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := cleanupTestDatabase(cleanup, maintenanceOptions, fixture.RootCAPEM, name); err != nil {
			t.Error("creation-fault database cleanup remained unresolved")
		}
	})
	if _, err := creator.native.Exec(ctx, "CREATE DATABASE "+sdk.Identifier{name}.Sanitize()); err == nil || !creator.native.IsClosed() {
		t.Fatal("creation response was not lost on a retired connection")
	}
	select {
	case <-proxy.dropped:
	case <-ctx.Done():
		t.Fatal("actual CREATE acknowledgement was not intercepted")
	}
	observer, err := connect(ctx, maintenanceOptions, fixture.RootCAPEM)
	if err != nil {
		t.Fatal("independent creation observer failed", err)
	}
	err = observer.native.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", name).Scan(&exists)
	closeErr := observer.close(ctx)
	if err != nil || closeErr != nil || !exists {
		t.Fatal("lost-CREATE fixture did not independently exist")
	}
	if err := cleanupTestDatabase(ctx, maintenanceOptions, fixture.RootCAPEM, name); err != nil {
		t.Fatal("fresh-connection cleanup could not reconcile lost creation acknowledgement")
	}
	t.Log("Lost real CREATE response was independently confirmed and cleaned through fresh maintenance connections.")
}
