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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
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
	var vendor, isolation, synchronous, zone, dateStyle, encoding, tlsVersion, cipher string
	err = admin.native.QueryRow(ctx, "SELECT version(),current_setting('default_transaction_isolation'),current_setting('synchronous_commit'),current_setting('TimeZone'),current_setting('DateStyle'),current_setting('client_encoding'),version,cipher FROM pg_stat_ssl WHERE pid=pg_backend_pid()").Scan(&vendor, &isolation, &synchronous, &zone, &dateStyle, &encoding, &tlsVersion, &cipher)
	if err != nil || tlsVersion == "" || cipher == "" {
		t.Fatal("actual PostgreSQL session metadata unavailable")
	}
	t.Logf("Observed PostgreSQL=%s isolation=%s synchronous_commit=%s TimeZone=%s DateStyle=%s encoding=%s TLS=%s cipher=%s; non-superuser CREATEDB/SCRAM verified", vendor, isolation, synchronous, zone, dateStyle, encoding, tlsVersion, cipher)
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("test database identity unavailable")
	}
	name := "gh28_" + hex.EncodeToString(nonce[:])
	if err := provisionTestDatabase(t, ctx, admin, maintenanceOptions, options.RootCAPEM, name); err != nil {
		t.Fatal("authorized isolated database creation failed")
	}
	if err := provisionTestDatabase(t, ctx, admin, maintenanceOptions, options.RootCAPEM, name); !errors.Is(err, ErrState) {
		t.Fatal("preexisting fixture was adopted")
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
	verifyServiceCoreCapabilities(t, ctx, options, reader)
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
	if len(name) != 29 || !strings.HasPrefix(name, "gh28_") {
		return failure(ErrInput, "fixture-name")
	}
	if _, err := hex.DecodeString(name[5:]); err != nil {
		return failure(ErrInput, "fixture-name")
	}
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

func provisionTestDatabase(t *testing.T, ctx context.Context, creator *connection, maintenance *sdk.ConnConfig, roots, name string) error {
	t.Helper()
	if len(name) != 29 || !strings.HasPrefix(name, "gh28_") {
		return failure(ErrInput, "fixture-name")
	}
	if _, err := hex.DecodeString(name[5:]); err != nil {
		return failure(ErrInput, "fixture-name")
	}
	var exists bool
	if err := creator.native.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", name).Scan(&exists); err != nil || exists {
		return failure(ErrState, "fixture-preexisting", err)
	}
	responsible := false
	t.Cleanup(func() {
		if !responsible {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := cleanupTestDatabase(cleanup, maintenance, roots, name); err != nil {
			t.Error("owned test database cleanup failed", err)
			return
		}
		t.Log("Owned isolated database reconciled; independent absence check passed.")
	})
	tag, err := creator.native.Exec(ctx, "CREATE DATABASE "+sdk.Identifier{name}.Sanitize())
	responsible = fixtureCreationMayExist(tag, err)
	return err
}
func fixtureCreationMayExist(tag pgconn.CommandTag, err error) bool {
	if tag.String() == "CREATE DATABASE" || err == nil {
		return true
	}
	var native *pgconn.PgError
	if errors.As(err, &native) {
		severity := native.SeverityUnlocalized
		if severity == "" {
			severity = native.Severity
		}
		if severity == "ERROR" && !strings.HasPrefix(native.Code, "08") {
			return false
		}
	}
	return !pgconn.SafeToRetry(err)
}

func TestFixtureCreationResponsibility(t *testing.T) {
	for _, test := range []struct {
		tag  string
		err  error
		want bool
	}{
		{"CREATE DATABASE", nil, true},
		{"CREATE DATABASE", &pgconn.PgError{SeverityUnlocalized: "FATAL", Code: "57P01"}, true},
		{"", &pgconn.PgError{SeverityUnlocalized: "FATAL", Code: "57P01"}, true},
		{"", &pgconn.PgError{SeverityUnlocalized: "ERROR", Code: "42P04"}, false},
		{"", io.EOF, true},
	} {
		if fixtureCreationMayExist(pgconn.NewCommandTag(test.tag), test.err) != test.want {
			t.Fatal("CREATE effect and failure classification were conflated")
		}
	}
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
	var worker sync.WaitGroup
	worker.Go(func() {
		receipt, err := bound.database.Query(work, correlation("cancel"), "SELECT pg_sleep(30)")
		done <- response{receipt, err}
	})
	var outcome response
	received, drained := false, false
	defer func() {
		cancel(nil)
		worker.Wait()
		if !received {
			outcome = <-done
		}
		if outcome.receipt != nil && !drained {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			record, err := bound.inbox.Next(cleanup)
			if err != nil {
				t.Error("cancellation evidence cleanup failed", err)
				return
			}
			if _, err = record.Receipt().WaitReleased(cleanup); err != nil {
				t.Error("cancellation remained owned", err)
				return
			}
			if err = record.Release(); err != nil {
				t.Error(err)
			}
		}
	}()
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
	select {
	case outcome = <-done:
		received = true
	case <-time.After(20 * time.Second):
		t.Fatal("real cancellation did not return and clean up")
	}
	result := operationResult(t, outcome.receipt, outcome.err)
	if !entered || !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) {
		t.Fatal("real cancellation lacked entry or original cause")
	}
	drain(t, bound.inbox, 1)
	drained = true
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
	name := "gh28_" + hex.EncodeToString(nonce[:])
	if err := provisionTestDatabase(t, ctx, creator, maintenanceOptions, fixture.RootCAPEM, name); err == nil || !creator.native.IsClosed() {
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
	var exists bool
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

func serviceOwnedCall(t *testing.T, inbox *invocation.Inbox[Result], receipt *invocation.Receipt[Result], finish func(context.Context) (*invocation.Receipt[Result], error)) *invocation.DeliveryRecord[Result] {
	t.Helper()
	var record *invocation.DeliveryRecord[Result]
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := finish(ctx)
		if err != nil && !errors.Is(err, sdk.ErrTxClosed) {
			t.Error("owned service scope cleanup failed", err)
		}
		result, err := receipt.WaitReleased(ctx)
		if err != nil {
			t.Error("owned service scope remained", err)
			return
		}
		if result.Outcome.Cleanup != nil {
			t.Error("independent service cleanup failed", result.Outcome.Cleanup)
		}
		if record != nil {
			if err = record.Release(); err != nil {
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
func servicePointCleanup(point *Savepoint) func(context.Context) (*invocation.Receipt[Result], error) {
	return func(ctx context.Context) (*invocation.Receipt[Result], error) {
		receipt, err := point.Rollback(ctx)
		if err == nil {
			return receipt, nil
		}
		_, parentErr := point.parent.Rollback(ctx)
		if errors.Is(parentErr, sdk.ErrTxClosed) {
			parentErr = nil
		}
		return point.call.Receipt(), parentErr
	}
}
func serviceRead(t *testing.T, fixture boundFixture, sql string, args ...any) Result {
	t.Helper()
	result := queryResult(t, fixture.database, "service-read", sql, args...)
	drain(t, fixture.inbox, 1)
	if result.Err() != nil {
		t.Fatal("real bounded read failed", result.Err())
	}
	return result.Outcome.Value
}
func serviceExec(t *testing.T, fixture boundFixture, sql string, args ...any) Result {
	t.Helper()
	receipt, err := fixture.database.Exec(context.Background(), correlation("service-exec"), sql, args...)
	result := operationResult(t, receipt, err)
	drain(t, fixture.inbox, 1)
	if result.Err() != nil {
		t.Fatal("real bounded execution failed", result.Err())
	}
	return result.Outcome.Value
}
func serviceFirst(t *testing.T, result Result) [][]byte {
	t.Helper()
	row, err := result.First()
	if err != nil {
		t.Fatal(err)
	}
	return row.ValuesCopy()
}
func verifyServiceCoreCapabilities(t *testing.T, ctx context.Context, options OptionsV1, reader *connection) {
	t.Helper()
	options.Name, options.MaxConnections = "core", 1
	fixture := bindFixture(t, options, 3)
	receipt, err := fixture.database.Ping(ctx, correlation("native-ping"))
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("real Ping failed", result.Err())
	}
	drain(t, fixture.inbox, 1)
	serviceExec(t, fixture, "/* ordinary DDL */ CREATE TABLE core_items(id integer PRIMARY KEY, note text)")
	merged := serviceExec(t, fixture, "MERGE INTO core_items AS target USING (VALUES(1,'merged')) AS source(id,note) ON target.id=source.id WHEN NOT MATCHED THEN INSERT(id,note) VALUES(source.id,source.note)")
	if merged.RowsAffected() != 1 || merged.CommandTag() != "MERGE 1" {
		t.Fatal("native MERGE evidence changed")
	}
	var note string
	if err := reader.native.QueryRow(ctx, "SELECT note FROM core_items WHERE id=1").Scan(&note); err != nil || note != "merged" {
		t.Fatal("independent MERGE readback failed")
	}
	if string(serviceFirst(t, serviceRead(t, fixture, "/* leading comment */ VALUES (42::int4)"))[0]) != "42" {
		t.Fatal("ordinary VALUES failed")
	}
	if len(serviceRead(t, fixture, "EXPLAIN SELECT * FROM core_items").RowsCopy()) == 0 {
		t.Fatal("bounded EXPLAIN returned no plan")
	}
	if len(serviceRead(t, fixture, "TABLE core_items").RowsCopy()) != 1 {
		t.Fatal("ordinary TABLE failed")
	}

	query := "SELECT $1::timestamp,$2::text,$3::bytea,$4::numeric(30,5),$5::bytea"
	stamp := time.Date(2026, 9, 13, 14, 0, 0, 123456000, time.FixedZone("offset", 2*3600))
	args := []any{stamp, true, []byte{}, "12345678901234567890.00100", []byte(nil)}
	ordinary := serviceRead(t, fixture, query, args...)
	values := serviceFirst(t, ordinary)
	if string(values[0]) != "2026-09-13 12:00:00.123456" || string(values[1]) != "t" || string(values[2]) != "\\x" || string(values[3]) != "12345678901234567890.00100" || values[4] != nil {
		t.Fatal("native text parameter contract changed")
	}
	statement, prepared, err := fixture.database.Prepare(ctx, correlation("prepared"), query)
	if err != nil || statement == nil {
		t.Fatal("real preparation failed", err)
	}
	root := serviceOwnedCall(t, fixture.inbox, prepared, statement.Close)
	for range 3 {
		receipt, err := statement.Query(ctx, correlation("prepared-read"), args...)
		result := operationResult(t, receipt, err)
		drain(t, fixture.inbox, 1)
		if result.Err() != nil || !reflect.DeepEqual(serviceFirst(t, result.Outcome.Value), values) || !reflect.DeepEqual(result.Outcome.Value.ColumnsCopy(), ordinary.ColumnsCopy()) {
			t.Fatal("prepared and ordinary text contracts diverged", result.Err())
		}
	}
	if _, err = statement.Query(ctx, correlation("invalid-argument"), struct{}{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("unsupported prepared argument reached native work")
	}
	receipt, err = statement.Query(ctx, correlation("after-invalid"), args...)
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("input refusal invalidated preparation", result.Err())
	}
	drain(t, fixture.inbox, 1)
	receipt, err = statement.Close(ctx)
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}

	insert, prepared, err := fixture.database.Prepare(ctx, correlation("prepared-insert"), "INSERT INTO core_items VALUES($1,$2)")
	if err != nil || insert == nil {
		t.Fatal(err)
	}
	root = serviceOwnedCall(t, fixture.inbox, prepared, insert.Close)
	for id := 2; id <= 3; id++ {
		receipt, err := insert.Exec(ctx, correlation("insert-reuse"), int32(id), "prepared")
		result := operationResult(t, receipt, err)
		drain(t, fixture.inbox, 1)
		if result.Err() != nil || result.Outcome.Value.RowsAffected() != 1 {
			t.Fatal("real prepared Exec reuse failed", result.Err())
		}
	}
	receipt, err = insert.Close(ctx)
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := reader.native.QueryRow(ctx, "SELECT count(*) FROM core_items").Scan(&count); err != nil || count != 3 {
		t.Fatal("prepared writes lost independent effects")
	}
	verifyServicePreparedFailures(t, ctx, fixture)
	verifyServiceSavepoints(t, ctx, fixture, reader)
	verifyServiceSessionAndTermination(t, ctx, fixture)
	verifyServicePoolLifecycle(t, ctx, options, reader)
	t.Log("Real ordinary SQL, repeated native preparation/text semantics, savepoints, session reset, chain termination and pool lifecycle passed.")
}
func verifyServicePreparedFailures(t *testing.T, ctx context.Context, fixture boundFixture) {
	t.Helper()
	statement, prepared, err := fixture.database.Prepare(ctx, correlation("native-error-prepare"), "SELECT $1::integer")
	if err != nil || statement == nil {
		t.Fatal(err)
	}
	root := serviceOwnedCall(t, fixture.inbox, prepared, statement.Close)
	receipt, err := statement.Query(ctx, correlation("native-error"), "not-an-integer")
	failed := operationResult(t, receipt, err)
	drain(t, fixture.inbox, 1)
	var native *pgconn.PgError
	if !errors.As(failed.Err(), &native) || native.Code != "22P02" {
		t.Fatal("native execution-error control did not fail")
	}
	receipt, err = statement.Query(ctx, correlation("reuse-after-error"), "42")
	result := operationResult(t, receipt, err)
	drain(t, fixture.inbox, 1)
	if result.Err() != nil || string(serviceFirst(t, result.Outcome.Value)[0]) != "42" {
		t.Fatal("native statement error prevented valid reuse", result.Err())
	}
	receipt, err = statement.Close(ctx)
	_ = operationResult(t, receipt, err)
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}

	invalidating, prepared, err := fixture.database.Prepare(ctx, correlation("invalidation-prepare"), "DEALLOCATE ALL")
	if err != nil || invalidating == nil {
		t.Fatal(err)
	}
	root = serviceOwnedCall(t, fixture.inbox, prepared, invalidating.Close)
	receipt, err = invalidating.Exec(ctx, correlation("invalidate"))
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("native invalidation failed", result.Err())
	}
	drain(t, fixture.inbox, 1)
	receipt, err = invalidating.Exec(ctx, correlation("no-reprepare"))
	failed = operationResult(t, receipt, err)
	drain(t, fixture.inbox, 1)
	if !errors.As(failed.Err(), &native) || native.Code != "26000" {
		t.Fatal("invalidated statement was transparently re-prepared")
	}
	receipt, err = invalidating.Close(ctx)
	_ = operationResult(t, receipt, err)
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
}

func verifyServiceSavepoints(t *testing.T, ctx context.Context, fixture boundFixture, reader *connection) {
	t.Helper()
	tx, receipt, err := fixture.database.Begin(ctx, correlation("savepoint-root"), TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadWrite})
	if err != nil || tx == nil {
		t.Fatal(err)
	}
	root := serviceOwnedCall(t, fixture.inbox, receipt, tx.Rollback)
	point, receipt, err := tx.Savepoint(ctx, correlation("savepoint"))
	if err != nil || point == nil {
		t.Fatal(err)
	}
	child := serviceOwnedCall(t, fixture.inbox, receipt, servicePointCleanup(point))
	receipt, err = tx.Exec(ctx, correlation("point-write"), "UPDATE core_items SET note=$1 WHERE id=1", "discard")
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, fixture.inbox, 1)
	receipt, err = tx.Exec(ctx, correlation("point-error"), "INSERT INTO core_items VALUES(1,'duplicate')")
	failed := operationResult(t, receipt, err)
	drain(t, fixture.inbox, 1)
	var native *pgconn.PgError
	if !errors.As(failed.Err(), &native) || native.Code != "23505" {
		t.Fatal("savepoint error control did not fail")
	}
	receipt, err = point.Rollback(ctx)
	if result := operationResult(t, receipt, err); result.Err() != nil || result.Outcome.Value.SavepointOutcome() != SavepointRolledBack {
		t.Fatal("real savepoint rollback/release failed", result.Err())
	}
	if err = child.Release(); err != nil {
		t.Fatal(err)
	}
	receipt, err = tx.Exec(ctx, correlation("recovered-write"), "UPDATE core_items SET note=$1 WHERE id=1", "recovered")
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal("recovered transaction remained aborted", result.Err())
	}
	drain(t, fixture.inbox, 1)
	receipt, err = tx.Commit(ctx)
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	var note string
	if err = reader.native.QueryRow(ctx, "SELECT note FROM core_items WHERE id=1").Scan(&note); err != nil || note != "recovered" {
		t.Fatal("savepoint recovery failed independent commit readback")
	}

	tx, receipt, err = fixture.database.Begin(ctx, correlation("removed-point-root"), TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadWrite})
	if err != nil || tx == nil {
		t.Fatal(err)
	}
	root = serviceOwnedCall(t, fixture.inbox, receipt, tx.Rollback)
	point, receipt, err = tx.Savepoint(ctx, correlation("removed-point"))
	if err != nil || point == nil {
		t.Fatal(err)
	}
	child = serviceOwnedCall(t, fixture.inbox, receipt, servicePointCleanup(point))
	name := point.name
	receipt, err = point.Rollback(ctx)
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = child.Release(); err != nil {
		t.Fatal(err)
	}
	receipt, err = tx.Exec(ctx, correlation("missing-point"), "ROLLBACK TO SAVEPOINT "+name)
	failed = operationResult(t, receipt, err)
	drain(t, fixture.inbox, 1)
	if !errors.As(failed.Err(), &native) || native.Code != "3B001" {
		t.Fatal("native server savepoint was not released")
	}
	receipt, err = tx.Rollback(ctx)
	_ = operationResult(t, receipt, err)
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
}
func verifyServiceSessionAndTermination(t *testing.T, ctx context.Context, fixture boundFixture) {
	t.Helper()
	before := serviceFirst(t, serviceRead(t, fixture, "SELECT pg_backend_pid(),current_setting('application_name'),current_setting('TimeZone'),current_setting('DateStyle'),current_setting('client_encoding')"))
	tx, receipt, err := fixture.database.Begin(ctx, correlation("session-root"), TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadWrite})
	if err != nil || tx == nil {
		t.Fatal(err)
	}
	root := serviceOwnedCall(t, fixture.inbox, receipt, tx.Rollback)
	for _, sql := range []string{"SET application_name='changed-fixture'", "SET TimeZone='UTC+2'", "SET DateStyle='SQL, DMY'", "SET client_encoding='LATIN1'", "CREATE TEMP TABLE reset_fixture(value integer)"} {
		receipt, err := tx.Exec(ctx, correlation("session-change"), sql)
		if result := operationResult(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, fixture.inbox, 1)
	}
	receipt, err = tx.Query(ctx, correlation("same-session"), "SELECT current_setting('application_name'),current_setting('TimeZone'),current_setting('DateStyle'),current_setting('client_encoding')")
	inside := operationResult(t, receipt, err)
	drain(t, fixture.inbox, 1)
	if inside.Err() != nil {
		t.Fatal(inside.Err())
	}
	changed := serviceFirst(t, inside.Outcome.Value)
	if string(changed[0]) != "changed-fixture" || string(changed[1]) != "UTC+2" || string(changed[2]) != "SQL, DMY" || string(changed[3]) != "LATIN1" {
		t.Fatal("retained session affinity was lost")
	}
	receipt, err = tx.Commit(ctx)
	if result := operationResult(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err = root.Release(); err != nil {
		t.Fatal(err)
	}
	after := serviceFirst(t, serviceRead(t, fixture, "SELECT pg_backend_pid(),current_setting('application_name'),current_setting('TimeZone'),current_setting('DateStyle'),current_setting('client_encoding')"))
	if !reflect.DeepEqual(before, after) {
		t.Fatal("pooled session was replaced or defaults were not restored")
	}
	if string(serviceFirst(t, serviceRead(t, fixture, "SELECT to_regclass('pg_temp.reset_fixture') IS NULL"))[0]) != "t" {
		t.Fatal("temporary session object leaked through pooling")
	}
	for _, sql := range []string{"/* end */ COMMIT AND CHAIN", "/* end */ ROLLBACK AND CHAIN"} {
		tx, receipt, err := fixture.database.Begin(ctx, correlation("chain-root"), TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadWrite})
		if err != nil || tx == nil {
			t.Fatal(err)
		}
		root := serviceOwnedCall(t, fixture.inbox, receipt, tx.Rollback)
		receipt, err = tx.Exec(ctx, correlation("chain"), sql)
		if result := operationResult(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, fixture.inbox, 1)
		if _, err = tx.Exec(ctx, correlation("after-chain"), "UPDATE core_items SET note='unowned'"); !errors.Is(err, ErrState) {
			t.Fatal("chained transaction inherited original scope authority")
		}
		if _, err = tx.Commit(ctx); !errors.Is(err, ErrState) {
			t.Fatal("chained transaction became original commit acknowledgement")
		}
		receipt, err = tx.Rollback(ctx)
		if result := operationResult(t, receipt, err); result.Outcome.Value.TransactionOutcome() != FinalizationUnknown {
			t.Fatal("cleanup certified original chained outcome")
		}
		if err = root.Release(); err != nil {
			t.Fatal(err)
		}
	}
	copyResult := queryResult(t, fixture.database, "copy-refusal", "COPY (SELECT 1) TO STDOUT")
	if !errors.Is(copyResult.Err(), ErrUnsupported) || copyResult.Outcome.Value.Complete() {
		t.Fatal("real COPY stream became empty successful query")
	}
	drain(t, fixture.inbox, 1)
}
func verifyServicePoolLifecycle(t *testing.T, ctx context.Context, options OptionsV1, reader *connection) {
	t.Helper()
	options.Name, options.MaxConnections = "parallel-pool", 2
	fixture := bindFixture(t, options, 4)
	var roots []*invocation.DeliveryRecord[Result]
	var transactions []*Transaction
	var ids []int
	for index := range 2 {
		tx, receipt, err := fixture.database.Begin(ctx, correlation("parallel-root-"+strconv.Itoa(index)), TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadOnly})
		if err != nil || tx == nil {
			t.Fatal(err)
		}
		roots = append(roots, serviceOwnedCall(t, fixture.inbox, receipt, tx.Rollback))
		transactions = append(transactions, tx)
		receipt, err = tx.Query(ctx, correlation("pid"), "SELECT pg_backend_pid()")
		result := operationResult(t, receipt, err)
		drain(t, fixture.inbox, 1)
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
		id, err := strconv.Atoi(string(serviceFirst(t, result.Outcome.Value)[0]))
		if err != nil {
			t.Fatal("invalid fixture backend ID")
		}
		ids = append(ids, id)
	}
	if ids[0] == ids[1] || fixture.database.Stats().AcquiredResources() != 2 || fixture.database.Stats().MaxResources() != 2 {
		t.Fatal("native pool statistics or connection isolation changed")
	}
	if _, err := fixture.database.Ping(ctx, correlation("full")); !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("pool admitted excess root work")
	}
	work, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	done := make(chan *invocation.Receipt[Result], 2)
	for index, tx := range transactions {
		workers.Go(func() {
			receipt, err := tx.Query(work, correlation("parallel-"+strconv.Itoa(index)), "SELECT pg_sleep(0.2)")
			if err != nil {
				t.Error("parallel query setup failed", err)
			}
			done <- receipt
		})
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		var count int
		err := reader.native.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE (pid=$1 OR pid=$2) AND state='active' AND wait_event='PgSleep'", ids[0], ids[1]).Scan(&count)
		if err != nil {
			t.Fatal("independent overlap observation failed")
		}
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no actual server overlap was observed")
		}
		time.Sleep(time.Millisecond)
	}
	for range 2 {
		if result := operationResult(t, <-done, nil); result.Err() != nil {
			t.Fatal(result.Err())
		}
	}
	workers.Wait()
	drain(t, fixture.inbox, 2)
	for index, tx := range transactions {
		receipt, err := tx.Rollback(ctx)
		if result := operationResult(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		if err := roots[index].Release(); err != nil {
			t.Fatal(err)
		}
	}
	for _, idle := range []bool{false, true} {
		expiring := options
		expiring.Name, expiring.MaxConnections = "expiry", 1
		if idle {
			expiring.MaxIdleTime = 10 * time.Millisecond
		} else {
			expiring.MaxLifetime = 100 * time.Millisecond
		}
		pool := bindFixture(t, expiring, 1)
		first := string(serviceFirst(t, serviceRead(t, pool, "SELECT pg_backend_pid()"))[0])
		if pool.database.Stats().IdleResources() != 1 {
			t.Fatal("expiry oracle did not establish an idle resource")
		}
		deadline := time.Now().Add(3 * time.Second)
		for pool.database.Stats().TotalResources() != 0 {
			if time.Now().After(deadline) {
				t.Fatal("unattended real pool expiry did not occur")
			}
			time.Sleep(10 * time.Millisecond)
		}
		second := string(serviceFirst(t, serviceRead(t, pool, "SELECT pg_backend_pid()"))[0])
		if first == second {
			t.Fatal("expired connection was reused")
		}
	}
}
