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

package doris

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/go-sql-driver/mysql"
)

func TestNativeSQLTextTLSExactValuesAndIsolation(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newSQLPeer(t, secure)
		peer.switchAuth.Store(true)
		f := bindFixture(t, peer.options(), 1)
		for range 2 {
			receipt, err := f.client.Query(context.Background(), correlation("exact"), "SELECT exact")
			result := observe(t, receipt, err)
			if result.Err() != nil || !result.Outcome.Value.Complete() {
				t.Fatalf("native query failed: %v", result.Err())
			}
			values := result.Outcome.Value.RowsCopy()[0].ValuesCopy()
			if values[0] != nil || values[1] == nil || len(values[1]) != 0 || string(values[2]) != "18446744073709551615" ||
				string(values[3]) != "12345678901234567890.00100" || values[5][2] != 255 ||
				string(values[4]) != "2026-09-14 01:02:03.123456" || string(values[6]) != `{"nested":[null,18446744073709551615]}` {
				t.Fatal("lossy or ambiguous values")
			}
			values[2][0] = 'X'
			if string(result.Outcome.Value.RowsCopy()[0].ValuesCopy()[2]) != "18446744073709551615" {
				t.Fatal("aliased result")
			}
			drain(t, f.inbox, 1)
		}
		if secure && peer.handshakes.Load() != 2 {
			t.Fatal("SQL connection was reused or TLS bypassed")
		}
	}
}
func TestSQLBoundsPartialNativeErrorsAndNoRetry(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newSQLPeer(t, secure)
		o := peer.options()
		o.MaxRows = 1
		o.MaxResultBytes = 1024
		o.MaxPacketBytes = 4096
		f := bindFixture(t, o, 1)
		for _, test := range []struct {
			query string
			kind  error
			rows  int
		}{
			{"huge", ErrLimit, 0}, {"wide", ErrLimit, 0}, {"large", ErrLimit, 0}, {"over", ErrLimit, 1},
			{"malformed-column", ErrProtocol, 0}, {"column-error", ErrProtocol, 0}, {"malformed-row", ErrProtocol, 0}, {"false-eof", ErrProtocol, 0},
			{"infile", ErrUnsupported, 0}, {"more", ErrUnsupported, 0}, {"partial", ErrSQL, 1},
		} {
			before := peer.queries.Load()
			receipt, err := f.client.Query(context.Background(), correlation("reject"), "SELECT "+test.query)
			result := observe(t, receipt, err)
			if !errors.Is(result.Err(), test.kind) || result.Outcome.Value.Complete() || len(result.Outcome.Value.RowsCopy()) != test.rows {
				t.Fatalf("%s: error=%v complete=%t rows=%d", test.query, result.Err(), result.Outcome.Value.Complete(), len(result.Outcome.Value.RowsCopy()))
			}
			if peer.queries.Load() != before+1 {
				t.Fatal("SQL silently retried")
			}
			drain(t, f.inbox, 1)
		}
		receipt, err := f.client.Exec(context.Background(), correlation("lost"), "INSERT lost")
		result := observe(t, receipt, err)
		if result.Err() == nil || !result.Outcome.Value.Dispatched() || result.Outcome.Value.SQLAcknowledged() {
			t.Fatal("lost reply promoted to acknowledgement or no dispatch")
		}
		drain(t, f.inbox, 1)
	}
}

type invocationResult struct {
	receipt *invocation.Receipt[Result]
	err     error
}

func TestNativeErrorIdentityAndSQLInputBounds(t *testing.T) {
	peer := newSQLPeer(t, false)
	f := bindFixture(t, peer.options(), 1)
	receipt, err := f.client.Query(context.Background(), correlation("native"), "SELECT partial")
	result := observe(t, receipt, err)
	var native *sdk.MySQLError
	if !errors.As(result.Err(), &native) || native.Number != 1077 || string(native.SQLState[:]) != "HY000" {
		t.Fatal("native code/state lost")
	}
	conformance.Private(t, result.Err(), "native-error-canary")
	drain(t, f.inbox, 1)
	for _, query := range []string{"", strings.Repeat("x", MaxSQLBytes+1), "SELECT\x00"} {
		before := peer.queries.Load()
		if receipt, err := f.client.Query(context.Background(), correlation("input"), query); receipt != nil || !errors.Is(err, ErrInput) || peer.queries.Load() != before {
			t.Fatal("invalid SQL dispatched")
		}
	}
}

func TestNativeFalseEOFControlWithoutFraming(t *testing.T) {
	peer := newSQLPeer(t, false)
	peer.allowUnbounded.Store(true)
	cfg := sdk.NewConfig()
	cfg.Net, cfg.Addr = "tcp", peer.listener.Addr().String()
	cfg.User, cfg.Passwd, cfg.DBName = "synthetic", "credential-canary", "gh42"
	cfg.MaxAllowedPacket = MaxSQLBytes + 1024
	cfg.Logger = &sdk.NopLogger{}
	connector, err := sdk.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal("native control connection failed", err)
	}
	defer conn.Close()
	rows, err := conn.(driver.QueryerContext).QueryContext(ctx, "SELECT false-eof", nil)
	if err != nil {
		t.Fatal("native control did not reach row parser", err)
	}
	defer rows.Close()
	data := &resultData{}
	if err := consume(rows, defaults(peer.options()), data); err != nil || len(data.rows) != 0 {
		t.Fatal("native false-EOF counterexample changed")
	}
	t.Log("unframed driver returned complete empty rows for the malformed row; framed rejecting control is separate")
}

func TestSQLAcknowledgementAndAggregateResponseLimits(t *testing.T) {
	peer := newSQLPeer(t, true)
	options := peer.options()
	options.MaxResponseBytes = 1024
	fixture := bindFixture(t, options, 1)
	receipt, err := fixture.client.Query(context.Background(), correlation("response-limit"), "SELECT large")
	result := observe(t, receipt, err)
	if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() {
		t.Fatal("aggregate response bytes were not bounded")
	}
	drain(t, fixture.inbox, 1)
	receipt, err = fixture.client.Exec(context.Background(), correlation("count-overflow"), "INSERT overflow")
	result = observe(t, receipt, err)
	_, known := result.Outcome.Value.RowsAffected()
	if !errors.Is(result.Err(), ErrProtocol) || known || !result.Outcome.Value.SQLAcknowledged() || result.Outcome.Value.Complete() {
		t.Fatal("affected-row overflow invented a valid count or erased acknowledgement")
	}
	drain(t, fixture.inbox, 1)
	for _, query := range []string{"CREATE TABLE fixture (value INT)", "ALTER TABLE fixture ADD COLUMN other INT", "INSERT INTO fixture VALUES (1)", "UPDATE fixture SET value=2 WHERE value=1", "DELETE FROM fixture WHERE value=2", "MERGE INTO fixture USING other ON fixture.value=other.value WHEN MATCHED THEN DELETE", "DROP TABLE fixture"} {
		receipt, err = fixture.client.Exec(context.Background(), correlation("authorized-sql"), query)
		result = observe(t, receipt, err)
		if result.Err() != nil || !result.Outcome.Value.SQLAcknowledged() {
			t.Fatal("authorized text SQL was filtered", result.Err())
		}
		drain(t, fixture.inbox, 1)
	}
}

func TestFloatWidthsAndEmptyPasswordAuthenticationSwitch(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newSQLPeer(t, secure)
		peer.emptyPassword.Store(true)
		peer.switchAuth.Store(true)
		fixture := bindFixture(t, peer.options(), 1)
		receipt, err := fixture.client.Query(context.Background(), correlation("empty-auth-floats"), "SELECT floats")
		result := observe(t, receipt, err)
		if result.Err() != nil || !result.Outcome.Value.Complete() {
			t.Fatal("legal authentication/float result rejected", result.Err())
		}
		values := result.Outcome.Value.RowsCopy()[0].ValuesCopy()
		if string(values[0]) != "0.1" || string(values[1]) != "0.1" {
			t.Fatal("FLOAT precision was widened or DOUBLE lost precision")
		}
		drain(t, fixture.inbox, 1)
	}
}
