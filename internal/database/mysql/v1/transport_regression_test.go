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
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	sdk "github.com/go-sql-driver/mysql"
)

type allocationWitness struct {
	net.Conn
	maximum atomic.Int64
}

func (c *allocationWitness) Read(buffer []byte) (int, error) {
	for old := c.maximum.Load(); int64(len(buffer)) > old && !c.maximum.CompareAndSwap(old, int64(len(buffer))); old = c.maximum.Load() {
	}
	return c.Conn.Read(buffer)
}
func TestIncomingHeaderIsRejectedBeforeNativeAllocation(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		peer := newPeer(t, false, false)
		s := defaults(peer.options())
		s.ReadTimeout = 100 * time.Millisecond
		s.MaxPacketBytes = 1024
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		config := nativeConfig(s)
		var witness *allocationWitness
		config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			var conn net.Conn
			var err error
			if guarded {
				conn, err = openWire(ctx, s, (&net.Dialer{}).DialContext)
			} else {
				conn, err = (&net.Dialer{}).DialContext(ctx, network, address)
			}
			if err != nil {
				return nil, err
			}
			witness = &allocationWitness{Conn: conn}
			return witness, nil
		}
		connector, err := sdk.NewConnector(config)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		conn, err := connector.Connect(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		_, err = conn.(driver.QueryerContext).QueryContext(ctx, "SELECT huge_header", nil)
		_ = conn.Close()
		cancel()
		if err == nil {
			t.Fatal("header-only result unexpectedly succeeded")
		}
		if guarded && witness.maximum.Load() > 4096 {
			t.Fatal("oversized header reached native allocation")
		}
		if !guarded && witness.maximum.Load() < 8<<20 {
			t.Fatal("native allocation negative control was not reached")
		}
		t.Logf("guarded=%t maximum buffer offered by native reader=%d", guarded, witness.maximum.Load())
	}
}
func TestCertificatePinStillRequiresIdentityAndTrust(t *testing.T) {
	peer := newPeer(t, true, false)
	other := newPeer(t, true, false)
	block, _ := pem.Decode([]byte(peer.trust))
	digest := sha256.Sum256(block.Bytes)
	for _, mode := range []string{"correct", "wrong-pin", "wrong-root"} {
		options := peer.options()
		options.ServerName = ""
		options.ServerCertificateSHA256 = hex.EncodeToString(digest[:])
		if mode == "wrong-pin" {
			options.ServerCertificateSHA256 = strings.Repeat("0", 64)
		}
		if mode == "wrong-root" {
			options.RootCAPEM = other.trust
		}
		f := bindFixture(t, options, 1)
		receipt, err := f.db.Ping(context.Background(), correlation(mode))
		result := observe(t, receipt, err)
		if (result.Err() == nil) != (mode == "correct") {
			t.Fatal("certificate pin bypassed peer identity or chain verification")
		}
		drain(t, f.inbox, 1)
	}
}
func TestResultLimitPreservesIndependentDrainFailure(t *testing.T) {
	peer := newPeer(t, true, false)
	options := peer.options()
	options.MaxResultBytes = 1024
	f := bindFixture(t, options, 1)
	receipt, err := f.db.Query(context.Background(), correlation("drain"), "SELECT drain_error")
	result := observe(t, receipt, err)
	var native *sdk.MySQLError
	if !errors.Is(result.Outcome.Primary, ErrLimit) || !errors.As(result.Outcome.Cleanup, &native) || native.Number != 1213 {
		t.Fatal("retention refusal erased native row-drain failure", result.Err())
	}
	conformance.Private(t, result.Err(), "native-error-canary")
	record, err := f.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	independent, err := record.Receipt().WaitReleased(context.Background())
	if err != nil || !errors.Is(independent.Outcome.Primary, ErrLimit) || !errors.Is(independent.Outcome.Cleanup, native) {
		t.Fatal("handled direct error erased independent cleanup evidence")
	}
	if err = record.Release(); err != nil {
		t.Fatal(err)
	}
}
func TestNativeParseTimeAndPoolLimits(t *testing.T) {
	peer := newPeer(t, false, false)
	options := peer.options()
	options.ParseTime = true
	options.MaxIdleConnections = -1
	f := bindFixture(t, options, 1)
	receipt, err := f.db.Query(context.Background(), correlation("time"), "SELECT zero_date")
	result := observe(t, receipt, err)
	row, rowErr := result.Outcome.Value.First()
	if result.Err() != nil || rowErr != nil || string(row.ValuesCopy()[0]) != "0001-01-01T00:00:00Z" {
		t.Fatal("native parseTime zero-date semantics changed", result.Err())
	}
	drain(t, f.inbox, 1)
	if f.db.Stats().OpenConnections != 0 {
		t.Fatal("explicit no-idle policy retained a connection")
	}
	cfg := nativeConfig(defaults(OptionsV1{ClientFoundRows: true, ColumnsWithAlias: true, ParseTime: true}))
	if !cfg.ClientFoundRows || !cfg.ColumnsWithAlias || !cfg.ParseTime {
		t.Fatal("native result options were not applied")
	}
}
func FuzzWirePacket(f *testing.F) {
	for _, data := range [][]byte{{}, {0}, {0xfb}, {0xff, 1, 0, '#', 'H', 'Y', '0', '0', '0'}, {1}, greeting(false)} {
		f.Add(byte(0), data)
		f.Add(byte(1), data)
	}
	f.Fuzz(func(t *testing.T, phase byte, data []byte) {
		if len(data) > 64<<10 {
			return
		}
		w := &wire{settings: defaults(OptionsV1{}), phase: phase % 11, columns: 3, remaining: 3, fields: []byte{253, 8, 12}}
		_ = w.inspect(data)
	})
}

func TestAuthenticationSwitchFromDefaultGreeting(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newPeer(t, secure, false)
		peer.nativeAuth = true
		options := peer.options()
		options.Authentication = "mysql_native_password"
		fixture := bindFixture(t, options, 1)
		receipt, err := fixture.db.Ping(context.Background(), correlation("auth-switch"))
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal("explicit native authentication could not follow the server's default greeting", result.Err())
		}
		drain(t, fixture.inbox, 1)
	}
}

func TestPrepareMetadataIsValidatedBeforeNativeParsing(t *testing.T) {
	peer := newPeer(t, false, false)
	fixture := bindFixture(t, peer.options(), 1)
	statement, receipt, err := fixture.db.Prepare(context.Background(), correlation("prepare-metadata"), "SELECT malformed_metadata")
	result := observe(t, receipt, err)
	if statement != nil || !errors.Is(result.Err(), ErrProtocol) {
		t.Fatal("malformed prepared metadata reached native field decoding")
	}
	drain(t, fixture.inbox, 1)
}

func TestUnselectedAuthenticationSwitchIsRefused(t *testing.T) {
	peer := newPeer(t, true, false)
	peer.nativeAuth = true
	fixture := bindFixture(t, peer.options(), 1)
	receipt, err := fixture.db.Ping(context.Background(), correlation("auth-refusal"))
	if result := observe(t, receipt, err); !errors.Is(result.Err(), ErrProtocol) {
		t.Fatal("unselected legacy authentication was permitted")
	}
	drain(t, fixture.inbox, 1)
}

func TestEmptyPasswordAuthenticationSwitch(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newPeer(t, secure, false)
		peer.nativeAuth, peer.emptyPassword = true, true
		options := peer.options()
		options.Password, options.Authentication = "", "mysql_native_password"
		fixture := bindFixture(t, options, 1)
		receipt, err := fixture.db.Ping(context.Background(), correlation("empty-auth"))
		if result := observe(t, receipt, err); result.Err() != nil {
			t.Fatal("valid empty native authentication response refused", result.Err())
		}
		drain(t, fixture.inbox, 1)
	}
}
