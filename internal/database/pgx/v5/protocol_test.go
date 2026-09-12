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
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestCopyResponsesDoNotBecomeEmptyQuerySuccess(t *testing.T) {
	for _, secure := range []bool{false, true} {
		peer := newProtocolPeer(t, secure)
		options := peer.options()
		config, err := nativeConfig(defaults(options))
		if err != nil {
			t.Fatal(err)
		}
		config.BuildFrontend = pgproto3.NewFrontend
		native, err := connect(context.Background(), config, options.RootCAPEM)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := native.native.Query(context.Background(), "COPY fixture TO STDOUT")
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if rows.Err() != nil || count != 0 || rows.CommandTag().String() != "COPY 1" {
			t.Fatal("native silent-COPY control changed")
		}
		if err = native.close(context.Background()); err != nil {
			t.Fatal(err)
		}
		fixture := bindFixture(t, options, 1)
		result := queryResult(t, fixture.database, "copy", "COPY fixture TO STDOUT")
		if !errors.Is(result.Err(), ErrUnsupported) || result.Outcome.Value.Complete() {
			t.Fatal("COPY stream was certified as an empty ordinary query")
		}
		drain(t, fixture.inbox, 1)
	}
}

func TestSessionResetFailureIsIndependentCleanup(t *testing.T) {
	peer := newProtocolPeer(t, false)
	peer.failReset.Store(true)
	fixture := bindFixture(t, peer.options(), 1)
	result := queryResult(t, fixture.database, "reset", "SELECT cells")
	var native *pgconn.PgError
	if result.Outcome.Primary != nil || !result.Outcome.Value.Complete() || !errors.As(result.Outcome.Cleanup, &native) || native.Code != "42501" {
		t.Fatal("successful query lost independent reset failure", result.Err())
	}
	if fixture.database.Stats().TotalResources() != 0 {
		t.Fatal("reset-failed connection was reused")
	}
	drain(t, fixture.inbox, 1)
}

func TestNativePoolStatisticsPingAndUnattendedExpiration(t *testing.T) {
	for _, idle := range []bool{false, true} {
		peer := newProtocolPeer(t, false)
		options := peer.options()
		if idle {
			options.MaxIdleTime = 10 * time.Millisecond
		} else {
			options.MaxLifetime = 10 * time.Millisecond
		}
		fixture := bindFixture(t, options, 1)
		before := fixture.database.Stats()
		receipt, err := fixture.database.Ping(context.Background(), correlation("ping"))
		if result := operationResult(t, receipt, err); result.Err() != nil || !result.Outcome.Value.Complete() {
			t.Fatal(result.Err())
		}
		drain(t, fixture.inbox, 1)
		if before.TotalResources() != 0 || fixture.database.Stats().AcquireCount() != 0 {
			t.Fatal("native snapshots or counter scope changed")
		}
		deadline := time.Now().Add(3 * time.Second)
		for fixture.database.Stats().TotalResources() != 0 {
			if time.Now().After(deadline) {
				t.Fatal("unattended native expiration did not finish")
			}
			time.Sleep(10 * time.Millisecond)
		}
		receipt, err = fixture.database.Ping(context.Background(), correlation("replacement"))
		if result := operationResult(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, fixture.inbox, 1)
		if peer.connects.Load() != 2 || fixture.inbox.Usage() != (invocation.InboxUsage{}) {
			t.Fatal("expired connection not replaced exactly once")
		}
	}
}

func FuzzProtocolFence(f *testing.F) {
	f.Add([]byte{'Z', 0, 0, 0, 5, 'I'}, uint8(1))
	f.Add([]byte{'H', 0, 0, 0, 7, 0, 0, 0}, uint8(7))
	f.Add([]byte{'T', 0, 0, 0, 4}, uint8(5))
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		if len(data) > 64<<10 {
			return
		}
		fence := &copyFence{reader: bytes.NewReader(data)}
		buffer := make([]byte, int(chunk)%64+1)
		read := 0
		for {
			count, err := fence.Read(buffer)
			if count < 0 || count > len(buffer) {
				t.Fatal("invalid framed read count")
			}
			read += count
			if read > len(data) {
				t.Fatal("framing created data")
			}
			if err != nil {
				break
			}
			if count == 0 {
				t.Fatal("framing made no progress")
			}
		}
		if len(data) >= 5 && data[0] == 'H' && data[1] == 0 && data[2] == 0 && data[3] == 0 && data[4] >= 4 {
			reader := &copyFence{reader: bytes.NewReader(data)}
			output, err := io.ReadAll(reader)
			if len(output) != 0 || !errors.Is(err, ErrUnsupported) {
				t.Fatal("COPY response escaped framing")
			}
		}
	})
}
