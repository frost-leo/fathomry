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
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	sdk "github.com/go-sql-driver/mysql"
)

func TestSQLRejectsUnownedGreetingExtensions(t *testing.T) {
	for _, secure := range []bool{false, true} {
		for _, extensions := range []uint32{0, 1 << 4, ^uint32(0)} {
			peer := newSQLPeer(t, secure)
			peer.extensions.Store(extensions)
			f := bindFixture(t, peer.options(), 1)
			receipt, err := f.client.Query(context.Background(), correlation("greeting-extensions"), "SELECT exact")
			result := observe(t, receipt, err)
			drain(t, f.inbox, 1)
			if extensions == 0 {
				if result.Err() != nil || !result.Outcome.Value.Complete() {
					t.Fatal("ordinary Doris greeting rejected", result.Err())
				}
			} else if !errors.Is(result.Err(), ErrUnsupported) || result.Outcome.Value.Dispatched() || peer.queries.Load() != 0 {
				t.Fatal("unsupported result mode reached native command execution")
			}
		}
	}
	for offset := range 10 {
		greeting := peerGreeting(false)
		capabilities := bytes.IndexByte(greeting[1:], 0) + 15
		greeting[capabilities+8+offset] = 1
		w := wire{settings: defaults(OptionsV1{Plaintext: true})}
		if !errors.Is(w.greeting(greeting), ErrUnsupported) {
			t.Fatal("nonzero reserved greeting byte accepted", offset)
		}
	}
}

func TestSQLCancellationClosesSocketAndPreservesUncertainty(t *testing.T) {
	peer := newSQLPeer(t, true)
	f := bindFixture(t, peer.options(), 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan invocationResult, 1)
	go func() {
		r, e := f.client.Query(ctx, correlation("cancel"), "SELECT stalled")
		done <- invocationResult{r, e}
	}()
	select {
	case <-peer.entered:
	case <-time.After(time.Second):
		t.Fatal("query not entered")
	}
	cancel()
	select {
	case call := <-done:
		result := observe(t, call.receipt, call.err)
		if !errors.Is(result.Err(), context.Canceled) || result.Outcome.Value.Complete() || !result.Outcome.Value.Dispatched() {
			t.Fatal("cancellation lost evidence")
		}
	case <-time.After(time.Second):
		t.Fatal("query/cleanup hung")
	}
	drain(t, f.inbox, 1)
}

func TestSQLUntrustedTLSFailsBeforeCommand(t *testing.T) {
	peer := newSQLPeer(t, true)
	o := peer.options()
	o.SQLServerName = "wrong.invalid"
	f := bindFixture(t, o, 1)
	receipt, err := f.client.Query(context.Background(), correlation("identity"), "SELECT exact")
	result := observe(t, receipt, err)
	if result.Err() == nil || peer.queries.Load() != 0 || result.Outcome.Value.Dispatched() {
		t.Fatal("untrusted SQL endpoint dispatched")
	}
	drain(t, f.inbox, 1)
}

func TestInitialServerErrorPreservesNativeIdentityWithoutDispatch(t *testing.T) {
	peer := newSQLPeer(t, false)
	peer.denyGreeting.Store(true)
	fixture := bindFixture(t, peer.options(), 1)
	receipt, err := fixture.client.Query(context.Background(), correlation("greeting-error"), "SELECT 1")
	result := observe(t, receipt, err)
	var native *sdk.MySQLError
	if !errors.As(result.Err(), &native) || native.Number != 1040 || result.Outcome.Value.Dispatched() || peer.queries.Load() != 0 {
		t.Fatal("initial server rejection lost native cause or dispatched SQL")
	}
	drain(t, fixture.inbox, 1)
}
