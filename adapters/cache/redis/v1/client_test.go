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

package redis

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	sdk "github.com/redis/go-redis/v9"
)

func TestPublicReplyStatesMixedOwnersAndPartialEffects(t *testing.T) {
	var effects atomic.Int32
	address := peer(t, func(_ net.Conn, args []string) string {
		switch strings.ToUpper(args[0]) {
		case "SET":
			effects.Add(1)
			return "+OK\r\n"
		case "XADD":
			return "-WRONGTYPE secret-stream-canary\r\n"
		case "INCR":
			effects.Add(1)
			return ""
		}
		switch args[1] {
		case "missing":
			return "$-1\r\n"
		case "empty":
			return "$0\r\n\r\n"
		case "binary":
			return "$4\r\na\x00\xffb\r\n"
		case "zero":
			return ":0\r\n"
		case "array":
			return "*0\r\n"
		case "big":
			return "(123456789012345678901234567890\r\n"
		case "nested":
			return "*2\r\n_\r\n-ERR nested-secret\r\n"
		default:
			return "-WRONGTYPE private-canary\r\n"
		}
	})
	owner, deps := openTest(t, testSettings(address))
	view := owner.Client().Cache()
	args := []string{"SET", "owned", "original"}
	immutable := command(t, Cache, args...)
	args[0] = "AUTH"
	result, err := view.Pipeline(testContext(t), immutable, command(t, Cache, "GET", "missing"),
		command(t, Cache, "GET", "empty"), command(t, Cache, "GET", "binary"), command(t, Messaging, "XADD", "owned", "*", "f", "v"))
	if !errors.Is(err, sdk.Nil) || !errors.Is(err, ErrMessagingCommand) || effects.Load() != 1 {
		t.Fatal("mixed error owners/effects lost", err)
	}
	replies := result.Replies()
	if len(replies) != 5 || !replies[1].HasValue() || replies[1].Value().Kind() != Null || replies[4].Capability() != Messaging || replies[4].HasValue() {
		t.Fatal("presence/classification lost")
	}
	if text, ok := replies[2].Value().Text(); !ok || text != "" {
		t.Fatal("empty text lost")
	}
	bytes, ok := replies[3].Value().Bytes()
	if !ok || string(bytes) != "a\x00\xffb" {
		t.Fatal("binary lost")
	}
	bytes[0] = 'z'
	replies[0] = Reply{}
	if original, _ := result.Replies()[3].Value().Text(); original != "a\x00\xffb" {
		t.Fatal("reply alias escaped")
	}
	var server sdk.Error
	if !errors.As(result.Replies()[4].Err(), &server) {
		t.Fatal("server error lost")
	}
	ack(t, deps.Evidence, 1)
	for _, key := range []string{"zero", "array", "big", "nested"} {
		value, err := view.Execute(testContext(t), command(t, Cache, "GET", key))
		if err != nil {
			t.Fatal(err)
		}
		reply := value.Replies()[0]
		if !reply.HasValue() {
			t.Fatal("decoded value absent")
		}
		switch key {
		case "zero":
			if n, ok := reply.Value().Integer(); !ok || n != 0 {
				t.Fatal("zero lost")
			}
		case "array":
			if reply.Value().Kind() != Array || len(reply.Value().Elements()) != 0 {
				t.Fatal("empty array lost")
			}
		case "big":
			if n, ok := reply.Value().BigInteger(); !ok || n != "123456789012345678901234567890" {
				t.Fatal("integer rounded")
			}
		case "nested":
			items := reply.Value().Elements()
			if items[0].Kind() != Null || items[1].Kind() != ErrorValue || !errors.Is(items[1].Err(), ErrCommand) {
				t.Fatal("nested error lost")
			}
			items[0] = Value{}
			if reply.Value().Elements()[0].Kind() != Null {
				t.Fatal("nested alias")
			}
		}
		ack(t, deps.Evidence, 1)
	}
	lost, err := view.Execute(testContext(t), command(t, Cache, "INCR", "owned"))
	if err == nil || effects.Load() != 2 || lost.Replies()[0].State() != Unknown || lost.Replies()[0].HasValue() || lost.Replies()[0].Value().Kind() != Unavailable {
		t.Fatal("lost acknowledgement invented a value or replayed")
	}
	ack(t, deps.Evidence, 1)
}

func TestBoundsMalformedFramesAndAuthorityBeforeDispatch(t *testing.T) {
	for _, frame := range []string{"$1073741824\r\n", "*-5\r\n", "(-\r\n", "*99999999\r\n"} {
		t.Run(strings.TrimSpace(frame), func(t *testing.T) {
			var entries atomic.Int32
			address := peer(t, func(_ net.Conn, _ []string) string { entries.Add(1); return frame })
			owner, deps := openTest(t, testSettings(address))
			view := owner.Client().Cache()
			if _, err := view.Execute(testContext(t), command(t, Cache, "AUTH", "secret")); !errors.Is(err, ErrAuthority) || entries.Load() != 0 {
				t.Fatal("protocol-control escape", err)
			}
			ack(t, deps.Evidence, 1)
			value, err := view.Execute(testContext(t), command(t, Cache, "GET", "owned"))
			if err == nil || value.Replies()[0].HasValue() {
				t.Fatal("malformed frame became output")
			}
			ack(t, deps.Evidence, 1)
		})
	}
	if _, err := NewCommand(Cache, "XREAD", "STREAMS", "owned", "0"); !errors.Is(err, ErrInput) {
		t.Fatal("Streams mislabeled cache")
	}
	if _, err := NewCommand("", "GET", "owned"); err == nil {
		t.Fatal("implicit semantic owner")
	}
	if _, err := NewCommand(Cache, "SET", "owned", strings.Repeat("x", 16<<20)); !errors.Is(err, ErrLimit) {
		t.Fatal("constructor bound bypassed")
	}
}

func TestSaturatedEvidenceCleanupAndSharedViews(t *testing.T) {
	var entries atomic.Int32
	address := peer(t, func(_ net.Conn, _ []string) string { entries.Add(1); return "+OK\r\n" })
	value := testSettings(address)
	policy, err := Recommend(value)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[Result](adapters.EvidenceOptions{Capacity: 2, MaxBytes: policy.Evidence.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := Open(context.Background(), value, Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Client().Cache().Execute(testContext(t), command(t, Cache, "SET", "owned", "first")); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Client().Messaging().Execute(testContext(t), command(t, Messaging, "PUBLISH", "channel", "second")); !errors.Is(err, adapters.ErrEvidence) || entries.Load() != 1 {
		t.Fatal("view minted evidence allowance", err)
	}
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("saturated cleanup needed admission", err)
	}
	ack(t, inbox, 2)
}
