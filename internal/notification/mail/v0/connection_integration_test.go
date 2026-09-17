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

package mail

import (
	"context"
	"crypto/x509"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestConnectionTLSAuthReuse(t *testing.T) {
	for _, mode := range []string{"implicit", "starttls"} {
		for _, auth := range []string{"none", "plain", "login", "xoauth2"} {
			t.Run(mode+"-"+auth, func(t *testing.T) {
				peer := newPeer(t, peerOptions{startTLS: mode == "starttls", auth: auth})
				options := testOptions(peer)
				options.TLSMode = mode
				options.Auth = auth
				if auth != "none" {
					options.Username = "fixture-user"
					options.Password = "fixture-secret"
				}
				bound := bindTest(t, options, 3)
				message := plainMessage(t, "reuse")
				for index := range 3 {
					got := sendTest(t, bound.client, "reuse-"+strconv.Itoa(index), message)
					if got.Err() != nil || got.Outcome.Value.Messages()[0].Effect() != Accepted {
						t.Fatalf("send failed: %v", got.Err())
					}
				}
				if peer.connections.Load() != 1 {
					t.Fatal("successful calls did not reuse one connection")
				}
				drain(t, bound.inbox)
			})
		}
	}
}
func TestConnectionRefusesUnverifiedOrUnavailablePolicy(t *testing.T) {
	for _, mode := range []string{"untrusted", "wrong-host", "no-starttls", "no-auth", "auth-rejected"} {
		t.Run(mode, func(t *testing.T) {
			peer := newPeer(t, peerOptions{wrongHostname: mode == "wrong-host", startTLS: mode == "no-starttls", noStartTLS: true, auth: "plain", rejectAuth: mode == "auth-rejected"})
			options := testOptions(peer)
			switch mode {
			case "untrusted":
				options.RootCAPEM = ""
			case "no-starttls":
				options.TLSMode = "starttls"
			case "no-auth":
				options.Auth = "login"
				options.Username = "fixture-user"
				options.Password = "fixture-secret"
			case "auth-rejected":
				options.Auth = "plain"
				options.Username = "fixture-user"
				options.Password = "fixture-secret"
			}
			bound := bindTest(t, options, 1)
			got := sendTest(t, bound.client, "refusal", plainMessage(t, "refusal"))
			if got.Err() == nil || got.Outcome.Value.Messages()[0].Effect() != NotAttempted {
				t.Fatal("unavailable policy was accepted")
			}
			if mode == "wrong-host" {
				var mismatch x509.HostnameError
				if !errors.As(got.Err(), &mismatch) {
					t.Fatal("hostname rejection lost its native cause")
				}
			}
			captured, commands := peer.snapshot()
			if len(captured) != 0 {
				t.Fatal("policy refusal sent DATA")
			}
			for _, command := range commands {
				if command == "MAIL" {
					t.Fatal("policy refusal reached MAIL")
				}
			}
			drain(t, bound.inbox)
		})
	}
}
func TestConnectionQuitFailureStillCloses(t *testing.T) {
	peer := newPeer(t, peerOptions{quitReply: "500 fixture quit failure"})
	bound := bindTest(t, testOptions(peer), 1)
	got := sendTest(t, bound.client, "quit", plainMessage(t, "quit"))
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, bound.inbox)
	bound.closeWant = ErrCleanup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := bound.assembly.Close(ctx); !errors.Is(err, ErrCleanup) {
		t.Fatal("QUIT cause was lost")
	}
	if got.Outcome.Value.Messages()[0].Effect() != Accepted {
		t.Fatal("QUIT changed prior acceptance")
	}
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for peer.active.Load() != 0 {
		select {
		case <-deadline:
			t.Fatal("QUIT failure leaked the native connection")
		case <-ticker.C:
		}
	}
}
func TestCleanupTimeoutCause(t *testing.T) {
	peer := newPeer(t, peerOptions{stall: "QUIT"})
	options := testOptions(peer)
	options.CloseTimeout = 20 * time.Millisecond
	bound := bindTest(t, options, 1)
	if got := sendTest(t, bound.client, "cleanup-budget", plainMessage(t, "cleanup-budget")); got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, bound.inbox)
	bound.closeWant = ErrCleanup
	err := bound.assembly.Close(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("source cleanup lost its own phase deadline cause")
	}
}
func TestHelloAddressLiterals(t *testing.T) {
	for _, value := range []struct{ input, wire string }{
		{"127.0.0.1", "[127.0.0.1]"}, {"::1", "[IPv6:::1]"}, {"worker.fixture.test", "worker.fixture.test"},
	} {
		t.Run(value.input, func(t *testing.T) {
			peer := newPeer(t, peerOptions{})
			options := testOptions(peer)
			options.Hello = value.input
			bound := bindTest(t, options, 1)
			if got := sendTest(t, bound.client, "hello", plainMessage(t, "hello")); got.Err() != nil {
				t.Fatal(got.Err())
			}
			peer.mu.Lock()
			hellos := append([]string(nil), peer.hellos...)
			peer.mu.Unlock()
			if len(hellos) != 1 || hellos[0] != value.wire {
				t.Fatal("EHLO domain or address literal was malformed")
			}
			drain(t, bound.inbox)
		})
	}
}
