/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package mail

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestReviewedExplicitOpportunisticAndFallback(t *testing.T) {
	for _, advertised := range []bool{false, true} {
		t.Run(strconv.FormatBool(advertised), func(t *testing.T) {
			peer := newPeer(t, peerOptions{startTLS: true, noStartTLS: !advertised})
			value := testOptions(peer)
			value.TLSMode = "opportunistic"
			value.AllowPlaintext = true
			bound := bindTest(t, value, 1)
			out := sendTest(t, bound.client, "opportunistic", plainMessage(t, "opportunistic"))
			if out.Err() != nil || out.Outcome.Value.SessionInfo().TLS != advertised {
				t.Fatal("explicit opportunistic negotiation", out.Err())
			}
			drain(t, bound.inbox)
		})
	}
	peer := newPeer(t, peerOptions{startTLS: true, noStartTLS: true, auth: "plain"})
	value := testOptions(peer)
	value.TLSMode = "opportunistic"
	value.AllowPlaintext = true
	value.Auth = "plain"
	value.Username = "fixture-user"
	value.Password = "fixture-secret"
	bound := bindTest(t, value, 1)
	out := sendTest(t, bound.client, "no-plaintext-auth", plainMessage(t, "refuse"))
	if !errors.Is(out.Err(), ErrUnsupported) {
		t.Fatal("credentials permitted without TLS")
	}
	_, commands := peer.snapshot()
	for _, command := range commands {
		if command == "AUTH" || command == "MAIL" {
			t.Fatal("plaintext credential/message dispatch")
		}
	}
	drain(t, bound.inbox)
	target := newPeer(t, peerOptions{})
	value = testOptions(target)
	value.FallbackPort = value.Port
	value.Port = 1
	refusal := errors.New("controlled first transport refusal")
	attempts := 0
	value.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		attempts++
		_, port, _ := net.SplitHostPort(address)
		if port == "1" {
			return nil, refusal
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	fallback := bindTest(t, value, 1)
	out = sendTest(t, fallback.client, "fallback", plainMessage(t, "fallback"))
	if out.Err() != nil || attempts != 2 || out.Outcome.Value.SessionInfo().Port != value.FallbackPort || !out.Outcome.Value.SessionInfo().TLS {
		t.Fatal("fallback mode/endpoint", out.Err())
	}
	if failures := out.Outcome.Value.ConnectionErrors(); len(failures) != 1 || !errors.Is(failures[0], refusal) {
		t.Fatal("fallback lost initial cause")
	}
	drain(t, fallback.inbox)
}
func TestReviewedContinuationSkipsMessageSpecificRefusal(t *testing.T) {
	peer := newPeer(t, peerOptions{})
	bound := bindTest(t, testOptions(peer), 1)
	first := mustMessage(t, Content{From: "sender@example.invalid", To: []string{"用户@example.invalid"}, Text: "body"})
	second := plainMessage(t, "ascii")
	receipt, err := bound.client.SendBatchWithOptions(context.Background(), fault.Correlation{Call: "continue-refusal"}, []Message{first, second}, BatchOptions{ContinueOnError: true})
	out := resolved(t, receipt, err)
	messages := out.Outcome.Value.Messages()
	if !errors.Is(out.Err(), ErrUnsupported) || messages[0].Effect() != NotAttempted || messages[1].Effect() != Accepted || out.Attempts.Observed != 1 {
		t.Fatal("message-specific refusal stopped continuation")
	}
	captured, _ := peer.snapshot()
	if len(captured) != 1 {
		t.Fatal("refused message submitted")
	}
	drain(t, bound.inbox)
}
func TestReviewedSendmailEnvelopeOverrideRefusesBeforeStart(t *testing.T) {
	bound := localBound(t)
	for _, argument := range []string{"-r", "-rattacker@example.invalid", "-t", "-tanything", "-it", "-vit", "-fother@example.invalid", "-bs", "--read-recipients"} {
		if _, err := bound.client.Sendmail(context.Background(), fault.Correlation{Call: "refuse-command"}, plainMessage(t, "refuse"), LocalDelivery{Executable: "/nonexistent/control", Arguments: []string{argument}}); !errors.Is(err, ErrInput) {
			t.Fatal("override not rejected before process entry", argument, err)
		}
	}
}
