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
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	smtp "github.com/wneessen/go-mail/smtp"
)

func TestSMTPRecipientAndDataEffects(t *testing.T) {
	for _, test := range []struct {
		name             string
		peer             peerOptions
		effect           Effect
		stage            Stage
		primary, cleanup error
		code             int
		captured         int
	}{
		{"accepted", peerOptions{}, Accepted, StageAck, nil, nil, 250, 1},
		{"rcpt-rejected", peerOptions{rejectRecipient: 2}, NotAccepted, StageRecipient, ErrSend, nil, 550, 0},
		{"data-rejected", peerOptions{dataReply: "554 5.6.0 body rejected"}, NotAccepted, StageAck, ErrSend, nil, 554, 1},
		{"ack-lost", peerOptions{dropAck: true}, Unknown, StageAck, ErrSend, nil, 0, 1},
		{"reset-failed", peerOptions{resetReply: "500 5.5.1 reset failed"}, Accepted, StageAck, nil, ErrCleanup, 250, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer := newPeer(t, test.peer)
			bound := bindTest(t, testOptions(peer), 1)
			message, err := NewMessage(mailContent(t))
			if err != nil {
				t.Fatal(err)
			}
			got := sendTest(t, bound.client, test.name, message)
			delivery := got.Outcome.Value.Messages()[0]
			if delivery.Effect() != test.effect || delivery.Stage() != test.stage || delivery.Code() != test.code ||
				!sameError(got.Outcome.Primary, test.primary) || !sameError(got.Outcome.Cleanup, test.cleanup) {
				t.Fatal("effect/stage/error dimensions changed")
			}
			captured, _ := peer.snapshot()
			if len(captured) != test.captured {
				t.Fatal("result disagrees with independent SMTP oracle")
			}
			if len(captured) > 0 {
				if captured[0].sender != "<bounce@fixture.test>" || len(captured[0].recipients) != 3 || captured[0].recipients[2] != "<hidden@fixture.test>" {
					t.Fatal("envelope association changed")
				}
				parsed, _ := parseMIME(t, captured[0].data)
				if parsed.Header.Get("Bcc") != "" {
					t.Fatal("BCC disclosed in delivered MIME")
				}
			}
			recipients := delivery.Recipients()
			if !recipients[0].Accepted() {
				t.Fatal("first RCPT acceptance was lost")
			}
			if test.name == "rcpt-rejected" {
				if recipients[1].Accepted() || recipients[1].Code() != 550 || recipients[1].EnhancedCode() != "5.1.1" || recipients[2].Attempted() {
					t.Fatal("RCPT-stage facts conflated")
				}
				conformance.Cause(t, got.Err(), func(err *textproto.Error) bool { return err.Code == 550 })
			}
			recipients[0] = Recipient{}
			if !delivery.Recipients()[0].Accepted() {
				t.Fatal("recipient snapshot aliases caller")
			}
			entries := got.Outcome.Value.Messages()
			entries[0] = Delivery{}
			if got.Outcome.Value.Messages()[0].ID() != "message@fixture.test" {
				t.Fatal("batch snapshot aliases caller")
			}
			conformance.Private(t, got.Err(), "fixture private rejection", "hidden@fixture.test")
			drain(t, bound.inbox)
		})
	}
}
func sameError(got, want error) bool {
	if want == nil {
		return got == nil
	}
	return errors.Is(got, want)
}
func TestSMTPBatchStopsWithoutResendAndPreservesPositions(t *testing.T) {
	peer := newPeer(t, peerOptions{rejectMessage: 2})
	bound := bindTest(t, testOptions(peer), 1)
	got := sendTest(t, bound.client, "batch", plainMessage(t, "first"), plainMessage(t, "second"), plainMessage(t, "third"))
	messages := got.Outcome.Value.Messages()
	if len(messages) != 3 || messages[0].Effect() != Accepted || messages[1].Effect() != NotAccepted ||
		messages[2].Effect() != NotAttempted || messages[2].ID() != "third@fixture.test" || got.Attempts.Observed != 2 || !got.Attempts.Exact {
		t.Fatal("partial batch facts changed")
	}
	captured, commands := peer.snapshot()
	if len(captured) != 1 || peer.connections.Load() != 1 || strings.Count(strings.Join(commands, " "), "MAIL") != 2 {
		t.Fatal("failure triggered hidden retries")
	}
	drain(t, bound.inbox)
}
func TestSMTPValidationAfterPartialBatchDoesNotEraseFirstAcceptance(t *testing.T) {
	peer := newPeer(t, peerOptions{})
	options := testOptions(peer)
	options.MaxMIMEBytes = 1024
	bound := bindTest(t, options, 1)
	content := mailContent(t)
	bad, err := NewMessage(content)
	if err != nil {
		t.Fatal(err)
	}
	got := sendTest(t, bound.client, "partial-mime", plainMessage(t, "first"), bad)
	messages := got.Outcome.Value.Messages()
	if !errors.Is(got.Err(), ErrLimit) || messages[0].Effect() != Accepted || messages[1].Effect() != NotAttempted || messages[1].Stage() != StageCompose {
		t.Fatal("partial preparation facts lost")
	}
	captured, _ := peer.snapshot()
	if len(captured) != 1 {
		t.Fatal("oversized MIME reached DATA")
	}
	drain(t, bound.inbox)
}
func TestSMTPCancellationJoinsSocket(t *testing.T) {
	peer := newPeer(t, peerOptions{stall: "DATA"})
	options := testOptions(peer)
	options.Timeout = 5 * time.Second
	bound := bindTest(t, options, 2)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	canceled := errors.New("fixture cancel cause")
	message := plainMessage(t, "cancel")
	done := make(chan error, 1)
	go func() {
		receipt, err := bound.client.Send(ctx, fault.Correlation{Call: "cancel"}, message)
		if err != nil {
			done <- err
			return
		}
		result, present := receipt.Result()
		if !present {
			done <- errors.New("fixture result missing")
			return
		}
		done <- result.Err()
	}()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case command := <-peer.reached:
			if command == "DATA" {
				cancel(canceled)
				goto waiting
			}
		case <-deadline.C:
			t.Fatal("DATA never entered")
		}
	}
waiting:
	select {
	case gotErr := <-done:
		if !errors.Is(gotErr, context.Canceled) || !errors.Is(gotErr, canceled) {
			t.Fatal("cancellation causes lost")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not terminate native I/O")
	}
	drain(t, bound.inbox)
}
func TestSMTPReplyBoundsBeforeNativeAllocation(t *testing.T) {
	peer := newPeer(t, peerOptions{greeting: "220 " + strings.Repeat("x", 16384)})
	options := testOptions(peer)
	options.MaxReplyBytes = 4096
	bound := bindTest(t, options, 1)
	got := sendTest(t, bound.client, "reply-bound", plainMessage(t, "reply-bound"))
	if !errors.Is(got.Err(), ErrLimit) || got.Outcome.Value.Messages()[0].Effect() != NotAttempted {
		t.Fatal("unbounded native reply accepted")
	}
	drain(t, bound.inbox)
}

type failWriteConn struct {
	net.Conn
	fail     bool
	sentinel error
}

func (conn *failWriteConn) Write(data []byte) (int, error) {
	if conn.fail {
		return 0, conn.sentinel
	}
	return conn.Conn.Write(data)
}

// Reproduce the native terminator-flush defect independently of TCP timing.
func TestNativeDataCloseIgnoresFlushFailure(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		t.Run(map[bool]string{false: "native-counterexample", true: "provider-control"}[controlled], func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			_ = client.SetDeadline(time.Now().Add(time.Second))
			_ = server.SetDeadline(time.Now().Add(time.Second))
			sentinel := errors.New("fixture flush failure")
			wire := &failWriteConn{Conn: client, sentinel: sentinel}
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = io.WriteString(server, "220 fixture\r\n")
				reader := textproto.NewReader(bufio.NewReader(server))
				for {
					line, err := reader.ReadLine()
					if err != nil {
						return
					}
					switch {
					case strings.HasPrefix(line, "EHLO"):
						_, _ = io.WriteString(server, "250 fixture\r\n")
					case strings.HasPrefix(line, "MAIL"), strings.HasPrefix(line, "RCPT"):
						_, _ = io.WriteString(server, "250 ok\r\n")
					case line == "DATA":
						_, _ = io.WriteString(server, "354 ready\r\n")
						// An unsolicited response exposes native Close's discarded write error.
						_, _ = io.WriteString(server, "250 2.0.0 premature\r\n")
						return
					}
				}
			}()
			native, err := smtp.NewClient(wire, "fixture.test")
			if err != nil {
				t.Fatal(err)
			}
			if err = native.Mail("<from@fixture.test>"); err != nil {
				t.Fatal(err)
			}
			if err = native.Rcpt("<to@fixture.test>"); err != nil {
				t.Fatal(err)
			}
			writer, err := native.Data()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = writer.Write([]byte("tiny body")); err != nil {
				t.Fatal(err)
			}
			wire.fail = true
			if controlled {
				_, _, err = finishData(writer, native)
				if !errors.Is(err, sentinel) {
					t.Fatal("controlled DATA flush lost original failure")
				}
			} else if err = writer.Close(); err != nil {
				t.Fatal("native defect counterexample changed")
			}
			_ = client.Close()
			<-done
		})
	}
}
