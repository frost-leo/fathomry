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
	"bytes"
	"context"
	"errors"
	"github.com/frost-leo/fathomry/internal/fault"
	sdk "github.com/wneessen/go-mail"
	"io"
	"mime"
	"mime/multipart"
	stdmail "net/mail"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPGPNativeCounterexampleAndManagedStructure(t *testing.T) {
	for _, kind := range []int{1, 2} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			parts := []Part{{ContentType: "application/pgp-encrypted", Data: []byte("Version: 1")}, {ContentType: "application/octet-stream", Data: []byte("opaque ciphertext")}}
			if kind == 2 {
				parts = []Part{{ContentType: "text/plain", Data: []byte("signed body")}, {ContentType: "application/pgp-signature", Data: []byte("opaque signature")}}
			}
			native := sdk.NewMsg(sdk.WithPGPType(sdk.PGPType(kind)), sdk.WithBoundary("fixture-boundary"))
			native.SetBodyString(sdk.ContentType(parts[0].ContentType), string(parts[0].Data))
			native.AddAlternativeString(sdk.ContentType(parts[1].ContentType), string(parts[1].Data))
			var malformed bytes.Buffer
			if _, err := native.WriteTo(&malformed); err != nil {
				t.Fatal(err)
			}
			parsed, err := stdmail.ReadMessage(&malformed)
			if err != nil {
				t.Fatal(err)
			}
			_, params, parseErr := mime.ParseMediaType(parsed.Header.Get("Content-Type"))
			if parseErr == nil {
				reader := multipart.NewReader(parsed.Body, params["boundary"])
				for {
					part, err := reader.NextPart()
					if err != nil {
						parseErr = err
						break
					}
					if _, err = io.Copy(io.Discard, part); err != nil {
						parseErr = err
						break
					}
				}
				if errors.Is(parseErr, io.EOF) {
					t.Fatal("native PGP counterexample no longer reproduces")
				}
			}
			if parseErr == nil {
				t.Fatal("native malformed structure accepted")
			}
			message := mustMessage(t, Content{PGP: kind, PGPHash: "pgp-sha256", Parts: parts})
			wire, err := compose(context.Background(), message, defaults(OptionsV1{}))
			if err != nil {
				t.Fatal(err)
			}
			parsed, decoded := parseMIME(t, wire)
			media, params, err := mime.ParseMediaType(parsed.Header.Get("Content-Type"))
			if err != nil {
				t.Fatal(err)
			}
			if len(decoded) != 2 || kind == 1 && media != "multipart/encrypted" || kind == 2 && (media != "multipart/signed" || params["micalg"] != "pgp-sha256") {
				t.Fatal("managed PGP envelope")
			}
			for index := range parts {
				if strings.TrimSpace(string(decoded[index].data)) != string(parts[index].Data) {
					t.Fatal("PGP opaque payload changed")
				}
			}
		})
	}
}
func TestScopedWriterCannotEscapeOrSuppressOverflow(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	var escaped io.Writer
	message := prepareTest(t, bound.client, plainMessage(t, "writer"), Preparation{Parts: []PartInput{{Part: Part{ContentType: "text/plain"}, Input: Input{Write: func(_ context.Context, writer io.Writer) error {
		escaped = writer
		_, err := io.WriteString(writer, "bounded body")
		return err
	}}}}})
	before := message.EMLCopy()
	if _, err := escaped.Write([]byte("late")); err == nil || !bytes.Equal(before, message.EMLCopy()) {
		t.Fatal("writer escaped its scope")
	}
	receipt, err := bound.client.PrepareMessage(context.Background(), fault.Correlation{Call: "overflow"}, plainMessage(t, "overflow"), Preparation{Parts: []PartInput{{Part: Part{ContentType: "text/plain"}, Input: Input{Write: func(_ context.Context, writer io.Writer) error {
		_, _ = writer.Write(bytes.Repeat([]byte("x"), 65<<10))
		return nil
	}}}}})
	if result := resolved(t, receipt, err); !errors.Is(result.Err(), ErrLimit) {
		t.Fatal("callback suppressed writer overflow")
	}
}
func TestSendmailCancellationAndExitFailureJoin(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	directory := t.TempDir()
	command := filepath.Join(directory, "fake-sendmail")
	if err := os.WriteFile(command, []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	receipt, err := bound.client.Sendmail(ctx, fault.Correlation{Call: "cancel-process"}, plainMessage(t, "process"), LocalDelivery{Executable: command})
	out := resolved(t, receipt, err)
	process := out.Outcome.Value.ProcessInfo()
	if !process.Started || !process.ExitKnown || !errors.Is(out.Err(), context.DeadlineExceeded) || process.ExitCode == 0 {
		t.Fatal("process cancellation/join facts")
	}
	if err := os.WriteFile(command, []byte("#!/bin/sh\ncat >/dev/null\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	receipt, err = bound.client.Sendmail(context.Background(), fault.Correlation{Call: "failed-process"}, plainMessage(t, "exit"), LocalDelivery{Executable: command})
	out = resolved(t, receipt, err)
	process = out.Outcome.Value.ProcessInfo()
	if out.Err() == nil || !process.ExitKnown || process.ExitCode != 7 || process.InputBytes == 0 {
		t.Fatal("process exit failure facts")
	}
}
func TestResponseRecoveryRetainsOriginalAndRevokesReader(t *testing.T) {
	peer := newPeer(t, peerOptions{noopReply: "500 private initial failure\r\nignored recovery line\r\n250 recovered"})
	value := testOptions(peer)
	var escaped *ResponseReader
	calls := 0
	value.Native.ResponseHandlers = map[string]ResponseHandler{"noop": func(ctx context.Context, host, command string, reader *ResponseReader, original error) error {
		calls++
		escaped = reader
		line, err := reader.ReadLine()
		if err != nil || line != "ignored recovery line" {
			return errors.New("independent recovery mismatch")
		}
		return nil
	}}
	bound := bindTest(t, value, 1)
	out := sendTest(t, bound.client, "recovery", plainMessage(t, "recovery"))
	if out.Err() != nil || calls != 1 || len(out.Outcome.Value.ResponseErrors()) != 1 || out.Outcome.Value.Messages()[0].Effect() != Accepted {
		t.Fatal("recovery facts lost", out.Err())
	}
	if _, err := escaped.ReadLine(); err == nil {
		t.Fatal("response reader retained live authority")
	}
	drain(t, bound.inbox)
}

type cycleCause struct{}

func (*cycleCause) Error() string       { return "private cyclic error" }
func (value *cycleCause) Unwrap() error { return value }
func TestCallbackErrorInspectionIsBounded(t *testing.T) {
	cycle := &cycleCause{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = phaseFailure(ErrSend, "callback", context.Background(), cycle)
		replyError(cycle)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unbounded error traversal")
	}
}
