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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func reviewSettings() Settings {
	return Settings{Name: "review-fixed", Host: "localhost", Port: 465, Hello: "review.invalid", TLSMode: "implicit", Auth: "none", MaxActive: 1, QueuedCalls: 1, MaxMessages: 1, MaxRecipients: 1, MaxMessageBytes: 4096, MaxMIMEBytes: 8192, Timeout: 40 * time.Millisecond, CloseTimeout: 40 * time.Millisecond}
}
func reviewOwner(t testing.TB, value Settings, hooks NativeOptions, extra int) (*Owner, *adapters.Runtime) {
	t.Helper()
	prepared, err := Prepare(value, hooks)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	policy.Runtime.MaxActive += extra
	policy.Runtime.MaxWorkBytes += int64(extra) * policy.Budget.WorkBytes
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := prepared.Open(context.Background(), Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	return owner, runtime
}
func TestReviewedTLSContainerBoundsAndAccounting(t *testing.T) {
	value := reviewSettings()
	base, err := Prepare(value, NativeOptions{TLS: &tls.Config{}})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := base.Policy()
	small := &tls.Config{Certificates: []tls.Certificate{{OCSPStaple: make([]byte, 1024), SignedCertificateTimestamps: [][]byte{make([]byte, 128)}, SupportedSignatureAlgorithms: []tls.SignatureScheme{tls.PSSWithSHA256}}}, NextProtos: []string{"smtp"}}
	prepared, err := Prepare(value, NativeOptions{TLS: small})
	if err != nil {
		t.Fatal(err)
	}
	after, _ := prepared.Policy()
	if after.SourceWorkBytes <= before.SourceWorkBytes+1024 {
		t.Fatal("retained TLS data uncharged")
	}
	for _, large := range []*tls.Config{
		{Certificates: []tls.Certificate{{OCSPStaple: make([]byte, 8<<20)}}},
		{Certificates: []tls.Certificate{{SignedCertificateTimestamps: [][]byte{make([]byte, 8<<20)}}}},
		{Certificates: []tls.Certificate{{SupportedSignatureAlgorithms: make([]tls.SignatureScheme, 257)}}},
		{NextProtos: []string{strings.Repeat("x", 256)}},
	} {
		if _, err := Prepare(value, NativeOptions{TLS: large}); !errors.Is(err, ErrLimit) {
			t.Fatal("oversized TLS copy accepted", err)
		}
	}
	for _, size := range []int{33, 100000} {
		handlers := make(map[string]ResponseHandler, size)
		for i := 0; i < size; i++ {
			handlers[string(rune(i))] = func(context.Context, string, string, *ResponseReader, error) error { return nil }
		}
		allocations := testing.AllocsPerRun(5, func() {
			if _, err := Prepare(value, NativeOptions{ResponseHandlers: handlers}); !errors.Is(err, ErrLimit) {
				t.Fatal("oversized handler map accepted")
			}
		})
		if allocations > 30 {
			t.Fatalf("rejection allocated per input handler: %f for %d", allocations, size)
		}
	}
}

type reviewHeldReader struct {
	entered, proceed chan struct{}
	once             sync.Once
}

func (value *reviewHeldReader) Read([]byte) (int, error) {
	value.once.Do(func() { close(value.entered) })
	<-value.proceed
	return 0, io.EOF
}
func TestReviewedPublicAndNativeAdmissionBothExpire(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(string(rune('0'+extra)), func(t *testing.T) {
			owner, runtime := reviewOwner(t, reviewSettings(), NativeOptions{}, extra)
			defer runtime.Close(context.Background())
			defer owner.Close(context.Background())
			message, err := NewMessage(Content{From: "sender@example.invalid", To: []string{"to@example.invalid"}, Text: "body"})
			if err != nil {
				t.Fatal(err)
			}
			reader := &reviewHeldReader{entered: make(chan struct{}), proceed: make(chan struct{})}
			release := sync.OnceFunc(func() { close(reader.proceed) })
			defer release()
			first := make(chan error, 1)
			go func() {
				_, err := owner.Client().PrepareMessage(context.Background(), message, Preparation{Files: []FileInput{{Input: Input{Reader: reader}, File: Attachment{Name: "data.txt"}}}})
				first <- err
			}()
			<-reader.entered
			second := make(chan error, 1)
			go func() { _, err := owner.Client().Export(context.Background(), message, &bytes.Buffer{}); second <- err }()
			select {
			case err := <-second:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("admission cause lost", err)
				}
			case <-time.After(time.Second):
				t.Fatal("configured admission timeout bypassed")
			}
			release()
			<-first
		})
	}
}
func TestRecheckedSourceOpenAdmissionAndLifetime(t *testing.T) {
	settings := reviewSettings()
	prepared, err := Prepare(settings, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := prepared.Policy()
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := Dependencies{Runtime: runtime, Evidence: inbox}
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	// The successful Open's admission timer must not own the source lifetime.
	time.Sleep(2 * settings.Timeout)
	if owner.ShutdownComplete() {
		t.Fatal("source lifetime was coupled to admission timeout")
	}
	reader := &reviewHeldReader{entered: make(chan struct{}), proceed: make(chan struct{})}
	first := make(chan struct{})
	message, err := NewMessage(Content{})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(first)
		_, _ = owner.Client().PrepareMessage(context.Background(), message, Preparation{Files: []FileInput{{Input: Input{Reader: reader}, File: Attachment{Name: "data.txt"}}}})
	}()
	defer func() { close(reader.proceed); <-first }()
	select {
	case <-reader.entered:
	case <-time.After(time.Second):
		t.Fatal("source failed after admission timer expired")
	}
	setup, cancel := context.WithCancel(context.Background())
	defer cancel()
	opened := make(chan error, 1)
	go func() {
		later, err := prepared.Open(setup, dependencies)
		if later != nil {
			_ = later.Close(context.Background())
		}
		opened <- err
	}()
	select {
	case err := <-opened:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("source admission deadline lost", err)
		}
	case <-time.After(time.Second):
		cancel()
		<-opened
		t.Fatal("source Open bypassed admission timeout")
	}
}

func TestReviewedCloseWaitIsIndependentOfCleanupCallback(t *testing.T) {
	value := reviewSettings()
	value.TLSMode, value.AllowPlaintext = "none", true
	value.Timeout = time.Second
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	peerDone := make(chan struct{})
	cleanupCause := errors.New("controlled cleanup failure")
	hooks := NativeOptions{DialContext: func(context.Context, string, string) (net.Conn, error) {
		client, peer := net.Pipe()
		go func() {
			defer close(peerDone)
			defer peer.Close()
			io.WriteString(peer, "220 local peer\r\n")
			scanner := bufio.NewScanner(peer)
			for scanner.Scan() {
				if scanner.Text() == "QUIT" {
					io.WriteString(peer, "550 cleanup failure\r\n")
					return
				}
				io.WriteString(peer, "250 local\r\n")
			}
		}()
		return client, nil
	}, ResponseHandlers: map[string]ResponseHandler{"quit": func(context.Context, string, string, *ResponseReader, error) error {
		close(entered)
		<-unblock
		return cleanupCause
	}}}
	owner, runtime := reviewOwner(t, value, hooks, 0)
	defer runtime.Close(context.Background())
	defer owner.Close(context.Background())
	if _, err := owner.Client().Session(context.Background(), SessionRequest{}); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- owner.Close(short) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup hook missing")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("close wait cause", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close caller retained cleanup hook stack")
	}
	if owner.ShutdownComplete() {
		t.Fatal("callback cancellation claimed release")
	}
	release()
	if err := owner.Close(context.Background()); !errors.Is(err, cleanupCause) {
		t.Fatal("cleanup original cause lost", err)
	}
	if !owner.ShutdownComplete() {
		t.Fatal("cleanup did not join")
	}
	<-peerDone
}
