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

package minio

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestReviewCanceledDialRetainsLeaseAndLateFailure(t *testing.T) {
	_, options := newPeer(t)
	options.MaxActive = 1
	fixture := bindFixture(t, options, 4)
	fixture.client.owner.transport.CloseIdleConnections()
	entered, gate := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(gate) }) })
	late := errors.New("late-dial-error")
	fixture.client.owner.dialer.ControlContext = func(context.Context, string, string, syscall.RawConn) error {
		close(entered)
		<-gate
		return late
	}
	cause := errors.New("caller-cancellation-cause")
	ctx, cancel := context.WithCancelCause(deadline(t))
	defer cancel(nil)
	receipt, err := fixture.client.Stat(ctx, correlation("review-dial"), Address{Key: "owned/dial"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("dial was not reached")
	}
	cancel(cause)
	wait, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()
	if _, err := receipt.WaitReleased(wait); err == nil {
		t.Error("receipt released before the actual dial returned")
	}
	if fixture.assembly.Snapshot().Sources[0].Usage.Active != 1 {
		t.Error("late dial lost its admission lease")
	}
	if err := fixture.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) || fixture.assembly.Snapshot().Sources[0].Released {
		t.Error("source shutdown claimed live dial release")
	}
	unblock.Do(func() { close(gate) })
	result := settle(t, receipt, nil)
	if !errors.Is(result.Err(), late) || !errors.Is(result.Err(), cause) || !errors.Is(result.Err(), context.Canceled) {
		t.Error("late dial error or original cancellation cause was erased")
	}
	delivery, err := fixture.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	independent, err := delivery.Receipt().WaitReleased(deadline(t))
	if err != nil || !errors.Is(independent.Err(), late) || !errors.Is(independent.Err(), cause) {
		t.Error("independent evidence lost late dial or cancellation cause")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestReviewLateDialCallbackCannotAcquireAfterRelease(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	fixture.client.owner.transport.CloseIdleConnections()
	entered, gate := make(chan struct{}), make(chan struct{})
	returned := make(chan error, 1)
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(gate) }) })
	var acquisitions atomic.Int32
	fixture.client.owner.dialer.ControlContext = func(context.Context, string, string, syscall.RawConn) error {
		acquisitions.Add(1)
		return nil
	}
	dial := fixture.client.owner.transport.DialContext
	fixture.client.owner.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		close(entered)
		<-gate
		socket, err := dial(ctx, network, address)
		if socket != nil {
			_ = socket.Close()
		}
		returned <- err
		return socket, err
	}
	ctx, cancel := context.WithCancel(deadline(t))
	defer cancel()
	before := server.count()
	receipt, err := fixture.client.Stat(ctx, correlation("review-late-dial"), Address{Key: "owned/late"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("dial callback was not scheduled")
	}
	cancel()
	if result := settle(t, receipt, nil); !errors.Is(result.Err(), context.Canceled) {
		t.Fatal("canceled exchange was not finalized")
	}
	if err := fixture.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	unblock.Do(func() { close(gate) })
	select {
	case err := <-returned:
		if !errors.Is(err, ErrAuthority) || acquisitions.Load() != 0 || server.count() != before {
			t.Fatal("late callback acquired I/O after the exchange was finalized")
		}
	case <-deadline(t).Done():
		t.Fatal("late callback did not terminate")
	}
}

func TestReviewCanceledTLSHandshakeClosesOwnedSocket(t *testing.T) {
	server, options := newPeer(t)
	entered := make(chan struct{})
	closed := make(chan error, 1)
	var blocked atomic.Bool
	secure := httptest.NewUnstartedServer(http.HandlerFunc(server.serve))
	secure.Config.ErrorLog = log.New(io.Discard, "", 0)
	secure.TLS = &tls.Config{GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		if !blocked.Load() {
			return nil, nil
		}
		_ = hello.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		close(entered)
		_, err := io.Copy(io.Discard, hello.Conn)
		closed <- err
		return nil, err
	}}
	secure.StartTLS()
	t.Cleanup(secure.Close)
	options.Endpoint = secure.URL
	options.Plaintext = false
	options.RootCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: secure.Certificate().Raw}))
	fixture := bindFixture(t, options, 4)
	fixture.client.owner.transport.CloseIdleConnections()
	blocked.Store(true)
	ctx, cancel := context.WithCancelCause(deadline(t))
	defer cancel(nil)
	cause := errors.New("handshake-canceled")
	receipt, err := fixture.client.Stat(ctx, correlation("review-tls-cancel"), Address{Key: "owned/handshake"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("TLS handshake was not reached")
	}
	cancel(cause)
	result := settle(t, receipt, nil)
	if !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cause) {
		t.Fatal("TLS cancellation evidence was erased")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal("TLS peer did not observe actual socket closure", err)
		}
	case <-deadline(t).Done():
		t.Fatal("canceled TLS connection remained open")
	}
	fixture.client.owner.mu.Lock()
	remaining := len(fixture.client.owner.sockets)
	fixture.client.owner.mu.Unlock()
	if remaining != 0 {
		t.Fatal("canceled TLS connection retained an owned socket")
	}
}

type reviewFinalReader struct {
	content string
	err     error
}

func (reader *reviewFinalReader) Read(buffer []byte) (int, error) {
	count := copy(buffer, reader.content)
	reader.content = reader.content[count:]
	if reader.content == "" {
		return count, reader.err
	}
	return count, nil
}

func TestReviewOverflowRetainsSimultaneousReadFailure(t *testing.T) {
	for _, payload := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "payload"}[payload], func(t *testing.T) {
			cause := errors.New("overflow-read-cause")
			state := newExchange(defaults(OptionsV1{}), nil, payload)
			state.remaining = 0
			body := &boundedBody{body: io.NopCloser(&reviewFinalReader{content: "x", err: cause}), state: state, payload: payload}
			content, err := io.ReadAll(body)
			primary, _ := state.finish()
			if len(content) != 0 || !errors.Is(err, ErrLimit) || !errors.Is(err, cause) || !errors.Is(primary, cause) {
				t.Fatal("overflow erased the accompanying read failure")
			}
		})
	}
}

func TestReviewDownloadRetainsChecksumAndSinkFailures(t *testing.T) {
	_, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	expected := sha256.Sum256([]byte("good"))
	fixture.client.owner.wire.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Request: request, Header: http.Header{
			"Content-Length": {"4"}, "Last-Modified": {time.Unix(1700000000, 0).UTC().Format(http.TimeFormat)},
			"X-Amz-Checksum-Type": {"FULL_OBJECT"}, "X-Amz-Checksum-Sha256": {base64.StdEncoding.EncodeToString(expected[:])},
		}, Body: io.NopCloser(&reviewFinalReader{content: "evil", err: io.EOF})}, nil
	})
	sink := errors.New("sink-failure")
	receipt, err := fixture.client.Download(deadline(t), correlation("review-checksum-sink"), ReadRequest{Address: Address{Key: "owned/checksum"}}, &partialWriter{cause: sink})
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), sink) || result.Outcome.Value.Transfer().Bytes != 2 || result.Outcome.Value.Complete() {
		t.Fatal("partial sink failure or its accepted bytes was erased")
	}
	if !reviewContainsError(result.Err(), "checksum mismatch") {
		t.Fatal("simultaneous native checksum failure was erased by the sink failure")
	}
}

type reviewInvalidWriter struct{ content []byte }

func (writer *reviewInvalidWriter) Write(content []byte) (int, error) {
	writer.content = append(writer.content, content...)
	return len(content) + 1, nil
}

func TestReviewInvalidWriterDoesNotClaimUntouchedSink(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.store("owned/invalid-sink", []byte("abc"), nil)
	server.mu.Unlock()
	writer := &reviewInvalidWriter{}
	receipt, err := fixture.client.Download(deadline(t), correlation("review-invalid-sink"), ReadRequest{Address: Address{Key: "owned/invalid-sink"}}, writer)
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), ErrInput) || string(writer.content) != "abc" || result.Outcome.Value.Transfer().Effect != Unknown || result.Outcome.Value.Complete() {
		t.Fatal("invalid writer count was treated as evidence of no sink mutation")
	}
}

func TestReviewClosedResponseDropsNativeBodyAfterActualClose(t *testing.T) {
	state := newExchange(defaults(OptionsV1{}), nil, false)
	cause := errors.New("closed-response-cause")
	native := &gatedClose{Reader: strings.NewReader("content"), entered: make(chan struct{}), gate: make(chan struct{}), cause: cause}
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(native.gate) }) })
	body := &boundedBody{body: native, state: state}
	closed := make(chan error, 1)
	go func() { closed <- body.Close() }()
	select {
	case <-native.entered:
	case <-deadline(t).Done():
		t.Fatal("body close was not reached")
	}
	if body.body == nil {
		t.Error("native response was detached before actual close returned")
	}
	unblock.Do(func() { close(native.gate) })
	if err := <-closed; !errors.Is(err, cause) {
		t.Fatal("late native body-close cause was erased")
	}
	if body.body != nil {
		t.Error("closed response retains native body/header/socket graph")
	}
	if count, err := body.Read(make([]byte, 1)); count != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Error("closed response remained readable")
	}
	if err := body.Close(); !errors.Is(err, cause) {
		t.Error("repeated close lost its original cause")
	}
	_, cleanup := state.finish()
	if !errors.Is(cleanup, cause) {
		t.Error("native close cause was erased from independent cleanup evidence")
	}
}

type reviewCloseTracker struct {
	reads  int
	closes int
	cause  error
}

func (body *reviewCloseTracker) Read([]byte) (int, error) {
	body.reads++
	return 0, io.EOF
}
func (body *reviewCloseTracker) Close() error {
	body.closes++
	return body.cause
}

func TestReviewRefusedRoundTripClosesBodyWithoutSubmission(t *testing.T) {
	for _, mode := range []string{"unowned", "canceled", "target", "quota"} {
		t.Run(mode, func(t *testing.T) {
			value := defaults(OptionsV1{Bucket: "fixture", MaxRequests: 1})
			state := newExchange(value, nil, false)
			ctx := controlledContext(deadline(t), state)
			target := "http://127.0.0.1:1/fixture/owned/body"
			var expected error = ErrAuthority
			switch mode {
			case "unowned":
				ctx = deadline(t)
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx, expected = canceled, context.Canceled
			case "target":
				target = "http://127.0.0.1:2/fixture/owned/body"
			case "quota":
				state.requests = state.maximum
				expected = ErrLimit
			}
			before := state.requests
			cause := errors.New("refused-request-close-error")
			body := &reviewCloseTracker{cause: cause}
			request, err := http.NewRequestWithContext(ctx, http.MethodPut, target, body)
			if err != nil {
				t.Fatal(err)
			}
			handoffs := 0
			wire := &transport{settings: value, endpoint: "http://127.0.0.1:1", base: roundTripFunc(func(*http.Request) (*http.Response, error) {
				handoffs++
				return nil, errors.New("refused request reached native transport")
			})}
			response, err := wire.RoundTrip(request)
			if response != nil || !errors.Is(err, expected) || !errors.Is(err, cause) || body.closes != 1 || body.reads != 0 || handoffs != 0 || state.requests != before {
				t.Errorf("refusal did not close the body without submission: closes=%d reads=%d handoffs=%d", body.closes, body.reads, handoffs)
			}
			primary, cleanup := state.finish()
			if primary != nil || mode != "unowned" && !errors.Is(cleanup, cause) {
				t.Error("refusal body-close failure was not retained as independent cleanup evidence")
			}
		})
	}
}

func reviewContainsError(err error, text string) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), text) {
		return true
	}
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		for _, nested := range many.Unwrap() {
			if reviewContainsError(nested, text) {
				return true
			}
		}
	}
	return reviewContainsError(errors.Unwrap(err), text)
}
