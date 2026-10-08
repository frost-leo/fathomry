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

package http3

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	official "github.com/quic-go/quic-go"
	wire "github.com/quic-go/quic-go/quicvarint"
	nativehttp "github.com/sardanioss/http"
)

type priorityRoundTrip struct {
	response *nativehttp.Response
	err      error
}

func priorityRequest(t *testing.T, fixture *qpackPeer, ctx context.Context, priority string) *nativehttp.Request {
	t.Helper()
	request, err := nativehttp.NewRequestWithContext(ctx, "GET", "https://"+fixture.peer.LocalAddr().String()+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Priority", priority)
	return request
}

func consumePrioritySettings(t *testing.T, fixture *qpackPeer) {
	t.Helper()
	reader := wire.NewReader(fixture.control)
	frame, err := wire.Read(reader)
	if err != nil || frame != 4 {
		t.Fatal("client SETTINGS missing", frame, err)
	}
	length, err := wire.Read(reader)
	if err != nil || length > 1024 {
		t.Fatal("unexpected client SETTINGS length", length, err)
	}
	if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
		t.Fatal(err)
	}
}

func awaitPriorityStack(t *testing.T, ctx context.Context, nativeWrite bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		trace := make([]byte, 1<<20)
		trace = trace[:runtime.Stack(trace, true)]
		for _, stack := range strings.Split(string(trace), "\n\n") {
			if strings.Contains(stack, "(*rawConn).MaybeSendPriorityUpdate") &&
				strings.Contains(stack, "(*ClientConn).roundTrip") &&
				(!nativeWrite || strings.Contains(stack, "(*SendStream).write")) {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("priority operation never entered the expected native wait", context.Cause(ctx))
		}
	}
}

func TestFathomryPriorityUpdateCancellationDuringNativeWrite(t *testing.T) {
	fixture := newQPACKPeerSetup(t, 64, 1, []uint64{16}, false, func(client *ClientConn) {
		client.rawConn.sendGreaseFrames = true
	})
	consumePrioritySettings(t, fixture)
	ctx, cancel := context.WithCancelCause(fixture.ctx)
	defer cancel(nil)
	request := priorityRequest(t, fixture, ctx, "u=0, x=\""+strings.Repeat("x", 32<<10)+"\"")
	done := make(chan priorityRoundTrip, 1)
	go func() {
		response, err := fixture.client.RoundTrip(request)
		done <- priorityRoundTrip{response, err}
	}()
	reader := wire.NewReader(fixture.control)
	frame, err := wire.Read(reader)
	if err != nil || frame != priorityUpdateFrameType {
		t.Fatal("actual PRIORITY_UPDATE prefix missing", frame, err)
	}
	length, err := wire.Read(reader)
	if err != nil || length < 32<<10 {
		t.Fatal("large priority frame was not emitted", length, err)
	}
	streamID, err := wire.Read(reader)
	if err != nil || streamID != 0 {
		t.Fatal("wrong request priority target", streamID, err)
	}
	// Leave the remaining frame unread: wire credit plus the native pending
	// STREAM frame cannot hold this admitted field, so Write really blocks.
	awaitPriorityStack(t, fixture.ctx, true)
	cause := errors.New("cancel active priority write")
	cancel(cause)
	select {
	case result := <-done:
		if result.response != nil || !errors.Is(result.err, context.Canceled) || !errors.Is(result.err, cause) {
			t.Fatal("active priority cancellation lost its cause", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("request cancellation did not interrupt the native control write")
	}
	select {
	case <-fixture.peer.Context().Done():
		var application *official.ApplicationError
		if !errors.As(context.Cause(fixture.peer.Context()), &application) || application.ErrorCode != official.ApplicationErrorCode(ErrCodeRequestCanceled) {
			t.Fatal("partial control frame did not fail the connection explicitly", context.Cause(fixture.peer.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("partially emitted control frame remained reusable")
	}
	if fixture.ctx.Err() != nil {
		t.Fatal("fixture timeout, not request cancellation, ended native work")
	}
}

type priorityGateWriter struct {
	writer  io.Writer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (writer *priorityGateWriter) Write(data []byte) (int, error) {
	writer.calls.Add(1)
	writer.once.Do(func() {
		close(writer.entered)
		<-writer.release
	})
	return writer.writer.Write(data)
}

func TestFathomryPriorityUpdateQueuedCancellationKeepsConnection(t *testing.T) {
	writer := &priorityGateWriter{entered: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(writer.release) })
	defer unblock()
	fixture := newQPACKPeerSetup(t, 64, 1, nil, false, func(client *ClientConn) {
		client.rawConn.sendGreaseFrames = true
		writer.writer = client.rawConn.controlStrSend
		client.rawConn.controlStrSend = writer
	})
	consumePrioritySettings(t, fixture)
	firstDone := make(chan error, 1)
	go func() { firstDone <- fixture.client.rawConn.MaybeSendPriorityUpdate(0, "u=0, i") }()
	select {
	case <-writer.entered:
	case <-fixture.ctx.Done():
		t.Fatal("control writer did not enter")
	}
	ctx, cancel := context.WithCancelCause(fixture.ctx)
	defer cancel(nil)
	done := make(chan priorityRoundTrip, 1)
	request := priorityRequest(t, fixture, ctx, "u=1")
	go func() {
		response, err := fixture.client.RoundTrip(request)
		done <- priorityRoundTrip{response, err}
	}()
	awaitPriorityStack(t, fixture.ctx, false)
	cause := errors.New("cancel queued priority write")
	cancel(cause)
	select {
	case result := <-done:
		if result.response != nil || !errors.Is(result.err, context.Canceled) || !errors.Is(result.err, cause) {
			t.Fatal("queued priority cancellation lost its cause", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued priority write did not honor request cancellation")
	}
	if writer.calls.Load() != 1 || fixture.client.Context().Err() != nil {
		t.Fatal("canceled queue waiter wrote or closed the healthy connection", writer.calls.Load(), context.Cause(fixture.client.Context()))
	}
	unblock()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal("uncanceled control write did not complete", err)
		}
	case <-fixture.ctx.Done():
		t.Fatal("control writer was not joined")
	}
	reader := wire.NewReader(fixture.control)
	frame, err := wire.Read(reader)
	if err != nil || frame != priorityUpdateFrameType {
		t.Fatal("positive control frame absent", frame, err)
	}
	length, err := wire.Read(reader)
	if err != nil || length > 128 {
		t.Fatal("positive control frame length", length, err)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil || string(payload) != "\x00u=0, i" {
		t.Fatal("uncanceled control frame changed", payload, err)
	}
	reset, err := fixture.peer.AcceptStream(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reset)
	var streamError *official.StreamError
	if len(data) != 0 || !errors.As(err, &streamError) || streamError.ErrorCode != official.StreamErrorCode(ErrCodeRequestCanceled) {
		t.Fatal("queued canceled request dispatched HTTP bytes or retained its stream", data, err)
	}
	sibling, peer := fixture.request(t, fixture.ctx)
	peerHeaders(t, peer, []byte{0, 0, 0xd9})
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := sibling.ReadResponse()
	if err != nil || response.StatusCode != 200 {
		t.Fatal("healthy sibling failed after queued cancellation", err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
