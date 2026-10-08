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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nativehttp "github.com/nukilabs/http"
	nativequic "github.com/nukilabs/quic-go"
	utls "github.com/nukilabs/utls"
	official "github.com/quic-go/quic-go"
	wire "github.com/quic-go/quic-go/quicvarint"
)

type fathomryFeedbackProbe struct {
	writer  *qpackControlWriter
	armed   atomic.Bool
	entered chan struct{}
	once    sync.Once
}

func (probe *fathomryFeedbackProbe) Write(data []byte) (int, error) {
	return probe.WriteContext(probe.writer.client.Context(), data)
}
func (probe *fathomryFeedbackProbe) WriteContext(ctx context.Context, data []byte) (int, error) {
	if probe.armed.Load() {
		probe.once.Do(func() { close(probe.entered) })
	}
	return probe.writer.WriteContext(ctx, data)
}

func TestFathomryQPACKWireCriticalSTOPDuringFeedbackBackpressure(t *testing.T) {
	for _, target := range []string{"control", "decoder"} {
		t.Run(target, func(t *testing.T) {
			peer := newFathomryQPACKPeer(t, 64, 1, &official.Config{MaxIdleTimeout: 4 * time.Second, InitialStreamReceiveWindow: 16, MaxStreamReceiveWindow: 16})
			probe := &fathomryFeedbackProbe{writer: &qpackControlWriter{client: peer.client}, entered: make(chan struct{})}
			peer.client.decoder.SetDecoderStream(probe)
			if err := peer.client.decoder.CancelStreamContext(peer.ctx, 0); err != nil {
				t.Fatal(err)
			}
			receiver, err := peer.server.AcceptUniStream(peer.ctx)
			if err != nil {
				t.Fatal(err)
			}
			// Do not read the type byte: keeping all peer receive credit consumed
			// makes the following ErrWouldBlock a stable physical condition.
			for accepted := 2; accepted < 16; accepted++ {
				if err := probe.writer.stream.TryWriteAll([]byte{0x40}); err != nil {
					t.Fatal("filling declared decoder credit", accepted, err)
				}
			}
			if err := probe.writer.stream.TryWriteAll([]byte{0x40}); !errors.Is(err, nativequic.ErrWouldBlock) {
				t.Fatal("fixture did not exhaust actual credit", err)
			}
			probe.armed.Store(true)
			done := make(chan error, 1)
			go func() { done <- peer.client.decoder.CancelStreamContext(peer.ctx, 4) }()
			select {
			case <-probe.entered:
			case <-peer.ctx.Done():
				t.Fatal("feedback writer not entered")
			}
			timer := time.NewTicker(time.Millisecond)
			defer timer.Stop()
			entered := false
			for !entered {
				trace := make([]byte, 1<<20)
				trace = trace[:runtime.Stack(trace, true)]
				for _, stack := range strings.Split(string(trace), "\n\n") {
					if strings.Contains(stack, "[select]") && strings.Contains(stack, "(*SendStream).WriteAllContext") && strings.Contains(stack, "(*Decoder).CancelStreamContext") {
						entered = true
						break
					}
				}
				if entered {
					break
				}
				select {
				case err := <-done:
					t.Fatal("feedback did not remain blocked", err)
				case <-timer.C:
				case <-peer.ctx.Done():
					t.Fatal("actual feedback parking was not observed")
				}
			}
			if target == "control" {
				peer.clientControl.CancelRead(77)
			} else {
				receiver.CancelRead(78)
			}
			peer.wantClosed(t, ErrCodeClosedCriticalStream)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("blocked instruction was falsely accepted")
				}
			case <-peer.ctx.Done():
				t.Fatal("feedback write was not joined")
			}
			if peer.ctx.Err() != nil {
				t.Fatal("parent timeout, not critical watcher, ended work")
			}
		})
	}
}

type fathomryQPACKPeer struct {
	ctx           context.Context
	client        *ClientConn
	server        *official.Conn
	control       *official.SendStream
	encoder       *official.SendStream
	feedback      *official.ReceiveStream
	clientControl *official.ReceiveStream
}

func newFathomryQPACKPeer(t *testing.T, capacity, blocked uint64, configurations ...*official.Config) *fathomryQPACKPeer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	certificate := httptest.NewTLSServer(nil)
	certificates := certificate.TLS.Certificates
	roots := x509.NewCertPool()
	roots.AddCert(certificate.Certificate())
	certificate.Close()
	configuration := &official.Config{MaxIdleTimeout: 4 * time.Second}
	if len(configurations) > 0 {
		configuration = configurations[0]
	}
	listener, err := official.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: certificates, NextProtos: []string{"h3"}}, configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	type accepted struct {
		conn *official.Conn
		err  error
	}
	wait := make(chan accepted, 1)
	go func() { conn, err := listener.Accept(ctx); wait <- accepted{conn, err} }()
	connection, err := nativequic.DialAddr(ctx, listener.Addr().String(), &utls.Config{RootCAs: roots, NextProtos: []string{"h3"}}, &nativequic.Config{MaxIdleTimeout: 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var server *official.Conn
	select {
	case result := <-wait:
		if result.err != nil {
			t.Fatal(result.err)
		}
		server = result.conn
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	transport := &Transport{AdditionalSettings: map[uint64]uint64{1: capacity, 7: blocked}, MaxResponseHeaderBytes: 4096}
	client := transport.NewClientConn(connection)
	peer := &fathomryQPACKPeer{ctx: ctx, client: client, server: server}
	t.Cleanup(func() {
		_ = client.CloseWithError(0, "")
		_ = server.CloseWithError(0, "")
		client.controlMu.Lock()
		active := client.controlActive
		client.controlMu.Unlock()
		if active != 0 || client.decoder.BlockedStreams() != 0 {
			t.Errorf("native control work not joined: workers=%d blocked=%d", active, client.decoder.BlockedStreams())
		}
	})
	peer.control, err = server.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.control.Write([]byte{0, 4, 0}); err != nil {
		t.Fatal(err)
	}
	clientControl, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	peer.clientControl = clientControl
	reader := wire.NewReader(clientControl)
	kind, err := wire.Read(reader)
	if err != nil || kind != 0 {
		t.Fatal("missing client control", kind, err)
	}
	frame, err := wire.Read(reader)
	if err != nil || frame != 4 {
		t.Fatal("missing client settings", frame, err)
	}
	length, err := wire.Read(reader)
	if err != nil || length > 128 {
		t.Fatal("settings bound", length, err)
	}
	if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
		t.Fatal(err)
	}
	return peer
}

func (peer *fathomryQPACKPeer) open(t *testing.T, ctx context.Context) (*RequestStream, *official.Stream) {
	t.Helper()
	stream, err := peer.client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	address, _ := url.Parse("https://fixture.invalid/")
	if err := stream.SendRequestHeader(&nativehttp.Request{Method: "GET", URL: address, Header: make(nativehttp.Header)}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	server, err := peer.server.AcceptStream(peer.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader := wire.NewReader(server)
	kind, err := wire.Read(reader)
	if err != nil || kind != 1 {
		t.Fatal("request HEADERS missing", kind, err)
	}
	length, err := wire.Read(reader)
	if err != nil || length > 4096 {
		t.Fatal("request metadata bound", length, err)
	}
	if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
		t.Fatal(err)
	}
	return stream, server
}
func fathomryQPACKFrame(t *testing.T, stream *official.Stream, kind uint64, payload []byte, finish bool) {
	t.Helper()
	data := wire.Append(nil, kind)
	data = wire.Append(data, uint64(len(payload)))
	data = append(data, payload...)
	if _, err := stream.Write(data); err != nil {
		t.Fatal(err)
	}
	if finish {
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
func (peer *fathomryQPACKPeer) insert(t *testing.T) {
	t.Helper()
	if peer.encoder == nil {
		stream, err := peer.server.OpenUniStreamSync(peer.ctx)
		if err != nil {
			t.Fatal(err)
		}
		peer.encoder = stream
		if _, err := stream.Write([]byte{2}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := peer.encoder.Write([]byte{0x3f, 0x21, 0x41, 'x', 1, 'v'}); err != nil {
		t.Fatal(err)
	}
}
func (peer *fathomryQPACKPeer) nextFeedback(t *testing.T) (byte, uint64) {
	t.Helper()
	if peer.feedback == nil {
		stream, err := peer.server.AcceptUniStream(peer.ctx)
		if err != nil {
			t.Fatal(err)
		}
		kind, err := wire.Read(wire.NewReader(stream))
		if err != nil || kind != 3 {
			t.Fatal("missing decoder feedback stream", kind, err)
		}
		peer.feedback = stream
	}
	_ = peer.feedback.SetReadDeadline(time.Now().Add(time.Second))
	var first [1]byte
	if _, err := io.ReadFull(peer.feedback, first[:]); err != nil {
		t.Fatal("feedback missing", err)
	}
	prefix := byte(6)
	if first[0]&0x80 != 0 {
		prefix = 7
	}
	value, err := readFeedbackInteger(peer.feedback, first[0], prefix)
	if err != nil {
		t.Fatal(err)
	}
	return first[0], value
}
func (peer *fathomryQPACKPeer) waitBlocked(t *testing.T, count uint64) {
	t.Helper()
	timer := time.NewTicker(time.Millisecond)
	defer timer.Stop()
	for peer.client.decoder.BlockedStreams() != count {
		select {
		case <-timer.C:
		case <-peer.ctx.Done():
			t.Fatal("decoder did not enter expected blocked state", peer.client.decoder.BlockedStreams())
		}
	}
}
func (peer *fathomryQPACKPeer) wantClosed(t *testing.T, code ErrCode) {
	t.Helper()
	select {
	case <-peer.client.Context().Done():
	case <-peer.ctx.Done():
		t.Fatal("connection did not close for protocol violation")
	}
	var actual *nativequic.ApplicationError
	if !errors.As(context.Cause(peer.client.Context()), &actual) || actual.ErrorCode != nativequic.ApplicationErrorCode(code) {
		t.Fatalf("wrong connection failure: %v want %s", context.Cause(peer.client.Context()), code)
	}
}
func (peer *fathomryQPACKPeer) healthy(t *testing.T) {
	t.Helper()
	if peer.client.Context().Err() != nil {
		t.Fatal("sibling connection was closed", context.Cause(peer.client.Context()))
	}
	request, response := peer.open(t, peer.ctx)
	fathomryQPACKFrame(t, response, 1, []byte{0, 0, 0xd9}, true)
	result, err := request.ReadResponse()
	if err != nil {
		t.Fatal("healthy static sibling failed", err)
	}
	if _, err := io.ReadAll(result.Body); err != nil {
		t.Fatal(err)
	}
	_ = result.Body.Close()
}

func TestFathomryQPACKWireDynamicAndNormalFIN(t *testing.T) {
	peer := newFathomryQPACKPeer(t, 64, 1)
	request, response := peer.open(t, peer.ctx)
	fathomryQPACKFrame(t, response, 1, []byte{2, 0, 0xd9, 0x80}, true)
	type outcome struct {
		response *nativehttp.Response
		err      error
	}
	done := make(chan outcome, 1)
	go func() { response, err := request.ReadResponse(); done <- outcome{response, err} }()
	peer.waitBlocked(t, 1)
	if peer.client.Context().Err() != nil {
		t.Fatal("normal FIN ended connection")
	}
	peer.insert(t)
	select {
	case result := <-done:
		if result.err != nil || result.response.Header.Get("X") != "v" {
			t.Fatal("dynamic field not decoded after normal FIN", result.err)
		}
		if _, err := io.ReadAll(result.response.Body); err != nil {
			t.Fatal(err)
		}
		_ = result.response.Body.Close()
	case <-peer.ctx.Done():
		t.Fatal("dynamic decoder did not resume")
	}
	known, ack := uint64(0), false
	for !ack {
		first, value := peer.nextFeedback(t)
		if first&0x80 != 0 {
			if value != uint64(request.StreamID()) {
				t.Fatal("wrong section ACK", value)
			}
			known = max(known, 1)
			ack = true
		} else if first&0xc0 == 0 {
			known += value
		} else {
			t.Fatal("normal FIN produced cancellation")
		}
		if known > 1 {
			t.Fatal("feedback over-credit", known)
		}
	}
	peer.healthy(t)
}

func TestFathomryQPACKWireBlockedSettingsAndMalformed(t *testing.T) {
	for _, scenario := range []string{"zero-blocked", "one-blocked", "invalid-reference", "encoder-instruction", "encoder-eof", "decoder-eof", "decoder-ack"} {
		t.Run(scenario, func(t *testing.T) {
			blocked := uint64(1)
			if scenario == "zero-blocked" {
				blocked = 0
			}
			peer := newFathomryQPACKPeer(t, 64, blocked)
			switch scenario {
			case "encoder-eof", "decoder-eof", "decoder-ack", "encoder-instruction":
				stream, err := peer.server.OpenUniStreamSync(peer.ctx)
				if err != nil {
					t.Fatal(err)
				}
				kind := byte(2)
				if scenario == "decoder-eof" || scenario == "decoder-ack" {
					kind = 3
				}
				payload := []byte{kind}
				code := ErrCodeClosedCriticalStream
				if scenario == "decoder-ack" {
					payload = append(payload, 0x80)
					code = ErrCodeQPACKDecoderStreamError
				}
				if scenario == "encoder-instruction" {
					payload = append(payload, 0x3f, 0x61)
					code = ErrCodeQPACKEncoderStreamError
				}
				if _, err := stream.Write(payload); err != nil {
					t.Fatal(err)
				}
				if scenario == "encoder-eof" || scenario == "decoder-eof" {
					_ = stream.Close()
				}
				peer.wantClosed(t, code)
			default:
				if scenario == "invalid-reference" {
					peer.insert(t)
					first, count := peer.nextFeedback(t)
					if first&0xc0 != 0 || count != 1 {
						t.Fatal("initial insertion not received")
					}
				}
				request, response := peer.open(t, peer.ctx)
				block := []byte{2, 0, 0xd9, 0x80}
				if scenario == "invalid-reference" {
					block = []byte{0, 1, 0xd9, 0x80}
				}
				fathomryQPACKFrame(t, response, 1, block, false)
				done := make(chan error, 2)
				go func() { _, err := request.ReadResponse(); done <- err }()
				if scenario == "one-blocked" {
					peer.waitBlocked(t, 1)
					next, server := peer.open(t, peer.ctx)
					fathomryQPACKFrame(t, server, 1, block, false)
					go func() { _, err := next.ReadResponse(); done <- err }()
				}
				peer.wantClosed(t, ErrCodeQPACKDecompressionFailed)
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("invalid section succeeded")
					}
				case <-peer.ctx.Done():
					t.Fatal("invalid section remained blocked")
				}
			}
		})
	}
}

func TestFathomryQPACKWireReadAbortAndDeadlineKeepSibling(t *testing.T) {
	for _, method := range []string{"context", "cancel-read", "deadline", "remote-reset"} {
		t.Run(method, func(t *testing.T) {
			peer := newFathomryQPACKPeer(t, 64, 1)
			ctx, cancel := context.WithCancel(peer.ctx)
			defer cancel()
			request, response := peer.open(t, ctx)
			fathomryQPACKFrame(t, response, 1, []byte{2, 0, 0xd9, 0x80}, false)
			done := make(chan error, 1)
			go func() { _, err := request.ReadResponse(); done <- err }()
			peer.waitBlocked(t, 1)
			switch method {
			case "context":
				cancel()
			case "cancel-read":
				request.CancelRead(nativequic.StreamErrorCode(ErrCodeRequestCanceled))
			case "deadline":
				if err := request.SetReadDeadline(time.Unix(1, 0)); err != nil {
					t.Fatal(err)
				}
			case "remote-reset":
				response.CancelWrite(official.StreamErrorCode(73))
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("aborted field section succeeded")
				}
				if method == "deadline" && !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatal("deadline cause lost", err)
				}
			case <-peer.ctx.Done():
				t.Fatal("actual blocked decode did not terminate")
			}
			if method != "context" && ctx.Err() != nil {
				t.Fatal("stream operation canceled original parent")
			}
			peer.waitBlocked(t, 0)
			first, id := peer.nextFeedback(t)
			if first&0xc0 != 0x40 || id != uint64(request.StreamID()) {
				t.Fatal("stream cancellation feedback lost", first, id)
			}
			peer.healthy(t)
		})
	}
}

func TestFathomryQPACKWireAbandonedTrailersCancelOnce(t *testing.T) {
	peer := newFathomryQPACKPeer(t, 64, 1)
	peer.insert(t)
	if first, count := peer.nextFeedback(t); first&0xc0 != 0 || count != 1 {
		t.Fatal("initial insertion missing")
	}
	request, response := peer.open(t, peer.ctx)
	fathomryQPACKFrame(t, response, 1, []byte{0, 0, 0xd9}, false)
	result, err := request.ReadResponse()
	if err != nil {
		t.Fatal(err)
	}
	fathomryQPACKFrame(t, response, 1, []byte{2, 0, 0x80}, true)
	_ = result.Body.Close()
	_ = result.Body.Close()
	first, id := peer.nextFeedback(t)
	if first&0xc0 != 0x40 || id != uint64(request.StreamID()) {
		t.Fatal("abandoned trailers lacked cancellation")
	}
	next, server := peer.open(t, peer.ctx)
	fathomryQPACKFrame(t, server, 1, []byte{2, 0, 0xd9, 0x80}, true)
	result, err = next.ReadResponse()
	if err != nil {
		t.Fatal("dynamic sibling failed", err)
	}
	if _, err := io.ReadAll(result.Body); err != nil {
		t.Fatal(err)
	}
	_ = result.Body.Close()
	first, id = peer.nextFeedback(t)
	if first&0x80 == 0 || id != uint64(next.StreamID()) {
		t.Fatal("duplicate cancellation or ACK after cancellation", first, id)
	}
}
