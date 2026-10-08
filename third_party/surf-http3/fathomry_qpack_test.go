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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/qpack"
	"github.com/quic-go/quic-go"
)

func reviewFeedbackContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func reviewFeedbackPair(t *testing.T, configured ...*quic.Config) (*quic.Conn, *quic.Conn) {
	t.Helper()
	certificate := httptest.NewTLSServer(nil)
	t.Cleanup(certificate.Close)
	peerConfig := &quic.Config{MaxIncomingUniStreams: 8}
	if len(configured) > 0 && configured[0] != nil {
		peerConfig = configured[0].Clone()
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: certificate.TLS.Certificates, NextProtos: []string{NextProtoH3}}, peerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	roots := x509.NewCertPool()
	roots.AddCert(certificate.Certificate())
	client, err := quic.DialAddr(reviewFeedbackContext(t), listener.Addr().String(), &tls.Config{RootCAs: roots, NextProtos: []string{NextProtoH3}}, &quic.Config{MaxIncomingUniStreams: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.CloseWithError(0, "") })
	peer, err := listener.Accept(reviewFeedbackContext(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.CloseWithError(0, "") })
	return client, peer
}

func reviewFeedbackInteger(prefix uint, mask byte, value uint64) []byte {
	maximum := uint64(1<<prefix) - 1
	if value < maximum {
		return []byte{mask | byte(value)}
	}
	result := []byte{mask | byte(maximum)}
	for value -= maximum; value >= 128; value >>= 7 {
		result = append(result, byte(value&127)|128)
	}
	return append(result, byte(value))
}

func reviewFeedbackSeed(t *testing.T, state *qpackConnection) {
	t.Helper()
	data := reviewFeedbackInteger(5, 0x20, state.tableCapacity)
	data = append(data, 0x41, 'x', 1, 'v')
	if err := state.decoder.ParseEncoder(bytes.NewReader(data), func(uint64) error { return nil }); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func reviewFeedbackRun(t *testing.T, state *qpackConnection) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); state.writeFeedback() }()
	t.Cleanup(func() {
		_ = state.conn.CloseWithError(0, "")
		state.stop(context.Canceled)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("connection-owned decoder writer did not stop")
		}
	})
}

func reviewFeedbackReader(t *testing.T, peer *quic.Conn) (*quic.ReceiveStream, *bufio.Reader) {
	t.Helper()
	stream, err := peer.AcceptUniStream(reviewFeedbackContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var kind [1]byte
	if _, err := io.ReadFull(stream, kind[:]); err != nil || kind[0] != 3 {
		t.Fatal("missing decoder-stream type", kind, err)
	}
	return stream, bufio.NewReader(stream)
}

func reviewFeedbackQuiet(t *testing.T, stream *quic.ReceiveStream, reader *bufio.Reader) {
	t.Helper()
	if err := stream.SetReadDeadline(time.Now().Add(80 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	value, err := reader.ReadByte()
	var timed net.Error
	if !errors.As(err, &timed) || !timed.Timeout() {
		t.Fatalf("unexpected extra feedback: byte=%x error=%v", value, err)
	}
	if err := stream.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
}

func reviewFeedbackField(t *testing.T, section *qpackSection, block []byte, want qpack.HeaderField) {
	t.Helper()
	decode := section.Decode(block)
	field, err := decode()
	if err != nil || field != want {
		t.Fatal("field decode", field, err)
	}
	if _, err := decode(); err != io.EOF {
		t.Fatal("field section completion", err)
	}
}

func TestFathomryCandidateQPACKACKAdvancesKnownCountOnRealStream(t *testing.T) {
	client, peer := reviewFeedbackPair(t)
	state := newQPACKConnection(client, 128, 4, 16)
	reviewFeedbackSeed(t, state)
	section := state.section(reviewFeedbackContext(t), 0, 128)
	t.Cleanup(section.close)
	reviewFeedbackField(t, section, []byte{2, 0, 0x80}, qpack.HeaderField{Name: "x", Value: "v"})
	if err := state.insertedCount(1); err != nil {
		t.Fatal(err)
	}
	reviewFeedbackRun(t, state)
	stream, reader := reviewFeedbackReader(t, peer)
	if value, err := reader.ReadByte(); err != nil || value != 0x80 {
		t.Fatal("first section ACK", value, err)
	}
	reviewFeedbackQuiet(t, stream, reader)
	if err := state.decoder.ParseEncoder(bytes.NewReader([]byte{0x41, 'y', 1, 'w'}), state.insertedCount); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if value, err := reader.ReadByte(); err != nil || value != 1 {
		t.Fatal("later insert did not advance exactly one beyond the ACK", value, err)
	}
	for _, control := range []struct {
		block []byte
		want  qpack.HeaderField
	}{
		{[]byte{3, 0, 0x80}, qpack.HeaderField{Name: "y", Value: "w"}},
		{[]byte{2, 0, 0x80}, qpack.HeaderField{Name: "x", Value: "v"}},
	} {
		reviewFeedbackField(t, section, control.block, control.want)
		if value, err := reader.ReadByte(); err != nil || value != 0x80 {
			t.Fatal("same-stream later section lost its own ordered ACK", value, err)
		}
	}
	section.close()
	if value, err := reader.ReadByte(); err != nil || value != 0x40 {
		t.Fatal("cancellation was not ordered after completed section ACKs", value, err)
	}
	reviewFeedbackQuiet(t, stream, reader)
}

func TestFathomryCandidateQPACKConcurrentACKCancelPreservesFIFO(t *testing.T) {
	const raced = 128
	client, peer := reviewFeedbackPair(t)
	state := newQPACKConnection(client, 128, 4, 2*(raced+2))
	reviewFeedbackSeed(t, state)
	ctx := reviewFeedbackContext(t)
	first := state.section(ctx, 0, 128)
	reviewFeedbackField(t, first, []byte{2, 0, 0x80}, qpack.HeaderField{Name: "x", Value: "v"})
	first.close()
	second := state.section(ctx, 4, 128)
	second.close()
	if _, err := second.Decode([]byte{2, 0, 0x80})(); !errors.Is(err, context.Canceled) {
		t.Fatal("closed-first control decoded or acknowledged a section", err)
	}
	start := make(chan struct{})
	results := make(chan error, raced)
	var workers sync.WaitGroup
	for index := range raced {
		section := state.section(ctx, uint64(index+2)*4, 128)
		workers.Go(func() {
			<-start
			field, err := section.Decode([]byte{2, 0, 0x80})()
			if err == nil && field != (qpack.HeaderField{Name: "x", Value: "v"}) {
				err = errors.New("race changed decoded field")
			}
			results <- err
		})
		workers.Go(func() { <-start; section.close() })
	}
	close(start)
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("ACK/cancel race deadlocked")
	}
	for range raced {
		if err := <-results; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal("ACK/cancel race returned another error", err)
		}
	}
	state.mu.Lock()
	queued := append([]qpackFeedback(nil), state.queue...)
	state.mu.Unlock()
	canceled := make(map[uint64]bool)
	acknowledged := make(map[uint64]bool)
	var want []byte
	for _, event := range queued {
		if canceled[event.stream] {
			t.Fatal("ACK or duplicate cancellation followed cancellation", event.stream)
		}
		if event.cancel {
			canceled[event.stream] = true
			want = append(want, reviewFeedbackInteger(6, 0x40, event.stream)...)
		} else {
			if acknowledged[event.stream] || event.required != 1 {
				t.Fatal("single section generated duplicate/incorrect ACK")
			}
			acknowledged[event.stream] = true
			want = append(want, reviewFeedbackInteger(7, 0x80, event.stream)...)
		}
	}
	if len(canceled) != raced+2 || !acknowledged[0] || acknowledged[4] {
		t.Fatal("positive/rejecting ACK/cancel controls changed")
	}
	if err := state.insertedCount(1); err != nil {
		t.Fatal(err)
	}
	reviewFeedbackRun(t, state)
	stream, reader := reviewFeedbackReader(t, peer)
	got := make([]byte, len(want))
	if _, err := io.ReadFull(reader, got); err != nil || !bytes.Equal(got, want) {
		t.Fatal("wire feedback differs from serialized queue order", err)
	}
	reviewFeedbackQuiet(t, stream, reader)
}
