// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/nukilabs/http"
	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/http3"
	"github.com/nukilabs/quic-go/quicvarint"
)

func TestFathomryTunnelDeadlineStopIsIdempotent(t *testing.T) {
	for _, fired := range []bool{false, true} {
		deadline := newTunnelDeadline()
		deadline.timerDone = make(chan struct{})
		done := deadline.timerDone
		delay := time.Hour
		if fired {
			delay = 0
		}
		deadline.timer = time.AfterFunc(delay, func() { close(done) })
		if fired {
			<-done
		}
		deadline.stop(io.EOF)
		if deadline.timer != nil || deadline.timerDone != nil {
			t.Fatal("stopped timer retained a callback join that cannot complete")
		}
		deadline.stop(net.ErrClosed)
		if !errors.Is(context.Cause(deadline.context), io.EOF) {
			t.Fatal("cleanup changed the first terminal cause", context.Cause(deadline.context))
		}
	}
}

type capsuleTestStream struct {
	*deadlineStream
	reader *io.PipeReader
	writer *io.PipeWriter
}

func newCapsuleTestStream() *capsuleTestStream {
	reader, writer := io.Pipe()
	return &capsuleTestStream{deadlineStream: &deadlineStream{incoming: make(chan []byte, 1), entered: make(chan struct{}, 1), closed: make(chan struct{})}, reader: reader, writer: writer}
}
func (stream *capsuleTestStream) Read(data []byte) (int, error) { return stream.reader.Read(data) }
func (stream *capsuleTestStream) CancelRead(quic.StreamErrorCode) {
	_ = stream.reader.CloseWithError(io.ErrClosedPipe)
}
func (stream *capsuleTestStream) Close() error {
	_ = stream.reader.Close()
	_ = stream.writer.Close()
	return stream.deadlineStream.Close()
}

func TestFathomryCapsuleIngressQueueBoundAndJoinedClose(t *testing.T) {
	stream := newCapsuleTestStream()
	defer stream.Close()
	target := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443}
	tunnel := newH3Conn(stream, target, target)
	defer tunnel.Close()
	var releases atomic.Int32
	tunnel.release = func() { releases.Add(1) }
	written := make(chan error, 1)
	go func() {
		for index := range 2 * tunnelDatagramSlots {
			if err := http3.WriteCapsule(quicvarint.NewWriter(stream.writer), 0, []byte{0, byte(index)}); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("saturated ingress blocked control-stream progress")
	}
	if len(tunnel.datagrams) != tunnelDatagramSlots || FathomryTunnelIngressBytes < (tunnelDatagramSlots+2)*(64<<10) {
		t.Fatal("ingress queue or declared source envelope is not bounded", len(tunnel.datagrams), FathomryTunnelIngressBytes)
	}
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
	for _, done := range []<-chan struct{}{tunnel.readDone, tunnel.datagramDone} {
		select {
		case <-done:
		default:
			t.Fatal("Close returned before an ingress worker ended")
		}
	}
	if !tunnel.ReleaseConfirmed() || len(tunnel.datagrams) != 0 || releases.Load() != 1 {
		t.Fatal("Close retained ingress packets or tunnel admission", len(tunnel.datagrams), releases.Load())
	}
}

func TestFathomryDatagramContextAndPayloadBounds(t *testing.T) {
	for _, scenario := range []string{"empty-packet", "partial-context", "oversized", "empty-udp"} {
		t.Run(scenario, func(t *testing.T) {
			stream := newCapsuleTestStream()
			defer stream.Close()
			target := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443}
			tunnel := newH3Conn(stream, target, target)
			defer tunnel.Close()
			_ = tunnel.SetReadDeadline(time.Now().Add(time.Second))
			var payload []byte
			switch scenario {
			case "partial-context":
				payload = []byte{0x40}
			case "oversized":
				payload = make([]byte, maxTunnelPayload+2)
			case "empty-udp":
				payload = []byte{0}
			}
			stream.incoming <- payload
			count, address, err := tunnel.ReadFrom(make([]byte, 1))
			switch scenario {
			case "empty-udp":
				if count != 0 || address == nil || address.String() != target.String() || err != nil {
					t.Fatal("valid zero-length UDP payload was rejected", count, address, err)
				}
			case "oversized":
				var limit *quic.DatagramTooLargeError
				if !errors.As(err, &limit) || limit.MaxDatagramPayloadSize != maxTunnelPayload {
					t.Fatal("oversized datagram bound lost", err)
				}
			default:
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal("missing/truncated datagram context looked like clean EOF", err)
				}
			}
		})
	}
}

func TestFathomryCapsuleResponseNegotiation(t *testing.T) {
	for _, value := range []string{"?1", ` ?1; flag; number=-2; text="a; b"; binary=:YQ==:; token=abc; decimal=1.25 `} {
		response := &http.Response{StatusCode: 200, ContentLength: 0, Header: http.Header{http3.CapsuleProtocolHeader: {value}}}
		if err := validateCapsuleResponse(response); err != nil {
			t.Fatal("legal structured Boolean or synthesized native length rejected", value, err)
		}
	}
	for _, fields := range [][]string{nil, {"?0"}, {"true"}, {"?1", "?1"}, {"?1, ?1"}, {`?1; broken="`}} {
		response := &http.Response{StatusCode: 200, Header: http.Header{http3.CapsuleProtocolHeader: fields}}
		if err := validateCapsuleResponse(response); err == nil {
			t.Fatal("invalid structured Boolean negotiated capsules", fields)
		}
	}
	for _, status := range []int{204, 205, 206} {
		if err := validateCapsuleResponse(&http.Response{StatusCode: status, Header: http.Header{http3.CapsuleProtocolHeader: {"?1"}}}); err == nil {
			t.Fatal("prohibited capsule response status admitted", status)
		}
	}
	for _, field := range []string{"Content-Length", "Content-Type", "Transfer-Encoding"} {
		response := &http.Response{StatusCode: 200, Header: http.Header{http3.CapsuleProtocolHeader: {"?1"}, field: {""}}}
		if err := validateCapsuleResponse(response); err == nil {
			t.Fatal("prohibited capsule response field admitted", field)
		}
	}
}
