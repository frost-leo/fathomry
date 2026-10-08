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

package surf

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/g"
	http "github.com/enetx/http"
	h3 "github.com/enetx/http3"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
	"golang.org/x/net/http2/hpack"
)

type reviewQPACKFeedback struct {
	kind  string
	value uint64
}

type reviewQPACKPeer struct {
	t                 *testing.T
	ctx               context.Context
	connection        chan *quic.Conn
	feedback          chan reviewQPACKFeedback
	feedbackStream    chan *quic.ReceiveStream
	settings          chan map[uint64]uint64
	capacity, blocked uint64
	url               string
	transport         *h3.Transport
}

func reviewQPACKInteger(prefix byte, bits uint, value uint64) []byte {
	maximum := uint64(1<<bits) - 1
	if value < maximum {
		return []byte{prefix | byte(value)}
	}
	result := []byte{prefix | byte(maximum)}
	value -= maximum
	for value >= 128 {
		result = append(result, byte(value)|128)
		value >>= 7
	}
	return append(result, byte(value))
}

func reviewQPACKReadInteger(reader io.ByteReader, first byte, bits uint) (uint64, error) {
	maximum := uint64(1<<bits) - 1
	value := uint64(first) & maximum
	if value != maximum {
		return value, nil
	}
	for shift := uint(0); shift < 63; shift += 7 {
		part, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		value += uint64(part&127) << shift
		if part&128 == 0 {
			return value, nil
		}
	}
	return 0, errors.New("peer feedback integer overflow")
}

func reviewQPACKString(value string, huffman bool) []byte {
	data := []byte(value)
	flag := byte(0)
	if huffman {
		data = hpack.AppendHuffmanString(nil, value)
		flag = 128
	}
	return append(reviewQPACKInteger(flag, 7, uint64(len(data))), data...)
}

func reviewQPACKInsert(name, value string, huffman bool) []byte {
	data := []byte(name)
	flag := byte(0x40)
	if huffman {
		data = hpack.AppendHuffmanString(nil, name)
		flag |= 0x20
	}
	result := append(reviewQPACKInteger(flag, 5, uint64(len(data))), data...)
	return append(result, reviewQPACKString(value, huffman)...)
}

func reviewQPACKCapacity(value uint64) []byte { return reviewQPACKInteger(0x20, 5, value) }

func reviewQPACKFrame(writer io.Writer, kind uint64, payload []byte) error {
	data := quicvarint.Append(nil, kind)
	data = quicvarint.Append(data, uint64(len(payload)))
	_, err := writer.Write(append(data, payload...))
	return err
}

func newReviewQPACKPeer(t *testing.T, capacity, blocked uint64) *reviewQPACKPeer {
	t.Helper()
	cert := newPeers(t, func(stdhttp.ResponseWriter, *stdhttp.Request) {}, false)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.Listen(packet, &tls.Config{Certificates: cert.tcp.TLS.Certificates, NextProtos: []string{"h3"}}, &quic.Config{})
	if err != nil {
		_ = packet.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	peer := &reviewQPACKPeer{t: t, ctx: ctx, connection: make(chan *quic.Conn, 1), feedback: make(chan reviewQPACKFeedback, 128), feedbackStream: make(chan *quic.ReceiveStream, 1), settings: make(chan map[uint64]uint64, 1), capacity: capacity, blocked: blocked, url: "https://" + packet.LocalAddr().String()}
	var settings g.MapOrd[uint64, uint64]
	settings.Insert(1, capacity)
	settings.Insert(7, blocked)
	peer.transport = &h3.Transport{TLSClientConfig: &tls.Config{RootCAs: cert.roots}, AdditionalSettings: settings, MaxResponseHeaderBytes: 2048}
	var group sync.WaitGroup
	group.Go(func() {
		connection, err := listener.Accept(ctx)
		if err != nil {
			return
		}
		stop := context.AfterFunc(ctx, func() { _ = connection.CloseWithError(0, "") })
		defer stop()
		control, err := connection.OpenUniStreamSync(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := control.Write([]byte{0, 4, 0}); err != nil {
			t.Error(err)
			return
		}
		peer.connection <- connection
		for {
			stream, err := connection.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			group.Go(func() { peer.receiveUni(stream) })
		}
	})
	t.Cleanup(func() {
		_ = peer.transport.Close()
		cancel()
		_ = listener.Close()
		_ = packet.Close()
		done := make(chan struct{})
		go func() { group.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("QPACK peer cleanup stalled")
		}
	})
	return peer
}

func (peer *reviewQPACKPeer) receiveUni(stream *quic.ReceiveStream) {
	reader := bufio.NewReader(stream)
	kind, err := quicvarint.Read(reader)
	if err != nil {
		return
	}
	if kind == 0 {
		frame, err := quicvarint.Read(reader)
		if err != nil || frame != 4 {
			peer.t.Error("client SETTINGS absent", err)
			return
		}
		length, err := quicvarint.Read(reader)
		if err != nil || length > 4096 {
			peer.t.Error("client SETTINGS size", err)
			return
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			peer.t.Error(err)
			return
		}
		values := make(map[uint64]uint64)
		fields := bytes.NewReader(data)
		for fields.Len() > 0 {
			key, err := quicvarint.Read(fields)
			if err != nil {
				peer.t.Error(err)
				return
			}
			value, err := quicvarint.Read(fields)
			if err != nil {
				peer.t.Error(err)
				return
			}
			values[key] = value
		}
		peer.settings <- values
		_, _ = io.Copy(io.Discard, reader)
		return
	}
	if kind != 3 {
		_, _ = io.Copy(io.Discard, reader)
		return
	}
	peer.feedbackStream <- stream
	for {
		first, err := reader.ReadByte()
		if err != nil {
			return
		}
		instruction := reviewQPACKFeedback{kind: "increment"}
		bits := uint(6)
		if first&0x80 != 0 {
			instruction.kind = "ack"
			bits = 7
		} else if first&0x40 != 0 {
			instruction.kind = "cancel"
		}
		instruction.value, err = reviewQPACKReadInteger(reader, first, bits)
		if err != nil {
			peer.t.Error(err)
			return
		}
		select {
		case peer.feedback <- instruction:
		case <-peer.ctx.Done():
			return
		}
	}
}

func (peer *reviewQPACKPeer) connect() *quic.Conn {
	peer.t.Helper()
	select {
	case connection := <-peer.connection:
		select {
		case settings := <-peer.settings:
			if settings[1] != peer.capacity || settings[7] != peer.blocked {
				peer.t.Fatal("advertised QPACK authority differs", settings)
			}
		case <-peer.ctx.Done():
			peer.t.Fatal("client QPACK SETTINGS not observed")
		}
		return connection
	case <-peer.ctx.Done():
		peer.t.Fatal("QPACK peer connection absent")
		return nil
	}
}

func (peer *reviewQPACKPeer) request(ctx context.Context) <-chan struct {
	response *http.Response
	err      error
} {
	result := make(chan struct {
		response *http.Response
		err      error
	}, 1)
	input := request(peer.t, "GET", peer.url, "").WithContext(ctx)
	go func() {
		response, err := peer.transport.RoundTrip(input)
		result <- struct {
			response *http.Response
			err      error
		}{response, err}
	}()
	return result
}

func (peer *reviewQPACKPeer) requestStream(connection *quic.Conn) *quic.Stream {
	peer.t.Helper()
	stream, err := connection.AcceptStream(peer.ctx)
	if err != nil {
		peer.t.Fatal(err)
	}
	reader := quicvarint.NewReader(stream)
	kind, err := quicvarint.Read(reader)
	if err != nil || kind != 1 {
		peer.t.Fatal("client HEADERS absent", err)
	}
	length, err := quicvarint.Read(reader)
	if err != nil || length > 64<<10 {
		peer.t.Fatal("client HEADERS size", err)
	}
	if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
		peer.t.Fatal(err)
	}
	return stream
}

func (peer *reviewQPACKPeer) instructions(connection *quic.Conn, kind uint64, data []byte, finish bool) *quic.SendStream {
	peer.t.Helper()
	stream, err := connection.OpenUniStreamSync(peer.ctx)
	if err != nil {
		peer.t.Fatal(err)
	}
	if _, err := stream.Write(append(quicvarint.Append(nil, kind), data...)); err != nil {
		peer.t.Fatal(err)
	}
	if finish {
		if err := stream.Close(); err != nil {
			peer.t.Fatal(err)
		}
	}
	return stream
}

func (peer *reviewQPACKPeer) response(stream *quic.Stream, fields []byte) {
	peer.t.Helper()
	if err := reviewQPACKFrame(stream, 1, fields); err != nil {
		peer.t.Fatal(err)
	}
	if err := reviewQPACKFrame(stream, 0, []byte("ok")); err != nil {
		peer.t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		peer.t.Fatal(err)
	}
}

func (peer *reviewQPACKPeer) feedbackFor(kind string, value uint64) {
	peer.t.Helper()
	for {
		select {
		case got := <-peer.feedback:
			if got.kind == kind && got.value == value {
				return
			}
		case <-peer.ctx.Done():
			peer.t.Fatalf("missing decoder feedback %s=%d", kind, value)
		}
	}
}

func reviewQPACKConsume(t *testing.T, result <-chan struct {
	response *http.Response
	err      error
}, ctx context.Context) *http.Response {
	t.Helper()
	select {
	case got := <-result:
		if got.err != nil || got.response == nil {
			t.Fatal("wire response rejected", got.err)
		}
		data, err := io.ReadAll(got.response.Body)
		_ = got.response.Body.Close()
		if err != nil || string(data) != "ok" {
			t.Fatal("wire body changed", err)
		}
		return got.response
	case <-ctx.Done():
		t.Fatal("wire response did not complete")
		return nil
	}
}

func TestQPACKWireRepresentations(t *testing.T) {
	insert := reviewQPACKInsert("x-wire", "first", false)
	for _, control := range []struct {
		name                 string
		instructions, fields []byte
		key, value           string
		ric                  uint64
	}{
		{name: "static", fields: []byte{0, 0, 0xd9}},
		{name: "static-nonzero-base", fields: []byte{0, 7, 0xd9}},
		{name: "dynamic-indexed", instructions: insert, fields: []byte{2, 0, 0xd9, 0x80}, key: "X-Wire", value: "first", ric: 1},
		{name: "dynamic-name-literal", instructions: insert, fields: append([]byte{2, 0, 0xd9, 0x40}, reviewQPACKString("second", false)...), key: "X-Wire", value: "second", ric: 1},
		{name: "post-base-indexed", instructions: insert, fields: []byte{2, 0x80, 0xd9, 0x10}, key: "X-Wire", value: "first", ric: 1},
		{name: "post-base-name-literal", instructions: insert, fields: append([]byte{2, 0x80, 0xd9, 0x00}, reviewQPACKString("second", false)...), key: "X-Wire", value: "second", ric: 1},
		{name: "duplicate", instructions: append(append([]byte{}, insert...), 0), fields: []byte{3, 0, 0xd9, 0x80}, key: "X-Wire", value: "first", ric: 2},
		{name: "insert-dynamic-name", instructions: append(append(append([]byte{}, insert...), 0x80), reviewQPACKString("second", false)...), fields: []byte{3, 0, 0xd9, 0x80}, key: "X-Wire", value: "second", ric: 2},
		{name: "insert-static-name", instructions: append([]byte{0xc0 | 29}, reviewQPACKString("value", false)...), fields: []byte{2, 0, 0xd9, 0x80}, key: "Accept", value: "value", ric: 1},
		{name: "huffman-insert", instructions: reviewQPACKInsert("x-wire", "first", true), fields: []byte{2, 0, 0xd9, 0x80}, key: "X-Wire", value: "first", ric: 1},
	} {
		t.Run(control.name, func(t *testing.T) {
			peer := newReviewQPACKPeer(t, 128, 1)
			result := peer.request(peer.ctx)
			connection := peer.connect()
			stream := peer.requestStream(connection)
			if len(control.instructions) > 0 {
				peer.instructions(connection, 2, append(reviewQPACKCapacity(128), control.instructions...), false)
			}
			peer.response(stream, control.fields)
			response := reviewQPACKConsume(t, result, peer.ctx)
			if control.key != "" && response.Header.Get(control.key) != control.value {
				t.Fatalf("decoded field %s=%q", control.key, response.Header.Get(control.key))
			}
			if control.ric > 0 {
				peer.feedbackFor("ack", uint64(stream.StreamID()))
			}
		})
	}
}

func TestQPACKBlockUnblockAndCancel(t *testing.T) {
	for _, cancelBlocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel-%v", cancelBlocked), func(t *testing.T) {
			peer := newReviewQPACKPeer(t, 128, 1)
			ctx, cancel := context.WithCancel(peer.ctx)
			defer cancel()
			result := peer.request(ctx)
			connection := peer.connect()
			stream := peer.requestStream(connection)
			encoder := peer.instructions(connection, 2, reviewQPACKCapacity(128), false)
			if err := reviewQPACKFrame(stream, 1, []byte{2, 0, 0xd9, 0x80}); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-result:
				if got.response != nil {
					_ = got.response.Body.Close()
				}
				t.Fatal("field section did not block", got.err)
			case <-time.After(40 * time.Millisecond):
			}
			if cancelBlocked {
				cancel()
				select {
				case got := <-result:
					if !errors.Is(got.err, context.Canceled) {
						t.Fatal("blocked cancellation lost", got.err)
					}
				case <-peer.ctx.Done():
					t.Fatal("blocked decode ignored cancellation")
				}
				peer.feedbackFor("cancel", uint64(stream.StreamID()))
			}
			if _, err := encoder.Write(reviewQPACKInsert("x-wire", "first", false)); err != nil {
				t.Fatal(err)
			}
			if cancelBlocked {
				result = peer.request(peer.ctx)
				stream = peer.requestStream(connection)
				peer.response(stream, []byte{2, 0, 0xd9, 0x80})
			} else {
				if err := reviewQPACKFrame(stream, 0, []byte("ok")); err != nil {
					t.Fatal(err)
				}
				_ = stream.Close()
			}
			response := reviewQPACKConsume(t, result, peer.ctx)
			if response.Header.Get("X-Wire") != "first" {
				t.Fatal("unblocked field differs")
			}
			peer.feedbackFor("ack", uint64(stream.StreamID()))
		})
	}
}

func TestQPACKWrapEvictionAndEncoderNameCopy(t *testing.T) {
	peer := newReviewQPACKPeer(t, 64, 1)
	first := peer.request(peer.ctx)
	connection := peer.connect()
	encoder := peer.instructions(connection, 2, reviewQPACKCapacity(64), false)
	var retained []*http.Response
	for count := uint64(1); count <= 7; count++ {
		result := first
		if count > 1 {
			result = peer.request(peer.ctx)
		}
		stream := peer.requestStream(connection)
		instruction := reviewQPACKInsert("x-n", fmt.Sprint(count), false)
		if count == 4 {
			if _, err := encoder.Write(append(reviewQPACKCapacity(0), reviewQPACKCapacity(64)...)); err != nil {
				t.Fatal(err)
			}
		}
		if count > 1 && count != 4 {
			instruction = append([]byte{0x80}, reviewQPACKString(fmt.Sprint(count), false)...)
		}
		if _, err := encoder.Write(instruction); err != nil {
			t.Fatal(err)
		}
		peer.response(stream, []byte{byte(count%4 + 1), 0, 0xd9, 0x80})
		response := reviewQPACKConsume(t, result, peer.ctx)
		retained = append(retained, response)
		if response.Header.Get("X-N") != fmt.Sprint(count) {
			t.Fatalf("wrapped required insert count %d decoded %q", count, response.Header.Get("X-N"))
		}
		peer.feedbackFor("ack", uint64(stream.StreamID()))
	}
	for index, response := range retained {
		if response.Header.Get("X-N") != fmt.Sprint(index+1) {
			t.Fatal("eviction mutated a previously returned field")
		}
	}
}

func TestQPACKDuplicateCriticalAndValidCancellation(t *testing.T) {
	for _, kind := range []uint64{2, 3} {
		t.Run(fmt.Sprintf("duplicate-%d", kind), func(t *testing.T) {
			peer := newReviewQPACKPeer(t, 128, 1)
			result := peer.request(peer.ctx)
			connection := peer.connect()
			peer.requestStream(connection)
			peer.instructions(connection, kind, nil, false)
			peer.instructions(connection, kind, nil, false)
			select {
			case <-connection.Context().Done():
				var closeError *quic.ApplicationError
				if !errors.As(context.Cause(connection.Context()), &closeError) || closeError.ErrorCode != 0x103 {
					t.Fatal("duplicate critical stream code", context.Cause(connection.Context()))
				}
			case <-peer.ctx.Done():
				t.Fatal("duplicate critical stream ignored")
			}
			select {
			case got := <-result:
				if got.err == nil {
					if got.response != nil {
						_ = got.response.Body.Close()
					}
					t.Fatal("duplicate critical stream succeeded")
				}
			case <-peer.ctx.Done():
				t.Fatal("native waiter survived duplicate stream")
			}
		})
	}
	t.Run("cancellation-with-no-dynamic-request", func(t *testing.T) {
		peer := newReviewQPACKPeer(t, 128, 1)
		result := peer.request(peer.ctx)
		connection := peer.connect()
		stream := peer.requestStream(connection)
		peer.instructions(connection, 3, reviewQPACKInteger(0x40, 6, uint64(stream.StreamID())), false)
		peer.response(stream, []byte{0, 0, 0xd9})
		reviewQPACKConsume(t, result, peer.ctx)
	})
}

func TestQPACKInformationalAndTrailers(t *testing.T) {
	peer := newReviewQPACKPeer(t, 128, 1)
	result := peer.request(peer.ctx)
	connection := peer.connect()
	stream := peer.requestStream(connection)
	peer.instructions(connection, 2, append(reviewQPACKCapacity(128), reviewQPACKInsert("x-wire", "first", false)...), false)
	for _, fields := range [][]byte{{2, 0, 0xd8, 0x80}, {2, 0, 0xd9, 0x80}} {
		if err := reviewQPACKFrame(stream, 1, fields); err != nil {
			t.Fatal(err)
		}
	}
	if err := reviewQPACKFrame(stream, 0, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := reviewQPACKFrame(stream, 1, []byte{2, 0, 0x80}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	response := reviewQPACKConsume(t, result, peer.ctx)
	if response.StatusCode != 200 || response.Header.Get("X-Wire") != "first" || response.Trailer.Get("X-Wire") != "first" {
		t.Fatal("informational/final/trailer field state crossed sections")
	}
	for range 3 {
		peer.feedbackFor("ack", uint64(stream.StreamID()))
	}
}

func TestQPACKEvictedReference(t *testing.T) {
	peer := newReviewQPACKPeer(t, 64, 1)
	result := peer.request(peer.ctx)
	connection := peer.connect()
	stream := peer.requestStream(connection)
	encoder := peer.instructions(connection, 2, append(reviewQPACKCapacity(64), reviewQPACKInsert("x-n", "1", false)...), false)
	peer.response(stream, []byte{2, 0, 0xd9, 0x80})
	reviewQPACKConsume(t, result, peer.ctx)
	peer.feedbackFor("ack", uint64(stream.StreamID()))
	result = peer.request(peer.ctx)
	stream = peer.requestStream(connection)
	if _, err := encoder.Write(reviewQPACKInsert("x-n", "2", false)); err != nil {
		t.Fatal(err)
	}
	if err := reviewQPACKFrame(stream, 1, []byte{3, 0, 0xd9, 0x81}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connection.Context().Done():
		var closeError *quic.ApplicationError
		if !errors.As(context.Cause(connection.Context()), &closeError) || closeError.ErrorCode != 0x200 {
			t.Fatal("evicted reference did not close connection with decompression error", context.Cause(connection.Context()))
		}
	case <-peer.ctx.Done():
		t.Fatal("evicted reference left connection alive")
	}
	select {
	case got := <-result:
		if got.err == nil {
			if got.response != nil {
				_ = got.response.Body.Close()
			}
			t.Fatal("evicted reference succeeded")
		}
	case <-peer.ctx.Done():
		t.Fatal("invalid field retained waiter")
	}
}

func TestQPACKConnectionErrors(t *testing.T) {
	for _, control := range []struct {
		name                 string
		kind                 uint64
		instructions, fields []byte
		finish               bool
		reset                bool
		blocked              uint64
		code                 quic.ApplicationErrorCode
	}{
		{name: "capacity-overrun", kind: 2, instructions: reviewQPACKCapacity(129), blocked: 1, code: 0x201},
		{name: "entry-overrun", kind: 2, instructions: append(reviewQPACKCapacity(32), reviewQPACKInsert("x-wire", "v", false)...), blocked: 1, code: 0x201},
		{name: "insert-invalid-dynamic-name", kind: 2, instructions: append(reviewQPACKCapacity(128), 0x80, 0), blocked: 1, code: 0x201},
		{name: "duplicate-empty", kind: 2, instructions: append(reviewQPACKCapacity(128), 0), blocked: 1, code: 0x201},
		{name: "invalid-huffman", kind: 2, instructions: append(reviewQPACKCapacity(128), 0x61, 0xff, 0), blocked: 1, code: 0x201},
		{name: "encoder-critical-fin", kind: 2, finish: true, blocked: 1, code: 0x104},
		{name: "decoder-critical-fin", kind: 3, finish: true, blocked: 1, code: 0x104},
		{name: "encoder-critical-reset", kind: 2, instructions: append(reviewQPACKCapacity(128), reviewQPACKInsert("x-wire", "first", false)...), reset: true, blocked: 1, code: 0x104},
		{name: "decoder-critical-reset-mid-integer", kind: 3, instructions: []byte{0xff}, reset: true, blocked: 1, code: 0x104},
		{name: "decoder-zero-increment", kind: 3, instructions: []byte{0}, blocked: 1, code: 0x202},
		{name: "decoder-unearned-increment", kind: 3, instructions: []byte{1}, blocked: 1, code: 0x202},
		{name: "decoder-unearned-ack", kind: 3, instructions: []byte{0x80}, blocked: 1, code: 0x202},
		{name: "dynamic-with-zero-ric", fields: []byte{0, 0, 0xd9, 0x80}, blocked: 1, code: 0x200},
		{name: "ric-outside-full-range", fields: []byte{9, 0, 0xd9}, blocked: 1, code: 0x200},
		{name: "negative-base", fields: []byte{2, 0x81, 0xd9}, blocked: 1, code: 0x200},
		{name: "reference-at-required-count", kind: 2, instructions: append(append(reviewQPACKCapacity(128), reviewQPACKInsert("x-n", "1", false)...), reviewQPACKInsert("x-n", "2", false)...), fields: []byte{2, 1, 0xd9, 0x80}, blocked: 1, code: 0x200},
		{name: "invalid-static-index", fields: []byte{0, 0, 0xff, 36}, blocked: 1, code: 0x200},
		{name: "blocked-ceiling-zero", fields: []byte{2, 0, 0xd9, 0x80}, code: 0x200},
	} {
		t.Run(control.name, func(t *testing.T) {
			peer := newReviewQPACKPeer(t, 128, control.blocked)
			result := peer.request(peer.ctx)
			connection := peer.connect()
			stream := peer.requestStream(connection)
			if control.kind != 0 {
				instructionStream := peer.instructions(connection, control.kind, control.instructions, control.finish)
				if control.reset {
					if control.kind == 2 {
						peer.feedbackFor("increment", 1)
					} else {
						time.Sleep(40 * time.Millisecond)
					}
					instructionStream.CancelWrite(0x10c)
				}
			}
			if len(control.fields) > 0 {
				_ = reviewQPACKFrame(stream, 1, control.fields)
			}
			select {
			case <-connection.Context().Done():
				var closeError *quic.ApplicationError
				if !errors.As(context.Cause(connection.Context()), &closeError) || closeError.ErrorCode != control.code {
					t.Fatalf("connection code: got %v want %#x", context.Cause(connection.Context()), control.code)
				}
			case <-peer.ctx.Done():
				t.Fatalf("missing connection close %#x", control.code)
			}
			select {
			case got := <-result:
				if got.err == nil {
					if got.response != nil {
						_ = got.response.Body.Close()
					}
					t.Fatal("invalid QPACK succeeded")
				}
			case <-peer.ctx.Done():
				t.Fatal("native waiter survived QPACK connection error")
			}
		})
	}
}

func TestQPACKKnownCountFeedback(t *testing.T) {
	peer := newReviewQPACKPeer(t, 128, 1)
	first := peer.request(peer.ctx)
	connection := peer.connect()
	encoder := peer.instructions(connection, 2, reviewQPACKCapacity(128), false)
	var known, inserted uint64
	apply := func(feedback reviewQPACKFeedback, stream, required uint64) bool {
		switch feedback.kind {
		case "increment":
			if feedback.value == 0 || feedback.value > inserted-known {
				t.Fatalf("invalid known-count increment: known=%d increment=%d sent=%d", known, feedback.value, inserted)
			}
			known += feedback.value
		case "ack":
			if feedback.value != stream {
				t.Fatalf("unexpected ACK stream=%d want=%d", feedback.value, stream)
			}
			known = max(known, required)
			return true
		}
		return false
	}
	for count := uint64(1); count <= 20; count++ {
		result := first
		if count > 1 {
			result = peer.request(peer.ctx)
		}
		stream := peer.requestStream(connection)
		if count > 1 {
			peer.response(stream, []byte{byte(count%8 + 1), 0, 0xd9, 0x80})
		}
		inserted = count
		if _, err := encoder.Write(reviewQPACKInsert("x-n", fmt.Sprint(count), false)); err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			for known < count {
				select {
				case feedback := <-peer.feedback:
					if apply(feedback, uint64(stream.StreamID()), 0) {
						t.Fatal("ACK preceded field section")
					}
				case <-peer.ctx.Done():
					t.Fatal("initial insertion feedback absent")
				}
			}
			peer.response(stream, []byte{2, 0, 0xd9, 0x80})
		}
		response := reviewQPACKConsume(t, result, peer.ctx)
		if response.Header.Get("X-N") != fmt.Sprint(count) {
			t.Fatal("feedback case decoded wrong insertion")
		}
		acknowledged := false
		for !acknowledged {
			select {
			case feedback := <-peer.feedback:
				acknowledged = apply(feedback, uint64(stream.StreamID()), count)
			case <-peer.ctx.Done():
				t.Fatal("section ACK absent")
			}
		}
	}
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case feedback := <-peer.feedback:
			if feedback.kind != "cancel" {
				apply(feedback, ^uint64(0), 0)
			}
		case <-timer.C:
			return
		}
	}
}

func TestQPACKBlockedStreamSiblingIsolation(t *testing.T) {
	for _, secondDynamic := range []bool{false, true} {
		t.Run(fmt.Sprintf("second-dynamic-%v", secondDynamic), func(t *testing.T) {
			peer := newReviewQPACKPeer(t, 128, 1)
			first := peer.request(peer.ctx)
			connection := peer.connect()
			firstStream := peer.requestStream(connection)
			encoder := peer.instructions(connection, 2, reviewQPACKCapacity(128), false)
			if err := reviewQPACKFrame(firstStream, 1, []byte{2, 0, 0xd9, 0x80}); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-first:
				if got.response != nil {
					_ = got.response.Body.Close()
				}
				t.Fatal("first dynamic section was not blocked", got.err)
			case <-time.After(30 * time.Millisecond):
			}
			second := peer.request(peer.ctx)
			secondStream := peer.requestStream(connection)
			if secondDynamic {
				if err := reviewQPACKFrame(secondStream, 1, []byte{2, 0, 0xd9, 0x80}); err != nil {
					t.Fatal(err)
				}
				select {
				case <-connection.Context().Done():
					var closeError *quic.ApplicationError
					if !errors.As(context.Cause(connection.Context()), &closeError) || closeError.ErrorCode != 0x200 {
						t.Fatal("exceeded blocked-stream limit close code", context.Cause(connection.Context()))
					}
				case <-peer.ctx.Done():
					t.Fatal("second blocked stream exceeded advertised limit")
				}
				for _, result := range []<-chan struct {
					response *http.Response
					err      error
				}{first, second} {
					select {
					case got := <-result:
						if got.err == nil {
							if got.response != nil {
								_ = got.response.Body.Close()
							}
							t.Fatal("excess blocked section succeeded")
						}
					case <-peer.ctx.Done():
						t.Fatal("blocked section survived connection error")
					}
				}
				return
			}
			peer.response(secondStream, []byte{0, 0, 0xd9})
			reviewQPACKConsume(t, second, peer.ctx)
			select {
			case got := <-first:
				if got.response != nil {
					_ = got.response.Body.Close()
				}
				t.Fatal("static sibling changed blocked state", got.err)
			default:
			}
			if _, err := encoder.Write(reviewQPACKInsert("x-wire", "first", false)); err != nil {
				t.Fatal(err)
			}
			if err := reviewQPACKFrame(firstStream, 0, []byte("ok")); err != nil {
				t.Fatal(err)
			}
			if err := firstStream.Close(); err != nil {
				t.Fatal(err)
			}
			if response := reviewQPACKConsume(t, first, peer.ctx); response.Header.Get("X-Wire") != "first" {
				t.Fatal("unblocked sibling decoded wrong field")
			}
			peer.feedbackFor("ack", uint64(firstStream.StreamID()))
		})
	}
}

func TestQPACKShutdownJoinsBlockedWorkers(t *testing.T) {
	peer := newReviewQPACKPeer(t, 128, 1)
	var active atomic.Int32
	peer.transport.FathomryAcquireClient = func() (func(), error) { active.Add(1); return func() { active.Add(-1) }, nil }
	result := peer.request(peer.ctx)
	connection := peer.connect()
	stream := peer.requestStream(connection)
	peer.instructions(connection, 2, append(reviewQPACKCapacity(128), 0x46, 'x'), false)
	peer.instructions(connection, 3, []byte{0xff}, false)
	if err := reviewQPACKFrame(stream, 1, []byte{2, 0, 0xd9, 0x80}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.response != nil {
			_ = got.response.Body.Close()
		}
		t.Fatal("incomplete QPACK work was not pending", got.err)
	case <-time.After(30 * time.Millisecond):
	}
	closed := make(chan error, 1)
	go func() { closed <- peer.transport.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal("normal transport shutdown fabricated QPACK failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("transport close did not join blocked QPACK workers")
	}
	if active.Load() != 0 {
		t.Fatal("closed QPACK client retained reservation")
	}
	select {
	case got := <-result:
		if got.err == nil {
			if got.response != nil {
				_ = got.response.Body.Close()
			}
			t.Fatal("shutdown returned a false complete response")
		}
	case <-peer.ctx.Done():
		t.Fatal("QPACK waiter survived closed connection")
	}
}

func TestQPACKZeroAdvertisedCapacityRefusesInstructions(t *testing.T) {
	peer := newReviewQPACKPeer(t, 0, 0)
	result := peer.request(peer.ctx)
	connection := peer.connect()
	peer.requestStream(connection)
	peer.instructions(connection, 2, reviewQPACKCapacity(0), false)
	select {
	case <-connection.Context().Done():
		var closeError *quic.ApplicationError
		if !errors.As(context.Cause(connection.Context()), &closeError) || closeError.ErrorCode != 0x201 {
			t.Fatal("zero-capacity encoder instruction close code", context.Cause(connection.Context()))
		}
	case <-peer.ctx.Done():
		t.Fatal("encoder instruction accepted with advertised capacity zero")
	}
	select {
	case got := <-result:
		if got.err == nil {
			if got.response != nil {
				_ = got.response.Body.Close()
			}
			t.Fatal("invalid zero-capacity instruction succeeded")
		}
	case <-peer.ctx.Done():
		t.Fatal("zero-capacity protocol failure retained waiter")
	}
}

func TestQPACKPeerStopsDecoderCriticalStream(t *testing.T) {
	peer := newReviewQPACKPeer(t, 128, 1)
	result := peer.request(peer.ctx)
	connection := peer.connect()
	peer.requestStream(connection)
	select {
	case decoderStream := <-peer.feedbackStream:
		decoderStream.CancelRead(0x10c)
	case <-peer.ctx.Done():
		t.Fatal("client decoder stream absent")
	}
	time.Sleep(40 * time.Millisecond)
	peer.instructions(connection, 2, append(reviewQPACKCapacity(128), reviewQPACKInsert("x-wire", "first", false)...), false)
	select {
	case <-connection.Context().Done():
		var closeError *quic.ApplicationError
		if !errors.As(context.Cause(connection.Context()), &closeError) || closeError.ErrorCode != 0x104 {
			t.Fatal("stopped decoder critical stream close code", context.Cause(connection.Context()))
		}
	case <-peer.ctx.Done():
		t.Fatal("peer-stopped decoder stream left connection alive")
	}
	select {
	case got := <-result:
		if got.err == nil {
			if got.response != nil {
				_ = got.response.Body.Close()
			}
			t.Fatal("stopped decoder stream produced success")
		}
	case <-peer.ctx.Done():
		t.Fatal("critical stream failure retained response waiter")
	}
}
