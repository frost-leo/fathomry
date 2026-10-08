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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	official "github.com/quic-go/quic-go"
	wire "github.com/quic-go/quic-go/quicvarint"
	nativehttp "github.com/sardanioss/http"
	nativequic "github.com/sardanioss/quic-go"
	"github.com/sardanioss/quic-go/internal/protocol"
	utls "github.com/sardanioss/utls"
)

type qpackPeer struct {
	ctx            context.Context
	client         *ClientConn
	peer           *official.Conn
	encoder        *official.SendStream
	feedback       *official.ReceiveStream
	requestEncoder *official.ReceiveStream
	control        *official.ReceiveStream
}

func newQPACKPeer(t *testing.T, capacity, blocked uint64, receiveWindow ...uint64) *qpackPeer {
	t.Helper()
	return newQPACKPeerSetup(t, capacity, blocked, receiveWindow, false, nil)
}

func newQPACKPeerSetup(t *testing.T, capacity, blocked uint64, receiveWindow []uint64, datagrams bool, prepare func(*ClientConn)) *qpackPeer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	certificate := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificates := certificate.TLS.Certificates
	roots := x509.NewCertPool()
	roots.AddCert(certificate.Certificate())
	certificate.Close()
	config := &official.Config{MaxIdleTimeout: 3 * time.Second, EnableDatagrams: datagrams}
	if len(receiveWindow) == 1 {
		config.InitialStreamReceiveWindow = receiveWindow[0]
		config.MaxStreamReceiveWindow = receiveWindow[0]
	}
	listener, err := official.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: certificates, NextProtos: []string{"h3"}}, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan *official.Conn, 1)
	acceptErrors := make(chan error, 1)
	go func() {
		connection, err := listener.Accept(ctx)
		if err != nil {
			acceptErrors <- err
			return
		}
		accepted <- connection
	}()
	connection, err := nativequic.DialAddr(ctx, listener.Addr().String(), &utls.Config{RootCAs: roots, NextProtos: []string{"h3"}},
		&nativequic.Config{MaxIdleTimeout: 3 * time.Second, EnableDatagrams: datagrams})
	if err != nil {
		t.Fatal(err)
	}
	var peer *official.Conn
	select {
	case peer = <-accepted:
	case err := <-acceptErrors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	transport := &Transport{EnableDatagrams: datagrams, AdditionalSettings: map[uint64]uint64{1: capacity, 7: blocked}}
	if datagrams {
		transport.AdditionalSettings[settingDatagram] = 1
	}
	client := newClientConn(connection, transport.EnableDatagrams, transport.AdditionalSettings, transport.MaxResponseHeaderBytes, transport.DisableCompression, transport.SendGreaseFrames, transport.Logger)
	if prepare != nil {
		prepare(client)
	}
	client.acceptControlStreams()
	fixture := &qpackPeer{ctx: ctx, client: client, peer: peer}
	t.Cleanup(func() {
		_ = connection.CloseWithError(0, "")
		if err := client.FathomryCloseSenders(); err != nil {
			t.Error(err)
		}
		_ = peer.CloseWithError(0, "")
		client.controlMu.Lock()
		active := client.controlActive
		client.controlMu.Unlock()
		if active != 0 || client.decoder.BlockedStreams() != 0 {
			t.Error("native control work remained after joined close")
		}
	})
	control, err := peer.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fixture.encoder, err = peer.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.encoder.Write([]byte{2}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		stream, err := peer.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		kind, err := wire.Read(wire.NewReader(stream))
		if err != nil {
			t.Fatal(err)
		}
		if kind == 3 {
			fixture.feedback = stream
		}
		if kind == 2 {
			fixture.requestEncoder = stream
		}
		if kind == 0 {
			fixture.control = stream
		}
	}
	if fixture.feedback == nil {
		t.Fatal("client decoder stream missing")
	}
	settings := []byte{0, 4, 0}
	if datagrams {
		settings = []byte{0, 4, 2, 0x33, 1}
	}
	if _, err := control.Write(settings); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestFathomryDatagramReaderAdmission(t *testing.T) {
	for _, saturated := range []bool{false, true} {
		t.Run(fmt.Sprint(saturated), func(t *testing.T) {
			var scheduled atomic.Int64
			fixture := newQPACKPeerSetup(t, 64, 1, nil, true, func(client *ClientConn) {
				if saturated {
					client.rawConn.schedule = func(func()) bool { scheduled.Add(1); return false }
				}
			})
			if saturated {
				select {
				case <-fixture.peer.Context().Done():
					var application *official.ApplicationError
					if scheduled.Load() != 1 || !errors.As(context.Cause(fixture.peer.Context()), &application) || application.ErrorCode != official.ApplicationErrorCode(ErrCodeExcessiveLoad) {
						t.Fatalf("missing explicit reader admission failure: %v", context.Cause(fixture.peer.Context()))
					}
				case <-fixture.ctx.Done():
					t.Fatal("negotiated datagrams had no admitted receiver")
				}
				return
			}
			stream, _ := fixture.request(t, fixture.ctx)
			select {
			case <-fixture.client.ReceivedSettings():
			case <-fixture.ctx.Done():
				t.Fatal("settings absent")
			}
			payload := append(wire.Append(nil, uint64(stream.StreamID()/4)), []byte("received")...)
			if err := fixture.peer.SendDatagram(payload); err != nil {
				t.Fatal(err)
			}
			data, err := stream.ReceiveDatagram(fixture.ctx)
			if err != nil || string(data) != "received" {
				t.Fatalf("below-cap datagram path: %q %v", data, err)
			}
			stream.CancelRead(nativequic.StreamErrorCode(ErrCodeRequestCanceled))
		})
	}
}

func (fixture *qpackPeer) request(t *testing.T, ctx context.Context) (*RequestStream, *official.Stream) {
	t.Helper()
	stream, err := fixture.client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendRequestHeader(&nativehttp.Request{Method: "GET",
		URL: &url.URL{Scheme: "https", Host: fixture.peer.LocalAddr().String(), Path: "/"}, Header: make(nativehttp.Header)}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	incoming, err := fixture.peer.AcceptStream(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(incoming); err != nil {
		t.Fatal(err)
	}
	return stream, incoming
}

func peerHeaders(t *testing.T, stream *official.Stream, block []byte) {
	t.Helper()
	data := wire.Append(nil, 1)
	data = wire.Append(data, uint64(len(block)))
	data = append(data, block...)
	if _, err := stream.Write(data); err != nil {
		t.Fatal(err)
	}
}

func awaitQPACK(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	for !ready() {
		if ctx.Err() != nil {
			t.Fatal("native state did not reach expected boundary", context.Cause(ctx))
		}
		runtime.Gosched()
	}
}

func TestFathomryQPACKIndependentFragmentedPeer(t *testing.T) {
	fixture := newQPACKPeer(t, 64, 1)
	stream, peer := fixture.request(t, fixture.ctx)
	peerHeaders(t, peer, []byte{2, 0, 0xd9, 0x80})
	result := make(chan error, 1)
	go func() {
		response, err := stream.ReadResponse()
		if err == nil && (response.StatusCode != 200 || response.Header.Get("X") != "v") {
			err = errors.New("dynamic response field was lost")
		}
		if err == nil {
			err = response.Body.Close()
		}
		result <- err
	}()
	awaitQPACK(t, fixture.ctx, func() bool { return fixture.client.decoder.BlockedStreams() == 1 })
	if _, err := fixture.encoder.Write([]byte{0x3f}); err != nil {
		t.Fatal(err)
	}
	awaitQPACK(t, fixture.ctx, func() bool { return fixture.client.decoder.PendingBytes() == 1 })
	if _, err := fixture.encoder.Write([]byte{0x21, 0x41}); err != nil {
		t.Fatal(err)
	}
	awaitQPACK(t, fixture.ctx, func() bool { return fixture.client.decoder.PendingBytes() == 1 })
	if _, err := fixture.encoder.Write([]byte{'x', 1, 'v'}); err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-fixture.ctx.Done():
		t.Fatal(context.Cause(fixture.ctx))
	}
	var feedback [2]byte
	if _, err := io.ReadFull(fixture.feedback, feedback[:]); err != nil {
		t.Fatal(err)
	}
	if feedback != [2]byte{1, 0x80} {
		t.Fatalf("wire feedback = %x, want increment then section ack", feedback)
	}
}

func TestFathomryQPACKBlockedCancellationKeepsSibling(t *testing.T) {
	fixture := newQPACKPeer(t, 64, 1)
	ctx, cancel := context.WithCancelCause(fixture.ctx)
	stream, peer := fixture.request(t, ctx)
	peerHeaders(t, peer, []byte{2, 0, 0xd9, 0x80})
	returned := make(chan error, 1)
	go func() { _, err := stream.ReadResponse(); returned <- err }()
	awaitQPACK(t, fixture.ctx, func() bool { return fixture.client.decoder.BlockedStreams() == 1 })
	cause := errors.New("request stopped")
	cancel(cause)
	select {
	case err := <-returned:
		if !errors.Is(err, cause) {
			t.Fatalf("request cause missing: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked decoder did not cancel")
	}
	if fixture.client.conn.Context().Err() != nil {
		t.Fatal("request cancellation killed sibling connection")
	}
	sibling, otherPeer := fixture.request(t, fixture.ctx)
	peerHeaders(t, otherPeer, []byte{0, 0, 0xd9})
	if err := otherPeer.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := sibling.ReadResponse()
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("normal static sibling: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFathomryQPACKAdvertisedCapacityAndCriticalClosure(t *testing.T) {
	for _, test := range []struct {
		name   string
		action func(*qpackPeer) error
		code   ErrCode
	}{
		{"over_capacity", func(peer *qpackPeer) error { _, err := peer.encoder.Write([]byte{0x3f, 0x22}); return err }, ErrCodeQPACKEncoderStreamError},
		{"encoder_eof", func(peer *qpackPeer) error { return peer.encoder.Close() }, ErrCodeClosedCriticalStream},
		{"idle_decoder_stop", func(peer *qpackPeer) error {
			peer.feedback.CancelRead(official.StreamErrorCode(ErrCodeRequestCanceled))
			return nil
		}, ErrCodeClosedCriticalStream},
		{"idle_control_stop", func(peer *qpackPeer) error { peer.control.CancelRead(71); return nil }, ErrCodeClosedCriticalStream},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newQPACKPeer(t, 64, 1)
			if err := test.action(fixture); err != nil {
				t.Fatal(err)
			}
			select {
			case <-fixture.peer.Context().Done():
				var application *official.ApplicationError
				if !errors.As(context.Cause(fixture.peer.Context()), &application) || application.ErrorCode != official.ApplicationErrorCode(test.code) {
					t.Fatalf("peer terminal error = %v, want %#x", context.Cause(fixture.peer.Context()), test.code)
				}
			case <-fixture.ctx.Done():
				t.Fatal("critical protocol failure left connection open")
			}
		})
	}
}

func TestFathomryQPACKReadAbortAndDeadlineKeepSibling(t *testing.T) {
	for _, mode := range []string{"local", "deadline", "remote"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newQPACKPeer(t, 64, 1)
			stream, peer := fixture.request(t, fixture.ctx)
			peerHeaders(t, peer, []byte{2, 0, 0xd9, 0x80})
			returned := make(chan error, 1)
			go func() { _, err := stream.ReadResponse(); returned <- err }()
			awaitQPACK(t, fixture.ctx, func() bool { return fixture.client.decoder.BlockedStreams() == 1 })
			switch mode {
			case "local":
				stream.CancelRead(71)
			case "deadline":
				if err := stream.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
					t.Fatal(err)
				}
			case "remote":
				peer.CancelWrite(71)
			}
			select {
			case err := <-returned:
				if err == nil {
					t.Fatal("aborted decode succeeded")
				}
				if mode == "deadline" && !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatalf("deadline cause missing: %v", err)
				}
				if mode != "deadline" {
					var reset *nativequic.StreamError
					if !errors.As(err, &reset) || reset.ErrorCode != 71 {
						t.Fatalf("receive abort cause missing: %v", err)
					}
				}
			case <-time.After(time.Second):
				t.Fatal("read abort did not wake QPACK without ending parent")
			}
			if fixture.ctx.Err() != nil || fixture.client.conn.Context().Err() != nil {
				t.Fatal("read abort ended parent or sibling connection")
			}
			sibling, otherPeer := fixture.request(t, fixture.ctx)
			peerHeaders(t, otherPeer, []byte{0, 0, 0xd9})
			_ = otherPeer.Close()
			response, err := sibling.ReadResponse()
			if err != nil || response.StatusCode != 200 {
				t.Fatalf("healthy sibling failed: %v", err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFathomryQPACKAbandonedTrailersCancelReferences(t *testing.T) {
	fixture := newQPACKPeer(t, 64, 1)
	if _, err := fixture.encoder.Write([]byte{0x3f, 0x21, 0x41, 'x', 1, 'v'}); err != nil {
		t.Fatal(err)
	}
	awaitQPACK(t, fixture.ctx, func() bool { return fixture.client.decoder.InsertCount() == 1 })
	stream, peer := fixture.request(t, fixture.ctx)
	peerHeaders(t, peer, []byte{0, 0, 0xd9})
	peerHeaders(t, peer, []byte{2, 0, 0x80})
	response, err := stream.ReadResponse()
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	var feedback [2]byte
	if _, err := io.ReadFull(fixture.feedback, feedback[:]); err != nil {
		t.Fatal(err)
	}
	if feedback != [2]byte{1, 0x40} {
		t.Fatalf("abandoned trailer feedback=%x, want increment+cancellation", feedback)
	}
	stream.CancelRead(71)
	_ = fixture.feedback.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	var extra [1]byte
	if count, err := fixture.feedback.Read(extra[:]); count != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("duplicate cancellation or ack-after-cancel: %x %v", extra[:count], err)
	}
	if fixture.client.conn.Context().Err() != nil {
		t.Fatal("abandonment killed connection")
	}
}

func TestFathomryQPACKCriticalStopDuringFeedbackBackpressure(t *testing.T) {
	for _, critical := range []string{"encoder", "decoder", "control"} {
		t.Run(critical, func(t *testing.T) {
			fixture := newQPACKPeer(t, 64, 1, 16)
			if _, err := fixture.encoder.Write([]byte{0x3f, 0x21}); err != nil {
				t.Fatal(err)
			}
			// Include the native pending STREAM frame, not just peer wire credit.
			for count := uint64(1); count <= 16+uint64(protocol.MaxPacketBufferSize)+8; count++ {
				if _, err := fixture.encoder.Write([]byte{0x41, 'x', 1, 'v'}); err != nil {
					t.Fatal(err)
				}
				awaitQPACK(t, fixture.ctx, func() bool { return fixture.client.decoder.InsertCount() == count })
			}
			awaitQPACK(t, fixture.ctx, func() bool { return len(fixture.client.qpackFeedback) >= 8 })
			switch critical {
			case "encoder":
				fixture.requestEncoder.CancelRead(71)
			case "decoder":
				fixture.feedback.CancelRead(71)
			case "control":
				fixture.control.CancelRead(71)
			}
			select {
			case <-fixture.peer.Context().Done():
				var application *official.ApplicationError
				if !errors.As(context.Cause(fixture.peer.Context()), &application) || application.ErrorCode != official.ApplicationErrorCode(ErrCodeClosedCriticalStream) {
					t.Fatalf("backpressured critical stop: %v", context.Cause(fixture.peer.Context()))
				}
			case <-time.After(time.Second):
				t.Fatal("blocked feedback hid critical stream STOP_SENDING")
			}
		})
	}
}

func TestFathomryQPACKWatcherCapacityRefusesStream(t *testing.T) {
	fixture := newQPACKPeer(t, 64, 1)
	fixture.client.controlMu.Lock()
	added := 1032 - fixture.client.controlActive
	fixture.client.controlActive += added
	fixture.client.controlMu.Unlock()
	stream, err := fixture.client.OpenRequestStream(fixture.ctx)
	fixture.client.controlMu.Lock()
	fixture.client.controlActive -= added
	fixture.client.controlMu.Unlock()
	if stream != nil || err == nil {
		t.Fatal("successful request stream had no admitted read watcher")
	}
	rejected, err := fixture.peer.AcceptStream(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rejected)
	var reset *official.StreamError
	if len(data) != 0 || !errors.As(err, &reset) || reset.ErrorCode != official.StreamErrorCode(ErrCodeExcessiveLoad) {
		t.Fatalf("refused stream was not reset without HTTP bytes: %x %v", data, err)
	}
	sibling, peer := fixture.request(t, fixture.ctx)
	peerHeaders(t, peer, []byte{0, 0, 0xd9})
	_ = peer.Close()
	response, err := sibling.ReadResponse()
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("capacity refusal broke healthy sibling: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
