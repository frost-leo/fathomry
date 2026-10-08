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
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	official "github.com/quic-go/quic-go"
	wire "github.com/quic-go/quic-go/quicvarint"
	nativehttp "github.com/sardanioss/http"
	nativequic "github.com/sardanioss/quic-go"
)

func openDatagramSendStream(t *testing.T, fixture *qpackPeer) (*RequestStream, *official.Stream) {
	t.Helper()
	stream, err := fixture.client.OpenRequestStream(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stream.CancelRead(nativequic.StreamErrorCode(ErrCodeRequestCanceled))
		stream.CancelWrite(nativequic.StreamErrorCode(ErrCodeRequestCanceled))
	})
	if err := stream.SendRequestHeader(&nativehttp.Request{Method: "CONNECT", Proto: "connect-udp",
		URL: &url.URL{Scheme: "https", Host: fixture.peer.LocalAddr().String(), Path: "/"}, Header: make(nativehttp.Header)}); err != nil {
		t.Fatal(err)
	}
	peer, err := fixture.peer.AcceptStream(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader := wire.NewReader(peer)
	kind, err := wire.Read(reader)
	if err != nil || kind != 1 {
		t.Fatal("missing request headers", kind, err)
	}
	length, err := wire.Read(reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(io.Discard, peer, int64(length)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fixture.client.ReceivedSettings():
	case <-fixture.ctx.Done():
		t.Fatal("missing datagram settings")
	}
	return stream, peer
}

func TestFathomryDatagramSendJoinsNativeSendLifetime(t *testing.T) {
	for _, mode := range []string{"local-cancel", "local-close", "peer-stop", "caller-cancel", "receive-fin"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newQPACKPeerSetup(t, 64, 1, nil, true, nil)
			stream, peer := openDatagramSendStream(t, fixture)
			tracked := stream.str.datagramStream.(*stateTrackingStream)
			send := tracked.sendDatagramContext
			entered, writable, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var accepted atomic.Int32
			var enqueueContext context.Context
			tracked.sendDatagramContext = func(ctx context.Context, data []byte) error {
				enqueueContext = ctx
				close(entered)
				select {
				case <-ctx.Done():
					return context.Cause(ctx)
				case <-writable:
					if err := ctx.Err(); err != nil {
						return context.Cause(ctx)
					}
					if err := send(ctx, data); err != nil {
						return err
					}
					accepted.Add(1)
					return nil
				}
			}
			ctx, cancel := context.WithCancelCause(fixture.ctx)
			returned := make(chan error, 1)
			go func() { defer close(joined); returned <- stream.SendDatagramContext(ctx, []byte("blocked")) }()
			t.Cleanup(func() {
				cancel(context.Canceled)
				select {
				case <-joined:
				case <-fixture.ctx.Done():
					t.Error("entered datagram send was not reclaimed")
				}
			})
			select {
			case <-entered:
			case <-fixture.ctx.Done():
				t.Fatal("send never entered the blocked enqueue boundary")
			}
			cause := errors.New("only this caller ended")
			switch mode {
			case "local-cancel":
				stream.CancelWrite(0x120)
			case "local-close":
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			case "peer-stop":
				peer.CancelRead(0x121)
			case "caller-cancel":
				cancel(cause)
			case "receive-fin":
				if err := peer.Close(); err != nil {
					t.Fatal(err)
				}
				var data [1]byte
				if _, err := tracked.Read(data[:]); !errors.Is(err, io.EOF) {
					t.Fatal("peer did not finish only the receive half", err)
				}
				if stream.Context().Err() != nil {
					t.Fatal("receive FIN canceled the native send half")
				}
				close(writable)
			}
			select {
			case err := <-returned:
				if mode == "receive-fin" {
					if err != nil || accepted.Load() != 1 {
						t.Fatal("receive FIN incorrectly canceled datagram send", err, accepted.Load())
					}
					observed, err := fixture.peer.ReceiveDatagram(fixture.ctx)
					want := append(wire.Append(nil, uint64(stream.StreamID()/4)), []byte("blocked")...)
					if err != nil || !bytes.Equal(observed, want) {
						t.Fatal("accepted datagram absent on independent peer", observed, err)
					}
				} else {
					want := context.Cause(stream.Context())
					if mode == "caller-cancel" {
						want = cause
					} else if ctx.Err() != nil {
						t.Fatal("stream termination depended on caller cancellation")
					}
					if want == nil || !errors.Is(err, want) || accepted.Load() != 0 {
						t.Fatal("send did not retain native/caller cancellation", err, want, accepted.Load())
					}
					close(writable)
				}
			case <-time.After(time.Second):
				t.Fatal("entered datagram send ignored stream termination")
			}
			<-joined
			if enqueueContext.Err() == nil {
				t.Fatal("per-send cancellation authority survived its returned operation")
			}
			if mode != "receive-fin" && accepted.Load() != 0 {
				t.Fatal("failed send accepted data after returning its error")
			}
			sibling, _ := openDatagramSendStream(t, fixture)
			healthy, stop := context.WithCancel(fixture.ctx)
			if err := sibling.SendDatagramContext(healthy, []byte("sibling")); err != nil {
				t.Fatal("stream termination closed its healthy sibling", err)
			}
			stop()
			observed, err := fixture.peer.ReceiveDatagram(fixture.ctx)
			want := append(wire.Append(nil, uint64(sibling.StreamID()/4)), []byte("sibling")...)
			if err != nil || !bytes.Equal(observed, want) {
				t.Fatal("later caller cancellation erased accepted sibling packet", observed, err)
			}
		})
	}
}
