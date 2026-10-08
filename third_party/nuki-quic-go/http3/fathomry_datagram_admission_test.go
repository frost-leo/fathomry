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
	"testing"
	"time"

	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/quicvarint"
)

func TestFathomryDatagramReceiverAdmission(t *testing.T) {
	for _, admitted := range []bool{true, false} {
		name := "refused"
		if admitted {
			name = "admitted"
		}
		t.Run(name, func(t *testing.T) {
			local, peer := newConnPair(t, withDatagrams())
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			controlHandled := false
			conn := newRawConn(local, true, func() {}, func(*quic.ReceiveStream, *frameParser) {
				controlHandled = true
			}, nil, nil)
			workerDone := make(chan struct{})
			scheduled := 0
			conn.schedule = func(work func()) bool {
				scheduled++
				if !admitted {
					return false
				}
				go func() { defer close(workerDone); work() }()
				return true
			}
			stream, err := local.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			tracked := conn.TrackStream(stream)
			defer tracked.CancelRead(quic.StreamErrorCode(ErrCodeRequestCanceled))
			defer tracked.CancelWrite(quic.StreamErrorCode(ErrCodeRequestCanceled))
			control, err := peer.OpenUniStream()
			if err != nil {
				t.Fatal(err)
			}
			wire := (&settingsFrame{Datagram: true}).Append(quicvarint.Append(nil, streamTypeControlStream))
			if _, err := control.Write(wire); err != nil {
				t.Fatal(err)
			}
			received, err := local.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			handled := make(chan struct{})
			go func() { defer close(handled); conn.handleUnidirectionalStream(received, false) }()
			select {
			case <-handled:
			case <-ctx.Done():
				t.Fatal("control handling did not return", context.Cause(ctx))
			}
			if scheduled != 1 || controlHandled != admitted {
				t.Fatal("datagram admission did not gate control continuation", scheduled, controlHandled)
			}
			if !admitted {
				select {
				case <-peer.Context().Done():
					want := &quic.ApplicationError{Remote: true, ErrorCode: quic.ApplicationErrorCode(ErrCodeExcessiveLoad)}
					if !errors.Is(context.Cause(peer.Context()), want) {
						t.Fatal("refused receiver did not fail closed", context.Cause(peer.Context()))
					}
				case <-ctx.Done():
					t.Fatal("refused receiver left the QUIC connection live")
				}
			} else {
				payload := []byte("received-with-owned-worker")
				wire := append(quicvarint.Append(nil, uint64(stream.StreamID()/4)), payload...)
				if err := peer.SendDatagram(wire); err != nil {
					t.Fatal(err)
				}
				actual, err := tracked.ReceiveDatagram(ctx)
				if err != nil || !bytes.Equal(actual, payload) {
					t.Fatal("admitted receiver lost datagram", actual, err)
				}
				_ = local.CloseWithError(0, "")
				select {
				case <-workerDone:
				case <-ctx.Done():
					t.Fatal("owned datagram receiver did not terminate")
				}
			}
			tracked.CancelRead(quic.StreamErrorCode(ErrCodeRequestCanceled))
			tracked.CancelWrite(quic.StreamErrorCode(ErrCodeRequestCanceled))
			joined := make(chan struct{})
			go func() { defer close(joined); conn.qloggerWG.Wait() }()
			select {
			case <-joined:
			case <-ctx.Done():
				t.Fatal("datagram admission leaked qlog work")
			}
		})
	}
}
