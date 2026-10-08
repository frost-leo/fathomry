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
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/g"
	qpackdecoder "github.com/enetx/http3/internal/qpack"
	"github.com/quic-go/quic-go"
)

func reviewQueueEmpty(t *testing.T, state *qpackConnection) {
	t.Helper()
	ctx := reviewFeedbackContext(t)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		state.mu.Lock()
		count := len(state.queue)
		state.mu.Unlock()
		if count == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("writer did not dequeue feedback before reaching peer credit bound")
		case <-ticker.C:
		}
	}
}

func TestFathomryQPACKFeedbackBackpressureBoundAndShutdown(t *testing.T) {
	for _, control := range []string{"reader-resumes", "queue-one-over", "owner-shutdown"} {
		t.Run(control, func(t *testing.T) {
			client, peer := reviewFeedbackPair(t, &quic.Config{MaxIncomingUniStreams: 8, InitialStreamReceiveWindow: 70, MaxStreamReceiveWindow: 70, InitialConnectionReceiveWindow: 4096, MaxConnectionReceiveWindow: 4096})
			state := newQPACKConnection(client, 64, 0, 512)
			reviewFeedbackSeed(t, state)
			stopped := make(chan struct{})
			go func() { defer close(stopped); state.writeFeedback() }()
			t.Cleanup(func() {
				_ = client.CloseWithError(0, "")
				state.stop(nil)
				select {
				case <-stopped:
				case <-time.After(time.Second):
					t.Error("flow-controlled decoder writer did not stop")
				}
			})
			_, reader := reviewFeedbackReader(t, peer)
			var want []byte
			for index := 0; index < 300; index++ {
				stream := uint64(1<<61) + uint64(4*index)
				encoded := reviewFeedbackInteger(7, 0x80, stream)
				if len(encoded) != 10 {
					t.Fatal("fixture requires exact ten-byte feedback")
				}
				want = append(want, encoded...)
				if err := state.enqueue(qpackFeedback{stream: stream, required: 1}); err != nil {
					t.Fatal(err)
				}
			}
			stream := uint64(1<<61) + 4*300
			ctx := reviewFeedbackContext(t)
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			var queued int
			for {
				state.mu.Lock()
				queued = len(state.queue)
				state.mu.Unlock()
				if queued <= 154 {
					break
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal("writer did not consume enough records to fill native send buffer")
				}
			}
			// The selected QUIC writer can buffer 1452 bytes beyond at most71
			// bytes of peer credit. It cannot complete153 ten-byte records.
			if queued < 147 {
				t.Fatal("fixture did not retain feedback beyond actual wire/buffer credit", queued)
			}
			switch control {
			case "reader-resumes":
				got := make([]byte, len(want))
				if _, err := io.ReadFull(reader, got); err != nil || !bytes.Equal(got, want) {
					t.Fatal("feedback fragmented or reordered after peer credit resumed", err)
				}
				reviewQueueEmpty(t, state)
			case "queue-one-over":
				refused, admitted := false, 0
				for index := 1; index <= 2*state.limit; index++ {
					state.mu.Lock()
					queued, backing := len(state.queue), cap(state.queue)
					state.mu.Unlock()
					if queued > state.limit || backing > 2*state.limit {
						t.Fatal("feedback backing outside declared queue envelope", queued, backing)
					}
					err := state.enqueue(qpackFeedback{stream: stream + uint64(4*index), cancel: true})
					if errors.Is(err, qpackdecoder.ErrLimit) {
						refused = true
						break
					}
					if err != nil {
						t.Fatal("in-bound feedback refused for another reason", err)
					}
					admitted++
				}
				if !refused || admitted < state.limit-queued {
					t.Fatal("feedback did not preserve exact admission bound under delayed credit", refused, admitted)
				}
				select {
				case <-client.Context().Done():
				case <-reviewFeedbackContext(t).Done():
					t.Fatal("feedback saturation did not terminate unusable connection")
				}
			case "owner-shutdown":
				if err := client.CloseWithError(0, ""); err != nil {
					t.Fatal(err)
				}
				select {
				case <-stopped:
				case <-reviewFeedbackContext(t).Done():
					t.Fatal("source shutdown did not interrupt partial feedback Write")
				}
				state.stop(nil)
				state.mu.Lock()
				problem, queued := state.err, len(state.queue)
				state.mu.Unlock()
				if problem != nil || queued != 0 {
					t.Fatal("normal close fabricated decoder protocol failure or retained queue", problem, queued)
				}
			}
		})
	}
}

func TestFathomryQPACKClientQuotaWaitsForActualWorkerJoin(t *testing.T) {
	client, peer := reviewFeedbackPair(t)
	var settings g.MapOrd[uint64, uint64]
	settings.Insert(1, 64)
	settings.Insert(7, 1)
	owned := newClientConn(client, false, settings, 1024, false, nil, &FathomryQPACKLimits{MaxTableCapacity: 64, MaxBlockedStreams: 1, MaxFeedbackRecords: 16})
	entered, finishing, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	t.Cleanup(func() { unblock(); _ = client.CloseWithError(0, ""); _ = owned.FathomryWait() })
	original := owned.rawConn.qpackEncoderHandler
	owned.rawConn.qpackEncoderHandler = func(stream *quic.ReceiveStream) {
		close(entered)
		original(stream)
		close(finishing)
		<-release
	}
	owned.startUnidirectional()
	encoder, err := peer.OpenUniStreamSync(reviewFeedbackContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write([]byte{2, 0x3f}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-reviewFeedbackContext(t).Done():
		t.Fatal("encoder worker did not start")
	}
	ready := make(chan struct{})
	close(ready)
	var releases atomic.Int64
	roundTrip := &roundTripperWithCount{conn: client, clientConn: owned, dialing: ready, cancel: func() {}, release: func() { releases.Add(1) }}
	closed := make(chan error, 1)
	go func() { closed <- roundTrip.Close() }()
	select {
	case <-finishing:
	case <-reviewFeedbackContext(t).Done():
		t.Fatal("native parser did not leave connection shutdown")
	}
	select {
	case <-client.Context().Done():
	case <-reviewFeedbackContext(t).Done():
		t.Fatal("fixture did not establish QUIC shutdown before held worker cleanup")
	}
	if releases.Load() != 0 {
		t.Fatal("H3 client quota released on context cancellation, before worker join")
	}
	select {
	case err := <-closed:
		t.Fatal("Close reported completion before actual worker cleanup", err)
	default:
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil || releases.Load() != 1 || !roundTrip.closed.Load() {
			t.Fatal("actual worker join did not release exactly once", err, releases.Load())
		}
	case <-reviewFeedbackContext(t).Done():
		t.Fatal("worker release did not complete native Close")
	}
}

func TestFathomryQPACKFeedbackExactRecordLimit(t *testing.T) {
	client, _ := reviewFeedbackPair(t)
	state := newQPACKConnection(client, 64, 0, 2)
	t.Cleanup(func() { state.stop(nil) })
	for index := 0; index < 2; index++ {
		if err := state.enqueue(qpackFeedback{stream: uint64(4 * index), cancel: true}); err != nil {
			t.Fatal("exact feedback limit refused", index, err)
		}
	}
	state.mu.Lock()
	queued, closed := len(state.queue), state.closed
	state.mu.Unlock()
	if queued != 2 || closed {
		t.Fatal("exact limit did not retain precisely two records", queued, closed)
	}
	if err := state.enqueue(qpackFeedback{stream: 8, cancel: true}); !errors.Is(err, qpackdecoder.ErrLimit) {
		t.Fatal("third feedback record escaped two-record declaration", err)
	}
	state.mu.Lock()
	queued, closed = len(state.queue), state.closed
	state.mu.Unlock()
	if queued != 0 || !closed {
		t.Fatal("saturated feedback state did not close and clear", queued, closed)
	}
}
