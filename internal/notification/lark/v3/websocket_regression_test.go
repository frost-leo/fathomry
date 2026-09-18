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

package lark

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

func floodPartialFrames(t *testing.T, socket *peerSocket) func() {
	t.Helper()
	frame, _ := fragmentedFrame("deadline-partial", 2, 0, []byte("incomplete"))
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if socket.send(frame) != nil {
				return
			}
		}
	}()
	return func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("flood worker did not stop")
		}
	}
}

func TestWebSocketRegressionFragmentExpiryUnderTrafficWithHealthyPongs(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	options := socketTestOptions(peer)
	options.WebSocket.PongTimeout = time.Second
	options.WebSocket.FragmentTimeout = 30 * time.Millisecond
	options.WebSocket.MaxConnectAttempts = 1
	fixture := bindReceiverTest(t, options, 8, 2)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	stopFlood := floodPartialFrames(t, socket)
	defer stopFlood()
	select {
	case completed := <-done:
		result := awaitSocketResult(t, completed.receipt)
		stats := result.Outcome.Value.Stats()
		if !errors.Is(result.Err(), ErrProtocol) || stats.FragmentsDiscarded != 1 || stats.Pongs == 0 || stats.Frames < 10 {
			t.Errorf("fragment expiry not independently enforced: err=%v discarded=%d pongs=%d frames=%d", result.Err(), stats.FragmentsDiscarded, stats.Pongs, stats.Frames)
		}
	case <-time.After(300 * time.Millisecond):
		stopReceiver(t, cancel, done)
		t.Error("healthy pongs/duplicate traffic postponed fragment expiry")
	}
	drainSocketResults(t, fixture)
}

func TestWebSocketRegressionAckExpiryAndHeartbeatUnderTraffic(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	options := socketTestOptions(peer)
	options.WebSocket.PingInterval = 5 * time.Millisecond
	options.WebSocket.PongTimeout = time.Second
	options.WebSocket.FragmentTimeout = time.Second
	options.WebSocket.AckTimeout = 30 * time.Millisecond
	fixture := bindReceiverTest(t, options, 8, 2)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	if err := socket.send(eventFrame("ack-expiry", notificationEvent())); err != nil {
		t.Fatal(err)
	}
	event := nextEvent(t, fixture)
	stopFlood := floodPartialFrames(t, socket)
	defer stopFlood()
	ctx, end := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer end()
	result, err := event.Receipt().WaitReleased(ctx)
	if err != nil {
		stopReceiver(t, cancel, done)
		t.Fatalf("ACK expiry was postponed by duplicate traffic: %v", err)
	}
	ack, present := result.Outcome.Value.Acknowledgement()
	if !present || ack.Code() != 500 || ack.Effect() != WebSocketWritten || !errors.Is(result.Err(), ErrAcknowledgement) {
		t.Errorf("expiry did not write explicit rejection: present=%v code=%d effect=%d err=%v", present, ack.Code(), ack.Effect(), result.Err())
	}
	_, wireACK := socketACK(t, peer)
	if wireACK.StatusCode != 500 {
		t.Errorf("wire ACK code=%d", wireACK.StatusCode)
	}
	stats := fixture.receiver.Status()
	if !stats.Connected || stats.Pings < 2 || stats.Frames < 10 {
		t.Errorf("heartbeat/healthy session did not progress: connected=%v pings=%d frames=%d", stats.Connected, stats.Pings, stats.Frames)
	}
	stopReceiver(t, cancel, done)
	drainSocketResults(t, fixture)
}

func TestWebSocketRegressionFrameTrafficCannotPostponeDeadlines(t *testing.T) {
	peer := newSocketPeer(t, true, nil)
	options := socketTestOptions(peer)
	options.WebSocket.PongTimeout = 20 * time.Millisecond
	options.WebSocket.FragmentTimeout = 30 * time.Millisecond
	options.WebSocket.MaxConnectAttempts = 1
	fixture := bindReceiverTest(t, options, 16, 4)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	frame, _ := fragmentedFrame("partial", 2, 0, []byte("incomplete"))
	stopSending := make(chan struct{})
	senderDone := make(chan struct{})
	go func() {
		defer close(senderDone)
		for {
			select {
			case <-stopSending:
				return
			default:
			}
			if socket.send(frame) != nil {
				return
			}
		}
	}()
	select {
	case finished := <-done:
		result := awaitSocketResult(t, finished.receipt)
		t.Logf("terminated at intended deadline: frames=%d pongs=%d", result.Outcome.Value.Stats().Frames, result.Outcome.Value.Stats().Pongs)
	case <-time.After(200 * time.Millisecond):
		status := fixture.receiver.Status()
		t.Errorf("missed 20 ms pong and 30 ms fragment deadline after 200 ms of traffic: connected=%v frames=%d pongs=%d", status.Connected, status.Frames, status.Pongs)
		stopReceiver(t, cancel, done)
	}
	close(stopSending)
	select {
	case <-senderDone:
	case <-time.After(time.Second):
		t.Error("sender did not terminate")
	}
	drainSocketResults(t, fixture)
}

func TestWebSocketRegressionLifetimeCoversAdmissionWait(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	options := socketTestOptions(peer)
	options.QueuedCalls = 1
	options.Timeout = 300 * time.Millisecond
	options.WebSocket.Lifetime = 20 * time.Millisecond
	fixture := bindReceiverTest(t, options, 4, 2)
	lease, err := fixture.receiver.access.Acquire(context.Background(), fixture.receiver.owner.settings.reservation())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	started := time.Now()
	_, err = fixture.receiver.Listen(context.Background(), fault.Correlation{Call: "queued-review"}, fixture.events)
	elapsed := time.Since(started)
	if elapsed > 150*time.Millisecond {
		t.Errorf("20 ms lifetime ignored during admission: elapsed=%v error=%v", elapsed, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) || fixture.results.Usage().Outstanding != 0 || peer.bootstraps.Load() != 0 {
		t.Fatal("lifetime expiry lost its cause, leaked evidence, or began networking")
	}
}

func TestWebSocketRegressionRetainedRoutingFitsAllReservations(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	options := socketTestOptions(peer)
	options.MaxRequestBytes = 1024
	options.MaxAssetBytes = 1024
	options.MaxResponseBytes = 1024
	options.WebSocket.MaxFrameBytes = 1 << 20
	options.WebSocket.MaxMessageBytes = 1024
	options.WebSocket.MaxFragmentBytes = 1024
	options.WebSocket.MaxPending = 64
	fixture := bindReceiverTest(t, options, 65, 64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call, err := invocation.Begin(ctx, fixture.receiver.access, invocation.Request{
		Name: "websocket-listen", Correlation: fault.Correlation{Call: "listener"}, Shape: invocation.Session,
		Bytes: fixture.receiver.owner.settings.reservation(), EvidenceBytes: socketEvidenceBytes,
		Admission: invocation.Budget{Limit: time.Second}, AttemptsKnown: true, MaxAttempts: 1,
	}, fixture.results, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := &socketRun{receiver: fixture.receiver, ctx: ctx, call: call, events: fixture.events,
		id: fault.Correlation{Call: "listener"}, options: fixture.receiver.owner.settings.WebSocket,
		pending: map[*socketPending]bool{}, acknowledgements: make(chan *socketPending, 64), stats: WebSocketStats{Generation: 1}}
	defer func() {
		run.abandonPending(context.Canceled)
		call.Complete(invocation.Outcome[WebSocketResult]{})
		drainSocketResults(t, fixture)
		for fixture.events.Usage().Outstanding > 0 {
			_ = nextEvent(t, fixture)
		}
	}()
	frame := eventFrame("routing", notificationEvent())
	frame.LogIDNew = strings.Repeat("r", (1<<20)-1024)
	encoded, err := frame.Marshal()
	if err != nil || len(encoded) > options.WebSocket.MaxFrameBytes {
		t.Fatalf("bad fixture: %v bytes=%d", err, len(encoded))
	}
	if _, _, err := decodeSocketFrame(encoded); !errors.Is(err, ErrLimit) {
		t.Fatal("oversized routing metadata was accepted")
	}
	frame.LogIDNew = strings.Repeat("r", socketRoutingLimit/2)
	encoded, err = frame.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	for range options.WebSocket.MaxPending {
		decoded, headers, err := decodeSocketFrame(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if err := run.deliver(decoded, headers, decoded.Payload); err != nil {
			t.Fatal(err)
		}
	}
	var retained int64
	for pending := range run.pending {
		retained += int64(len(pending.frame.LogIDNew))
	}
	reserved := fixture.receiver.owner.settings.reservation() + fixture.results.Usage().ReservedBytes + fixture.events.Usage().ReservedBytes
	if retained > reserved {
		t.Errorf("retained LogIDNew bytes alone exceed root+ACK+event declared memory: retained=%d reserved=%d pending=%d", retained, reserved, len(run.pending))
	}
}

func TestWebSocketRegressionConfigurationValidatesEffectiveInterval(t *testing.T) {
	options := socketDefaults(&WebSocketOptions{}, "https://open.feishu.cn")
	configuration, err := parseSocketConfiguration([]byte(`{"PingInterval":1,"pinginterval":86400}`), options)
	if err == nil && configuration.pingInterval > 5*time.Minute {
		t.Errorf("case-folded native field bypassed 300-second validation: accepted interval=%v", configuration.pingInterval)
	}
}

func TestWebSocketRoutingMetadataAggregateBound(t *testing.T) {
	frame := eventFrame("routing", notificationEvent())
	frame.LogIDNew = strings.Repeat("r", socketRoutingLimit)
	encoded, err := frame.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeSocketFrame(encoded); !errors.Is(err, ErrLimit) {
		t.Fatal("routing protobuf overhead was not counted")
	}
	frame.LogIDNew = "valid-log-id"
	for _, name := range []string{"extra_one", "extra_two", "extra_three", "extra_four"} {
		frame.Headers = append(frame.Headers, larkws.Header{Key: name, Value: strings.Repeat("x", 2048)})
	}
	encoded, _ = frame.Marshal()
	if _, _, err := decodeSocketFrame(encoded); !errors.Is(err, ErrLimit) {
		t.Fatal("aggregate header storage escaped the routing limit")
	}
}

func TestWebSocketConfigurationRejectsAllCaseAliases(t *testing.T) {
	options := socketDefaults(&WebSocketOptions{}, "https://open.feishu.cn")
	for _, data := range []string{
		`{"pinginterval":120}`,
		`{"ReconnectInterval":1,"reconnectinterval":86400}`,
		`{"ReconnectCount":0,"reconnectcount":-1}`,
		`{"ReconnectNonce":0,"reconnectnonce":999999999}`,
	} {
		if _, err := parseSocketConfiguration([]byte(data), options); err == nil {
			t.Fatal("a case-folded configuration alias was accepted")
		}
	}
}
