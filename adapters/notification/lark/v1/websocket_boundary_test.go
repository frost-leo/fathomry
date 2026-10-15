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

package lark

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestRegressionWebSocketByteOnlySaturation(t *testing.T) {
	for _, mode := range []string{"event", "ack"} {
		t.Run(mode, func(t *testing.T) {
			peer := newWSPeer(t)
			owner, runtime, evidence := testOwner(t, peer.settings(), NativeOptions{})
			events := publicEvents(t, 64)
			if mode == "event" {
				var err error
				events, err = adapters.NewInbox[Incoming](adapters.EvidenceOptions{Capacity: 64, MaxBytes: owner.state.metadata.EventBytes + publicMetadataBytes - 1})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				options, _ := evidence.Options()
				usage, _ := evidence.Inspect()
				endpoint, err := adapters.Bind(runtime, adapters.Declaration[Result]{Evidence: evidence, Copy: func(value Result) Result { return value }})
				if err != nil {
					t.Fatal(err)
				}
				_, err = endpoint.Run(context.Background(), adapters.Request{Operation: "review.fill", WorkBytes: 1,
					EvidenceBytes: options.MaxBytes - usage.Bytes - owner.state.policy.Budget.EvidenceBytes},
					func(call *adapters.Call[Result]) { _ = call.Resolve(adapters.Outcome[Result]{}) })
				if err != nil {
					t.Fatal(err)
				}
			}
			cancel, done := startListen(owner.Client(), events)
			defer cancel()
			conn := nextConnection(t, peer)
			sendEvent(t, conn, "byte-only-saturation")
			select {
			case result := <-done:
				if !errors.Is(result.err, adapters.ErrEvidence) {
					t.Fatalf("missing byte-saturation cause: %v", result.err)
				}
				status, _ := events.Inspect()
				if status.Outstanding != 0 || len(result.result.UnforwardedEvents()) != 1 {
					t.Fatalf("unsafe event handoff: events=%d unforwarded=%d", status.Outstanding, len(result.result.UnforwardedEvents()))
				}
				if mode == "ack" {
					if len(result.result.UnforwardedAcknowledgements()) != 1 {
						t.Fatal("missing fallback ACK evidence")
					}
					ack, ok := result.result.UnforwardedAcknowledgements()[0].Acknowledgement()
					if !ok || ack.FrameID() != "byte-only-saturation" || ack.Effect() != WebSocketNotWritten {
						t.Fatal("fallback ACK effect or frame identity lost")
					}
					event := result.result.UnforwardedEvents()[0]
					ackResult := result.result.UnforwardedAcknowledgements()[0]
					if event.AcknowledgementID() == "" || event.AcknowledgementID() != ackResult.TechnicalID() || event.FrameID() != ack.FrameID() || event.ConnectionGeneration() != ack.ConnectionGeneration() || event.Attribution().Source != ackResult.Attribution().Source {
						t.Fatal("fallback event/ACK association lost")
					}
					usage, _ := evidence.Inspect()
					options, _ := evidence.Options()
					if usage.Outstanding >= options.Capacity {
						t.Fatal("test also saturated record count")
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("byte-only saturation failed to join")
			}
			noPositiveACK(t, peer)
		})
	}
}

func TestRegressionWebSocketACKWithEveryRootOccupied(t *testing.T) {
	peer := newWSPeer(t)
	owner, runtime, _ := testOwner(t, peer.settings(), NativeOptions{})
	events := publicEvents(t, 8)
	cancel, done := startListen(owner.Client(), events)
	defer cancel()
	conn := nextConnection(t, peer)
	reader := &heldReader{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(reader.release) })
	defer release()
	finiteCtx, stopFinite := context.WithCancel(context.Background())
	defer stopFinite()
	finiteDone := make(chan error, 1)
	go func() {
		_, err := owner.Client().UploadAsset(finiteCtx, false, AssetInput{Name: "fixture", FileType: "stream", Reader: reader}, RequestOptions{})
		finiteDone <- err
	}()
	select {
	case <-reader.entered:
	case <-time.After(time.Second):
		t.Fatal("finite reader did not enter")
	}
	stats, _ := runtime.Inspect()
	options, _ := runtime.Options()
	if stats.Active != options.MaxActive {
		t.Fatalf("root saturation not established: active=%d limit=%d", stats.Active, options.MaxActive)
	}
	sendEvent(t, conn, "no-root-permit")
	event, delivery := nextIncoming(t, events)
	_ = delivery.Ack()
	receipt, err := event.Acknowledge(context.Background(), JSON{})
	if err != nil {
		t.Fatal(err)
	}
	frame, code := peerACK(t, peer)
	if code != 200 || frame.headers["message_id"] != "no-root-permit" {
		t.Fatal("ACK needed a root permit or lost identity")
	}
	wait, stopWait := context.WithTimeout(context.Background(), time.Second)
	defer stopWait()
	snapshot, err := receipt.WaitReleased(wait)
	if err != nil || snapshot.Err() != nil {
		t.Fatal("ACK did not release while roots stayed occupied", err, snapshot.Err())
	}
	stopFinite()
	release()
	select {
	case err := <-finiteDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("finite cancellation lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("finite reader failed to join")
	}
	finishListen(t, cancel, done)
}

func TestRegressionWebSocketCredentialsOutliveCloseWait(t *testing.T) {
	peer := newWSPeer(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	defer release()
	owner, runtime, _ := testOwner(t, peer.settings(), NativeOptions{Credentials: func(ctx context.Context, purpose CredentialRequest) (Credentials, error) {
		close(entered)
		<-proceed
		return Credentials{}, ctx.Err()
	}})
	cancel, done := startListen(owner.Client(), publicEvents(t, 4))
	defer cancel()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("credential callback did not enter")
	}
	for count := 0; count < 3; count++ {
		wait, stop := context.WithTimeout(context.Background(), time.Millisecond)
		err := owner.Close(wait)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
			t.Fatal("Close confused canceled waiting with actual callback release", err)
		}
	}
	stats, _ := runtime.Inspect()
	if stats.Active != 2 {
		t.Fatalf("source/listener reservations released early: %d", stats.Active)
	}
	release()
	finishListen(t, cancel, done)
	for count := 0; count < 3; count++ {
		wait, stop := context.WithTimeout(context.Background(), time.Second)
		err := owner.Close(wait)
		stop()
		if err != nil || !owner.ShutdownComplete() {
			t.Fatal("later Close could not confirm actual join", err)
		}
	}
}

func TestRegressionWebSocketConfiguredLifetimeAllowsBackground(t *testing.T) {
	for _, cancellable := range []bool{false, true} {
		name := "background"
		if cancellable {
			name = "cancellable-control"
		}
		t.Run(name, func(t *testing.T) {
			peer := newWSPeer(t)
			settings := peer.settings()
			settings.WebSocket.Lifetime = 30 * time.Millisecond
			owner, _, _ := testOwner(t, settings, NativeOptions{})
			ctx := context.Background()
			if cancellable {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			}
			started := time.Now()
			_, err := owner.Client().Listen(ctx, publicEvents(t, 4))
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("configured finite lifetime must permit this context, elapsed=%v error=%v", time.Since(started), err)
			}
		})
	}
}

func TestRegressionWebSocketConfiguredLifetimeCoversPublicAdmission(t *testing.T) {
	peer := newWSPeer(t)
	settings := peer.settings()
	settings.WebSocket.Lifetime = 30 * time.Millisecond
	owner, runtime, evidence := testOwner(t, settings, NativeOptions{})
	endpoint, err := adapters.Bind(runtime, adapters.Declaration[Result]{Evidence: evidence, Copy: func(value Result) Result { return value }})
	if err != nil {
		t.Fatal(err)
	}
	proceed := make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	defer release()
	options, _ := runtime.Options()
	for count := 1; count < options.MaxActive; count++ {
		entered := make(chan struct{})
		_, err := endpoint.Start(context.Background(), adapters.Request{Operation: "review.occupy", WorkBytes: 1, EvidenceBytes: 1},
			func(call *adapters.Call[Result]) {
				close(entered)
				<-proceed
				_ = call.Resolve(adapters.Outcome[Result]{})
			})
		if err != nil {
			t.Fatal(err)
		}
		<-entered
	}
	started := time.Now()
	cancel, done := startListen(owner.Client(), publicEvents(t, 4))
	defer cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatal("configured lifetime did not bound public admission", result.err)
		}
	case <-time.After(150 * time.Millisecond):
		stats, _ := runtime.Inspect()
		t.Errorf("configured 30ms lifetime did not include public queue: elapsed=%v queued=%d", time.Since(started), stats.Queued)
		finishListen(t, cancel, done)
	}
}

func TestRegressionWebSocketFallbackRetainsTransportIdentity(t *testing.T) {
	peer := newWSPeer(t)
	owner, _, _ := testOwner(t, peer.settings(), NativeOptions{})
	events, err := adapters.NewInbox[Incoming](adapters.EvidenceOptions{Capacity: 64, MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := startListen(owner.Client(), events)
	defer cancel()
	conn := nextConnection(t, peer)
	sendEvent(t, conn, "transport-only-frame-id")
	select {
	case result := <-done:
		fallback := result.result.UnforwardedEvents()
		if len(fallback) != 1 {
			t.Fatal("missing fallback event")
		}
		routing, ok := any(fallback[0]).(interface {
			FrameID() string
			ConnectionGeneration() uint64
		})
		if !ok {
			t.Fatalf("fallback type %T erased transport frame and connection identity", fallback[0])
		}
		if routing.FrameID() != "transport-only-frame-id" || routing.ConnectionGeneration() != 1 {
			t.Fatal("fallback transport identity was retargeted")
		}
	case <-time.After(time.Second):
		t.Fatal("saturated listener failed to join")
	}
}
