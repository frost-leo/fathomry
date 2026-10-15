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
	"strconv"
	"sync"

	"github.com/frost-leo/fathomry/adapters/notification/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

// Incoming contains immutable authenticated data and an original-connection,
// one-decision ACK capability. Copies share the same decision. Inbox.Ack/Retry
// only transfers local evidence; neither invokes these platform operations.
type Incoming struct {
	private
	native      native.WebSocketEvent
	receipt     *adapters.Receipt[Result]
	attribution notification.Attribution
	source      notification.Info
}

func (value Incoming) Event() Event                          { return Event{native: value.native.Event()} }
func (value Incoming) FrameID() string                       { return value.native.FrameID() }
func (value Incoming) ConnectionGeneration() uint64          { return value.native.Generation() }
func (value Incoming) Attribution() notification.Attribution { return value.attribution }
func (value Incoming) Source() notification.Info             { return value.source.Clone() }
func (value Incoming) Receipt() *adapters.Receipt[Result] {
	if value.receipt == nil {
		return nil
	}
	copy := *value.receipt
	return &copy
}
func (value Incoming) Acknowledge(ctx context.Context, response JSON) (*adapters.Receipt[Result], error) {
	if value.receipt == nil {
		return nil, fail(ErrState, "ack")
	}
	_, err := value.native.Acknowledge(ctx, response.native)
	return value.Receipt(), translate(err, "ack")
}
func (value Incoming) Reject(ctx context.Context) (*adapters.Receipt[Result], error) {
	if value.receipt == nil {
		return nil, fail(ErrState, "ack")
	}
	_, err := value.native.Reject(ctx)
	return value.Receipt(), translate(err, "ack")
}

// Listen owns one finite-lifetime receiver until actual native/bridge join.
// A positive policy lifetime starts before admission and permits Background;
// otherwise ctx must be cancellable. Cancel ctx or close the source to stop it.
// Event and ACK records use separate
// public inboxes. Drain Result with NextReleased so the live listener/source
// cannot block finite requests. Saturation stops reception, never positive ACKs.
func (client *Client) Listen(ctx context.Context, events *adapters.Inbox[Incoming]) (Result, error) {
	if client == nil || client.lifetime == nil || ctx == nil || events == nil {
		return Result{}, fail(ErrInput, "listen")
	}
	if client.receiverLifetime > 0 {
		bounded, cancel := context.WithTimeout(ctx, client.receiverLifetime)
		defer cancel()
		ctx = bounded
	} else if ctx.Done() == nil {
		return Result{}, fail(ErrInput, "listen-lifetime")
	}
	eventEndpoint, err := adapters.Bind(client.runtime, adapters.Declaration[Incoming]{Evidence: events, Observer: client.observer, Copy: func(value Incoming) Incoming { return value }})
	if err != nil {
		return Result{}, err
	}
	live, stop := joinContexts(ctx, client.lifetime)
	defer stop()
	admission, stopAdmission := context.WithTimeout(live, client.budget.AdmissionTimeout)
	defer stopAdmission()
	run := func(call *adapters.Call[Result], handle Handle) {
		state := handle.state
		reject := func(err error) { _ = call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "listen")}) }
		if state == nil {
			reject(fail(ErrState, "listen"))
			return
		}
		if !state.metadata.WebSocket {
			reject(fail(ErrUnsupported, "listen"))
			return
		}
		if limit := state.metadata.ReceiverLifetime; limit > 0 && (client.receiverLifetime <= 0 || client.receiverLifetime > limit) {
			reject(fail(ErrLimit, "listen-lifetime"))
			return
		}
		if state.policy.Budget.WorkBytes > client.budget.WorkBytes || state.policy.Budget.EvidenceBytes > client.budget.EvidenceBytes || client.budget.AdmissionTimeout > state.timeout {
			reject(fail(ErrLimit, "listen"))
			return
		}
		release, err := state.use()
		if err != nil {
			reject(err)
			return
		}
		defer release()
		work, stopOwner := joinContexts(call.Context(), state.call.Context())
		defer stopOwner()
		work, cancel := context.WithCancelCause(work)
		defer cancel(nil)
		serial, ok := state.next()
		if !ok {
			reject(fail(ErrLimit, "listen"))
			return
		}
		size := state.metadata.Pending + 2
		inbox, err := invocation.NewInbox[native.WebSocketResult](size, int64(size)*state.metadata.AckBytes)
		if err != nil {
			reject(err)
			return
		}
		incoming, err := invocation.NewInbox[native.WebSocketEvent](size, int64(size)*state.metadata.EventBytes)
		if err != nil {
			reject(err)
			return
		}
		receiver, err := native.BindReceiver(state.assembly, state.selection, inbox, nil)
		if err != nil {
			reject(err)
			return
		}
		bridge := socketBridge{client: client, call: call, events: eventEndpoint, state: state, cancel: cancel,
			ready: map[string]*adapters.Receipt[Result]{}, changed: make(chan struct{})}
		collect, stopCollect := context.WithCancel(context.Background())
		var collectors sync.WaitGroup
		collectors.Add(2)
		go func() { defer collectors.Done(); bridge.results(collect, inbox) }()
		go func() { defer collectors.Done(); bridge.incoming(collect, incoming) }()
		receipt, setup := receiver.Listen(work, fault.Correlation{Call: "lark-listen-" + strconv.FormatUint(serial, 10)}, incoming)
		stopCollect()
		collectors.Wait()
		bridge.wait.Wait()
		// No producer remains. Drain records published during collector cancellation;
		// their facts stay on the already-admitted listener rather than being dropped.
		for incoming.Usage().Outstanding > 0 {
			record, _ := incoming.Next(context.Background())
			result, _ := record.Receipt().WaitReleased(context.Background())
			if result.Outcome.Present {
				bridge.missingEvents = append(bridge.missingEvents, captureUnforwarded(result.Outcome.Value, call))
			}
			_ = record.Release()
		}
		if bridge.session != nil {
			_ = bridge.session.Release()
		}
		for inbox.Usage().Outstanding > 0 {
			record, _ := inbox.Next(context.Background())
			result, _ := record.Receipt().WaitReleased(context.Background())
			if result.Nested {
				bridge.missing = append(bridge.missing, result)
			}
			_ = record.Release()
		}
		snapshot, _ := call.Receipt().Snapshot()
		final := Result{source: info(state.info), attribution: attribution(snapshot.Info()), unforwarded: bridge.missing, unforwardedEvents: bridge.missingEvents}
		var cleanup error
		if receipt != nil {
			observed, waitErr := receipt.WaitReleased(context.Background())
			final = projectSocket(observed, snapshot.Info())
			final.unforwarded, final.unforwardedEvents = bridge.missing, bridge.missingEvents
			setup = errors.Join(setup, waitErr, observed.Outcome.Primary)
			cleanup = observed.Outcome.Cleanup
		}
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: final, Primary: translate(errors.Join(setup, bridge.failure), "listen"), Cleanup: translate(cleanup, "cleanup")})
	}
	req := request("listen", client.id, client.budget.WorkBytes, client.budget.EvidenceBytes)
	var receipt *adapters.Receipt[Result]
	if client.source != nil {
		receipt, err = adapters.UsingWithLifetime(admission, live, client.endpoint, *client.source, req, run)
	} else {
		receipt, err = client.endpoint.RunWithLifetime(admission, live, req, func(call *adapters.Call[Result]) { run(call, client.direct) })
	}
	if receipt == nil {
		return Result{}, translate(err, "listen")
	}
	snapshot, waitErr := receipt.WaitReleased(context.Background())
	value, _ := snapshot.ValueCopy()
	return value, errors.Join(err, waitErr, snapshot.Err())
}

type socketBridge struct {
	client        *Client
	call          *adapters.Call[Result]
	events        adapters.Endpoint[Incoming]
	state         *sourceState
	cancel        context.CancelCauseFunc
	mu            sync.Mutex
	ready         map[string]*adapters.Receipt[Result]
	changed       chan struct{}
	failure       error
	missing       []invocation.Result[native.WebSocketResult]
	missingEvents []UnforwardedEvent
	session       *invocation.DeliveryRecord[native.WebSocketResult]
	wait          sync.WaitGroup
}

func (bridge *socketBridge) stopWithError(err error) {
	bridge.mu.Lock()
	if bridge.failure == nil {
		bridge.failure = err
	}
	bridge.mu.Unlock()
	bridge.cancel(err)
}
func (bridge *socketBridge) results(ctx context.Context, inbox *invocation.Inbox[native.WebSocketResult]) {
	for {
		record, err := inbox.Next(ctx)
		if err != nil {
			return
		}
		observed, _ := record.Receipt().Result()
		if !observed.Nested {
			bridge.session = record
			continue
		}
		key := observed.Context.Correlation.Call
		var child *adapters.Call[Result]
		var guard adapters.Guard
		var holdErr error
		receipt, admitErr := bridge.client.endpoint.Child(bridge.call.Context(), bridge.call.Scope(),
			request("websocket-ack", bridge.client.id, 0, bridge.state.metadata.AckBytes+publicMetadataBytes),
			func(call *adapters.Call[Result]) {
				guard, holdErr = call.Hold()
				if holdErr != nil {
					_ = call.Resolve(adapters.Outcome[Result]{Primary: holdErr})
					return
				}
				child = call
			})
		if child == nil {
			bridge.stopWithError(errors.Join(admitErr, holdErr, fail(ErrState, "ack-admission")))
			receipt = nil
		}
		bridge.mu.Lock()
		bridge.ready[key] = receipt
		close(bridge.changed)
		bridge.changed = make(chan struct{})
		bridge.mu.Unlock()
		bridge.wait.Add(1)
		go func() {
			defer bridge.wait.Done()
			result, waitErr := record.Receipt().WaitReleased(context.Background())
			if child != nil {
				metadata, _ := child.Receipt().Snapshot()
				_ = child.Resolve(adapters.Outcome[Result]{Present: result.Outcome.Present, Value: projectSocket(result, metadata.Info()),
					Primary: translate(errors.Join(waitErr, result.Outcome.Primary), "ack"), Cleanup: translate(result.Outcome.Cleanup, "cleanup")})
				_ = record.Release()
				_ = guard.Release()
			} else {
				bridge.mu.Lock()
				bridge.missing = append(bridge.missing, result)
				bridge.mu.Unlock()
				_ = record.Release()
			}
		}()
	}
}
func (bridge *socketBridge) incoming(ctx context.Context, inbox *invocation.Inbox[native.WebSocketEvent]) {
	for {
		record, err := inbox.Next(ctx)
		if err != nil {
			return
		}
		result, waitErr := record.Receipt().WaitReleased(context.Background())
		event := result.Outcome.Value
		if waitErr != nil || !result.Outcome.Present {
			bridge.stopWithError(errors.Join(waitErr, result.Err(), fail(ErrState, "event")))
			_ = record.Release()
			continue
		}
		nativeAck, _ := event.Receipt().Result()
		key := nativeAck.Context.Correlation.Call
		var receipt *adapters.Receipt[Result]
		found := false
		for !found {
			bridge.mu.Lock()
			receipt, found = bridge.ready[key]
			changed := bridge.changed
			if found {
				delete(bridge.ready, key)
			}
			bridge.mu.Unlock()
			if !found {
				select {
				case <-changed:
				case <-ctx.Done():
					found = true
				}
			}
		}
		if receipt != nil {
			delivered := false
			_, err = bridge.events.Child(bridge.call.Context(), bridge.call.Scope(),
				request("websocket-event", bridge.client.id, 0, bridge.state.metadata.EventBytes+publicMetadataBytes),
				func(call *adapters.Call[Incoming]) {
					metadata, _ := call.Receipt().Snapshot()
					_ = call.Resolve(adapters.Outcome[Incoming]{Present: true, Value: Incoming{native: event, receipt: receipt, attribution: attribution(metadata.Info()), source: info(result.Source)}})
					delivered = true
				})
			if !delivered && err == nil {
				err = fail(ErrState, "event-dispatch")
			}
		} else {
			err = fail(ErrState, "event-ack-reservation")
		}
		if err != nil {
			bridge.mu.Lock()
			bridge.missingEvents = append(bridge.missingEvents, captureUnforwarded(event, bridge.call))
			bridge.mu.Unlock()
			bridge.stopWithError(err)
		}
		_ = record.Release()
	}
}

func projectSocket(value invocation.Result[native.WebSocketResult], metadata adapters.Info) Result {
	socket := value.Outcome.Value
	return Result{present: value.Outcome.Present, source: info(value.Source), attribution: attribution(metadata), technicalID: value.Context.Correlation.Call,
		attempts: notification.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}, socket: &socket, primary: translate(value.Outcome.Primary, "websocket"), cleanup: translate(value.Outcome.Cleanup, "cleanup")}
}

// WebSocketStats is a numeric local transport snapshot, not durable processing.
type WebSocketStats struct {
	private
	Running, Connected                                                                       bool
	Generation, ConnectAttempts, Connections, Frames, BytesReceived, Delivered, Pings, Pongs uint64
	AckWritten, AckUnknown, Rejected, Abandoned, FragmentsDiscarded                          uint64
	Pending, LastHandshakeHTTP                                                               int
}

func socketStats(value native.WebSocketStats) WebSocketStats {
	return WebSocketStats{Running: value.Running, Connected: value.Connected, Generation: value.Generation, ConnectAttempts: value.ConnectAttempts, Connections: value.Connections, Frames: value.Frames, BytesReceived: value.BytesReceived, Delivered: value.Delivered, Pings: value.Pings, Pongs: value.Pongs, AckWritten: value.AckWritten, AckUnknown: value.AckUnknown, Rejected: value.Rejected, Abandoned: value.Abandoned, FragmentsDiscarded: value.FragmentsDiscarded, Pending: value.Pending, LastHandshakeHTTP: value.LastHandshakeHTTP}
}
func (value Result) WebSocketStats() WebSocketStats {
	if value.socket == nil {
		return WebSocketStats{}
	}
	return socketStats(value.socket.Stats())
}
func (owner *Owner) WebSocketStatus() WebSocketStats { return owner.Handle().WebSocketStatus() }
func (handle Handle) WebSocketStatus() WebSocketStats {
	if handle.state == nil {
		return WebSocketStats{}
	}
	return socketStats(handle.state.inspection.WebSocketStatus())
}

// WebSocketWrite distinguishes no write, ambiguous write and locally completed
// write. Even Written is not a platform acknowledgement.
type WebSocketWrite uint8

const (
	WebSocketNotWritten WebSocketWrite = iota
	WebSocketWriteUnknown
	WebSocketWritten
)

type WebSocketAck struct {
	private
	native native.WebSocketAck
}

func (value WebSocketAck) FrameID() string              { return value.native.FrameID() }
func (value WebSocketAck) ConnectionGeneration() uint64 { return value.native.Generation() }
func (value WebSocketAck) Code() int                    { return value.native.Code() }
func (value WebSocketAck) Effect() WebSocketWrite       { return WebSocketWrite(value.native.Effect()) }
func (value Result) Acknowledgement() (WebSocketAck, bool) {
	if value.socket == nil {
		return WebSocketAck{}, false
	}
	ack, ok := value.socket.Acknowledgement()
	return WebSocketAck{native: ack}, ok
}
func (value Result) Bootstrap() Exchange {
	if value.socket == nil {
		return Exchange{}
	}
	return Exchange{native: value.socket.Bootstrap()}
}
func (value Result) LastFailure() error {
	if value.socket == nil {
		return nil
	}
	return translate(value.socket.LastFailure(), "websocket")
}
func (value Result) UnforwardedAcknowledgements() []Result {
	if value.unforwarded == nil {
		return nil
	}
	result := make([]Result, len(value.unforwarded))
	for index, item := range value.unforwarded {
		meta := value.attribution
		result[index] = projectSocket(item, adapters.Info{Runtime: meta.Runtime, Operation: meta.Operation, ID: meta.ID, Sequence: meta.Sequence, Parent: meta.Parent, Depth: meta.Depth, Source: meta.Source})
	}
	return result
}
func (value Result) UnforwardedEvents() []UnforwardedEvent {
	if value.unforwardedEvents == nil {
		return nil
	}
	return append([]UnforwardedEvent(nil), value.unforwardedEvents...)
}

// UnforwardedEvent retains transport/ACK association without granting a decision
// capability when public handoff failed. Payload Event.ID is not a frame identity.
type UnforwardedEvent struct {
	private
	event          Event
	frameID, ackID string
	generation     uint64
	attribution    notification.Attribution
}

func (value UnforwardedEvent) Event() Event                          { return value.event }
func (value UnforwardedEvent) FrameID() string                       { return value.frameID }
func (value UnforwardedEvent) ConnectionGeneration() uint64          { return value.generation }
func (value UnforwardedEvent) AcknowledgementID() string             { return value.ackID }
func (value UnforwardedEvent) Attribution() notification.Attribution { return value.attribution }
func captureUnforwarded(event native.WebSocketEvent, call *adapters.Call[Result]) UnforwardedEvent {
	ack, _ := event.Receipt().Result()
	meta, _ := call.Receipt().Snapshot()
	return UnforwardedEvent{event: Event{native: event.Event()}, frameID: event.FrameID(), generation: event.Generation(), ackID: ack.Context.Correlation.Call, attribution: attribution(meta.Info())}
}
