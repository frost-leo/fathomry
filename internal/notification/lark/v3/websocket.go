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
	"math"
	"net"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/gorilla/websocket"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

// WebSocketStats is a value snapshot. Counters describe local transport/evidence
// facts, not durable processing. Pending counts uncompleted acknowledgements.
type WebSocketStats struct {
	private
	Running, Connected                                                                       bool
	Generation, ConnectAttempts, Connections, Frames, BytesReceived, Delivered, Pings, Pongs uint64
	AckWritten, AckUnknown, Rejected, Abandoned, FragmentsDiscarded                          uint64
	Pending                                                                                  int
	LastHandshakeHTTP                                                                        int
}

// WebSocketWrite distinguishes local write completion from remote acceptance.
type WebSocketWrite uint8

const (
	WebSocketNotWritten WebSocketWrite = iota
	WebSocketWriteUnknown
	WebSocketWritten
)

// WebSocketAck is immutable per-frame acknowledgement evidence. Written means a
// successful local WebSocket write, NOT a server confirmation or durable commit.
type WebSocketAck struct {
	private
	frameID    string
	generation uint64
	code       int
	effect     WebSocketWrite
}

func (value WebSocketAck) FrameID() string        { return value.frameID }
func (value WebSocketAck) Generation() uint64     { return value.generation }
func (value WebSocketAck) Code() int              { return value.code }
func (value WebSocketAck) Effect() WebSocketWrite { return value.effect }

// WebSocketResult holds a final session summary or one immutable ACK outcome.
// LastFailure preserves the last bounded recoverable failure, even after recovery.
type WebSocketResult struct {
	private
	stats       WebSocketStats
	ack         *WebSocketAck
	bootstrap   Exchange
	lastFailure error
}

func (value WebSocketResult) Stats() WebSocketStats { return value.stats }
func (value WebSocketResult) Acknowledgement() (WebSocketAck, bool) {
	if value.ack == nil {
		return WebSocketAck{}, false
	}
	return *value.ack, true
}
func (value WebSocketResult) Bootstrap() Exchange { return value.bootstrap }
func (value WebSocketResult) LastFailure() error  { return value.lastFailure }

// Receiver is a non-owning application capability. Listen owns its runtime until
// return. Caller cancellation stops only that operation, never the named source.
type Receiver struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[WebSocketResult]
	observer *invocation.Observer
}

// BindReceiver composes WebSocket capability over the SAME named source and
// limits. Its result inbox retains the session plus per-event ACK evidence;
// callers must drain it concurrently, retaining unresolved records separately.
func BindReceiver(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[WebSocketResult], observer *invocation.Observer) (*Receiver, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind-receiver")
	}
	value, limits := source.owner.settings, access.Limits()
	if !value.WebSocket.Enabled || value.Profile != "application" {
		return nil, failure(ErrUnsupported, "websocket-profile")
	}
	if limits.Active > value.MaxActive || limits.Bytes < value.reservation() || limits.MaxLeases < value.maxLeases() || limits.Queued > 32 ||
		limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "websocket-limits")
	}
	return &Receiver{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// Status returns a coherent numeric source snapshot, shared by borrowed receivers.
func (receiver *Receiver) Status() WebSocketStats {
	if receiver == nil || receiver.owner == nil {
		return WebSocketStats{}
	}
	receiver.owner.socketMu.Lock()
	defer receiver.owner.socketMu.Unlock()
	return receiver.owner.socketStatus
}

type socketRun struct {
	receiver         *Receiver
	ctx              context.Context
	call             *invocation.Call[WebSocketResult]
	events           *invocation.Inbox[WebSocketEvent]
	id               fault.Correlation
	options          socketSettings
	stats            WebSocketStats
	bootstrap        Exchange
	lastFailure      error
	cleanup          error
	sequence         uint64
	pending          map[*socketPending]bool
	acknowledgements chan *socketPending
	mu               sync.Mutex
	active           bool
	generation       uint64
}

func (run *socketRun) publish() {
	run.stats.Pending = len(run.pending)
	run.receiver.owner.socketMu.Lock()
	run.receiver.owner.socketStatus = run.stats
	run.receiver.owner.socketMu.Unlock()
}

// Listen blocks through bootstrap, receive, heartbeat and bounded reconnect.
// It starts no application callback. Events carry explicit Acknowledge/Reject
// capabilities; silence expires to a negative ACK, never implicit success.
// Cancel the supplied lifetime context before closing the assembly. Return means
// sockets, timers and the single read worker have actually stopped.
func (receiver *Receiver) Listen(ctx context.Context, id fault.Correlation, events *invocation.Inbox[WebSocketEvent]) (*invocation.Receipt[WebSocketResult], error) {
	if receiver == nil || receiver.owner == nil || ctx == nil || events == nil {
		return nil, failure(ErrInput, "websocket-listen")
	}
	value := receiver.owner.settings
	work, cancel, err := (invocation.Budget{Limit: value.WebSocket.Lifetime}).Context(ctx, invocation.Lifetime)
	if err != nil {
		return nil, err
	}
	defer cancel()
	call, err := invocation.Begin(work, receiver.access, invocation.Request{Name: "websocket-listen", Correlation: id, Shape: invocation.Session,
		Bytes: value.reservation(), EvidenceBytes: socketEvidenceBytes, Admission: invocation.Budget{Limit: value.Timeout},
		AttemptsKnown: true, MaxAttempts: math.MaxUint64}, receiver.inbox, receiver.observer)
	if err != nil {
		return nil, err
	}
	select {
	case receiver.owner.socketGate <- struct{}{}:
		defer func() { <-receiver.owner.socketGate }()
	default:
		call.Complete(invocation.Outcome[WebSocketResult]{Present: true, Primary: failure(ErrLimit, "websocket-active")})
		return call.Receipt(), nil
	}
	run := &socketRun{receiver: receiver, ctx: work, call: call, events: events, id: id, options: value.WebSocket, pending: map[*socketPending]bool{},
		acknowledgements: make(chan *socketPending, value.WebSocket.MaxPending), stats: WebSocketStats{Running: true}}
	run.publish()
	err = run.loop()
	run.stats.Running = false
	run.stats.Connected = false
	run.publish()
	var primary error
	if err != nil {
		primary = failure(ErrWebSocket, "listen", err, work.Err(), context.Cause(work))
	}
	call.Complete(invocation.Outcome[WebSocketResult]{Present: true, Value: WebSocketResult{stats: run.stats, bootstrap: run.bootstrap, lastFailure: run.lastFailure}, Primary: primary, Cleanup: run.cleanup})
	return call.Receipt(), nil
}
func (run *socketRun) loop() error {
	delay := run.options.ReconnectMin
	limit := run.options.MaxConnectAttempts
	for attempt := 0; attempt < limit; attempt++ {
		if err := run.ctx.Err(); err != nil {
			return err
		}
		run.stats.ConnectAttempts++
		run.publish()
		conn, configuration, err := run.connect()
		if err == nil {
			if configuration.connectLimit > 0 {
				limit = min(limit, configuration.connectLimit)
			}
			run.stats.Connections++
			run.stats.Generation++
			run.stats.Connected = true
			run.mu.Lock()
			run.generation = run.stats.Generation
			run.active = true
			run.mu.Unlock()
			run.publish()
			err = run.serve(conn, &configuration)
			if configuration.connectLimit > 0 {
				limit = min(limit, configuration.connectLimit)
			}
			run.stats.Connected = false
			run.publish()
		}
		if err == nil {
			return nil
		}
		run.lastFailure = failure(ErrWebSocket, "websocket-connection", err)
		if run.ctx.Err() != nil {
			return run.ctx.Err()
		}
		if !socketRetryable(err) {
			return err
		}
		if attempt+1 >= limit {
			return failure(ErrLimit, "websocket-reconnect", err)
		}
		delay = reconnectDelay(delay, configuration.reconnectFloor, run.options)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-run.ctx.Done():
			timer.Stop()
			return run.ctx.Err()
		}
	}
	return failure(ErrLimit, "websocket-reconnect")
}

type socketRead struct {
	data []byte
	err  error
}

func (run *socketRun) serve(conn *websocket.Conn, configuration *socketConfiguration) (resultErr error) {
	reads := make(chan socketRead, 1)
	done := make(chan struct{})
	work, cancel := context.WithCancel(run.ctx)
	go func() {
		defer close(done)
		for {
			kind, data, err := conn.ReadMessage()
			if err == nil && kind != websocket.BinaryMessage {
				err = failure(ErrProtocol, "websocket-message-type")
			}
			select {
			case reads <- socketRead{data, err}:
			case <-work.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		run.mu.Lock()
		run.active = false
		run.mu.Unlock()
		cancel()
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			run.cleanup = errors.Join(run.cleanup, failure(ErrCleanup, "websocket-close", err))
		}
		<-done
		run.abandonPending(failure(ErrAcknowledgement, "websocket-generation-ended", resultErr, run.ctx.Err(), context.Cause(run.ctx)))
	}()
	fragments := fragmentStore{options: run.options, items: map[string]*fragmentedMessage{}}
	pingInterval := configuration.pingInterval
	if pingInterval == 0 {
		pingInterval = run.options.PingInterval
	}
	if err := run.ping(conn, configuration.service); err != nil {
		return err
	}
	awaitingPong := true
	nextPing := time.Now().Add(pingInterval)
	pongDeadline := time.Now().Add(run.options.PongTimeout)
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	for {
		now := time.Now()
		if err := work.Err(); err != nil {
			return err
		}
		if awaitingPong && !now.Before(pongDeadline) {
			return failure(ErrWebSocket, "websocket-pong-timeout")
		}
		if expired := fragments.expire(now); expired > 0 {
			run.stats.FragmentsDiscarded += uint64(expired)
			return failure(ErrProtocol, "websocket-fragment-expired")
		}
		for pending := range run.pending {
			if !now.Before(pending.deadline) {
				if err := run.writeAcknowledgement(conn, pending, true); err != nil {
					return err
				}
			}
		}
		if !awaitingPong && !time.Now().Before(nextPing) {
			if err := run.ping(conn, configuration.service); err != nil {
				return err
			}
			awaitingPong = true
			pongDeadline = time.Now().Add(run.options.PongTimeout)
		}
		deadline := nextPing
		if awaitingPong {
			deadline = pongDeadline
		}
		for pending := range run.pending {
			if pending.deadline.Before(deadline) {
				deadline = pending.deadline
			}
		}
		if fragmentDeadline := fragments.deadline(); !fragmentDeadline.IsZero() && fragmentDeadline.Before(deadline) {
			deadline = fragmentDeadline
		}
		timer.Reset(max(0, time.Until(deadline)))
		select {
		case <-work.Done():
			return work.Err()
		case read := <-reads:
			if read.err != nil {
				if errors.Is(read.err, websocket.ErrReadLimit) {
					return failure(ErrLimit, "websocket-frame", read.err)
				}
				return read.err
			}
			run.stats.Frames++
			run.stats.BytesReceived += uint64(len(read.data))
			frame, headers, err := decodeSocketFrame(read.data)
			if err != nil {
				return err
			}
			if frame.Method == int32(larkws.FrameTypeControl) {
				if headers[larkws.HeaderType] != "pong" {
					return failure(ErrProtocol, "websocket-control")
				}
				conf, err := parseSocketConfiguration(frame.Payload, run.options)
				if err != nil {
					return err
				}
				if conf.pingInterval > 0 {
					pingInterval = conf.pingInterval
				}
				if conf.connectLimit > 0 {
					configuration.connectLimit = conf.connectLimit
				}
				if conf.reconnectFloor > 0 {
					configuration.reconnectFloor = conf.reconnectFloor
				}
				awaitingPong = false
				nextPing = time.Now().Add(pingInterval)
				run.stats.Pongs++
			} else {
				payload, complete, err := fragments.add(frame, headers, time.Now())
				if err != nil {
					return err
				}
				if complete {
					if err := run.deliver(frame, headers, payload); err != nil {
						return err
					}
				}
			}
			run.publish()
		case pending := <-run.acknowledgements:
			if _, ok := run.pending[pending]; !ok {
				continue
			}
			if err := run.writeAcknowledgement(conn, pending, false); err != nil {
				return err
			}
		case <-timer.C:
		}
		timer.Stop()
	}
}
func (run *socketRun) ping(conn *websocket.Conn, service int32) error {
	data, err := larkws.NewPingFrame(service).Marshal()
	if err != nil {
		return failure(ErrProtocol, "websocket-ping", err)
	}
	if _, err = run.call.Attempt(); err != nil {
		return err
	}
	if err = conn.SetWriteDeadline(time.Now().Add(run.options.WriteTimeout)); err != nil {
		return err
	}
	if err = conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		return err
	}
	run.stats.Pings++
	run.publish()
	return nil
}
func socketRetryable(err error) bool {
	return !errors.Is(err, ErrInput) && !errors.Is(err, ErrProtocol) && !errors.Is(err, ErrUnsupported) && !errors.Is(err, ErrLimit) && !errors.Is(err, ErrCleanup) && !errors.Is(err, ErrResponse) &&
		!errors.Is(err, invocation.ErrEvidence) && !errors.Is(err, ErrAuth) && !errors.Is(err, context.Canceled)
}
