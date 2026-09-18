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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/gorilla/websocket"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

// WebSocketEvent contains immutable event data plus a one-shot ACK capability.
// Copies share that decision, not a new quota. The ACK result is independently
// reserved BEFORE event handoff. Releasing event evidence does not acknowledge it.
type WebSocketEvent struct {
	private
	event   Event
	pending *socketPending
}

func (value WebSocketEvent) Event() Event { return value.event }
func (value WebSocketEvent) FrameID() string {
	if value.pending == nil {
		return ""
	}
	return value.pending.frameID
}
func (value WebSocketEvent) Generation() uint64 {
	if value.pending == nil {
		return 0
	}
	return value.pending.generation
}

// Receipt observes the ACK independently, including timeout, disconnect or
// saturation. An unresolved receipt is not evidence of an ACK having been sent.
func (value WebSocketEvent) Receipt() *invocation.Receipt[WebSocketResult] {
	if value.pending == nil {
		return nil
	}
	return value.pending.call.Receipt()
}

// Acknowledge requests protocol code 200 after caller-owned processing or durable
// handoff. Optional reply is a bounded native card-action response object. Keep
// ctx live until the receipt is final; canceling a wait does not retract bytes.
func (value WebSocketEvent) Acknowledge(ctx context.Context, reply JSON) (*invocation.Receipt[WebSocketResult], error) {
	return value.submit(ctx, 200, reply)
}

// Reject requests code 500 without echoing business errors to the wire. Platform
// retry/delivery guarantees remain external; no exactly-once processing is claimed.
func (value WebSocketEvent) Reject(ctx context.Context) (*invocation.Receipt[WebSocketResult], error) {
	return value.submit(ctx, 500, JSON{})
}

type socketPending struct {
	run                *socketRun
	call               *invocation.Call[WebSocketResult]
	frame              larkws.Frame
	frameID            string
	generation         uint64
	deadline, received time.Time
	claimed, closed    bool
	code               int
	response           JSON
	ctx                context.Context
}

func (value WebSocketEvent) submit(ctx context.Context, code int, reply JSON) (*invocation.Receipt[WebSocketResult], error) {
	pending := value.pending
	if pending == nil || ctx == nil || len(reply.value) > socketReplyLimit || reply.value != "" && !object(reply) {
		return nil, failure(ErrInput, "websocket-ack")
	}
	if err := ctx.Err(); err != nil {
		return nil, failure(ErrAcknowledgement, "websocket-ack", err, context.Cause(ctx))
	}
	run := pending.run
	run.mu.Lock()
	defer run.mu.Unlock()
	if !run.active || pending.closed || pending.claimed || pending.generation != run.generation || !time.Now().Before(pending.deadline) || run.ctx.Err() != nil {
		return pending.call.Receipt(), failure(ErrAcknowledgement, "websocket-ack-state")
	}
	pending.claimed = true
	pending.code = code
	pending.response = reply
	pending.ctx = ctx
	select {
	case run.acknowledgements <- pending:
		return pending.call.Receipt(), nil
	default:
		pending.claimed = false
		pending.response = JSON{}
		pending.ctx = nil
		return pending.call.Receipt(), failure(ErrLimit, "websocket-ack-queue")
	}
}
func (run *socketRun) correlation(label string) (fault.Correlation, error) {
	if run.sequence == math.MaxUint64 {
		return fault.Correlation{}, failure(ErrLimit, "websocket-sequence")
	}
	run.sequence++
	digest := sha256.Sum256([]byte(run.id.Call + "\x00" + run.receiver.access.Info().Configuration.Revision))
	value := run.id
	value.Parent = run.id.Call
	value.Call = "ws-" + hex.EncodeToString(digest[:12]) + "-" + strconv.FormatUint(run.sequence, 10) + "-" + label
	return value, nil
}
func (run *socketRun) deliver(frame larkws.Frame, headers map[string]string, payload []byte) error {
	if len(run.pending) >= run.options.MaxPending {
		return failure(ErrLimit, "websocket-pending")
	}
	event, err := run.receiver.owner.settings.socketEvent(payload)
	if err != nil {
		run.stats.Rejected++
		return err
	}
	id, err := run.correlation("ack")
	if err != nil {
		return err
	}
	call, err := invocation.BeginNested(run.ctx, run.call.Scope(), invocation.Request{Name: "websocket-ack", Correlation: id, Shape: invocation.Finite,
		EvidenceBytes: socketEvidenceBytes, Admission: invocation.Budget{Limit: run.options.AckTimeout}, AttemptsKnown: true, MaxAttempts: 1}, run.receiver.inbox, run.receiver.observer)
	if err != nil {
		return err
	}
	frame.Payload = nil
	now := time.Now()
	pending := &socketPending{run: run, call: call, frame: frame, frameID: headers[larkws.HeaderMessageID], generation: run.stats.Generation, received: now, deadline: now.Add(run.options.AckTimeout)}
	id, err = run.correlation("event")
	if err != nil {
		call.Complete(invocation.Outcome[WebSocketResult]{Primary: err})
		return err
	}
	incoming, err := invocation.BeginNested(run.ctx, run.call.Scope(), invocation.Request{Name: "websocket-event", Correlation: id, Shape: invocation.Finite,
		EvidenceBytes: run.options.eventBytes(), Admission: invocation.Budget{Limit: run.options.AckTimeout}, AttemptsKnown: true, MaxAttempts: 1}, run.events, run.receiver.observer)
	if err != nil {
		call.Complete(invocation.Outcome[WebSocketResult]{Present: true, Value: WebSocketResult{ack: &WebSocketAck{frameID: pending.frameID, generation: pending.generation}}, Primary: err})
		return err
	}
	run.pending[pending] = true
	incoming.Complete(invocation.Outcome[WebSocketEvent]{Present: true, Value: WebSocketEvent{event: event, pending: pending}})
	run.stats.Delivered++
	run.publish()
	return nil
}
func (value settings) socketEvent(payload []byte) (Event, error) {
	if err := checkJSON(payload, value.WebSocket.MaxMessageBytes); err != nil {
		return Event{}, failure(ErrProtocol, "websocket-event", err)
	}
	if err := exactEventFields(payload); err != nil {
		return Event{}, failure(ErrProtocol, "websocket-event", err)
	}
	var envelope struct {
		Schema string                 `json:"schema"`
		Header *larkevent.EventHeader `json:"header"`
		Event  json.RawMessage        `json:"event"`
	}
	if json.Unmarshal(payload, &envelope) != nil || envelope.Schema != "2.0" || envelope.Header == nil {
		return Event{}, failure(ErrProtocol, "websocket-event")
	}
	header := envelope.Header
	if header.AppID != value.AppID || !identifier(header.EventID) || !identifier(header.TenantKey) || value.TenantKey != "" && header.TenantKey != value.TenantKey {
		return Event{}, failure(ErrAuth, "websocket-event-identity")
	}
	if !strings.HasPrefix(header.EventType, "im.message.") && header.EventType != "card.action.trigger" {
		return Event{}, failure(ErrUnsupported, "websocket-event-type")
	}
	if !shortText(header.EventType, 128) || !shortText(header.CreateTime, 32) || len(envelope.Event) == 0 || envelope.Event[0] != '{' {
		return Event{}, failure(ErrProtocol, "websocket-event")
	}
	return Event{id: header.EventID, kind: header.EventType, app: header.AppID, tenant: header.TenantKey, created: header.CreateTime, data: string(envelope.Event)}, nil
}
func (run *socketRun) writeAcknowledgement(conn *websocket.Conn, pending *socketPending, expired bool) (writeErr error) {
	run.mu.Lock()
	if pending.closed {
		run.mu.Unlock()
		return nil
	}
	pending.closed = true
	code, response, ctx := pending.code, pending.response, pending.ctx
	pending.response = JSON{}
	pending.ctx = nil
	run.mu.Unlock()
	delete(run.pending, pending)
	var primary error
	if expired || !time.Now().Before(pending.deadline) {
		code = 500
		response = JSON{}
		primary = failure(ErrAcknowledgement, "websocket-ack-expired")
	}
	if ctx != nil && ctx.Err() != nil {
		code = 500
		response = JSON{}
		primary = failure(ErrAcknowledgement, "websocket-ack-canceled", ctx.Err(), context.Cause(ctx))
	}
	ack := &WebSocketAck{frameID: pending.frameID, generation: pending.generation, code: code}
	defer func() {
		if writeErr != nil {
			primary = errors.Join(primary, failure(ErrAcknowledgement, "websocket-ack-write", writeErr))
		}
		pending.call.Complete(invocation.Outcome[WebSocketResult]{Present: true, Value: WebSocketResult{ack: ack}, Primary: primary})
		pending.frame = larkws.Frame{}
		run.publish()
	}()
	if run.ctx.Err() != nil {
		return run.ctx.Err()
	}
	native := larkws.NewResponseByCode(code)
	if response.value != "" {
		native.Data = response.Bytes()
	}
	payload, err := json.Marshal(native)
	if err != nil {
		return err
	}
	frame := pending.frame
	frame.Payload = payload
	headers := make([]larkws.Header, 0, len(frame.Headers)+1)
	for _, header := range frame.Headers {
		if header.Key != larkws.HeaderBizRt {
			headers = append(headers, header)
		}
	}
	frame.Headers = append(headers, larkws.Header{Key: larkws.HeaderBizRt, Value: strconv.FormatInt(time.Since(pending.received).Milliseconds(), 10)})
	data, err := frame.Marshal()
	if err != nil {
		return err
	}
	if len(data) > run.options.MaxFrameBytes {
		return failure(ErrLimit, "websocket-ack-bytes")
	}
	if _, err = pending.call.Attempt(); err != nil {
		return err
	}
	deadline := time.Now().Add(run.options.WriteTimeout)
	if code == 200 {
		if pending.deadline.Before(deadline) {
			deadline = pending.deadline
		}
		if ctx != nil {
			if requested, ok := ctx.Deadline(); ok && requested.Before(deadline) {
				deadline = requested
			}
		}
	}
	if err = conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	ack.effect = WebSocketWriteUnknown
	if err = conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		run.stats.AckUnknown++
		return err
	}
	ack.effect = WebSocketWritten
	run.stats.AckWritten++
	if code != 200 {
		run.stats.Rejected++
	}
	return nil
}
func (run *socketRun) abandonPending(cause error) {
	for pending := range run.pending {
		run.mu.Lock()
		pending.closed = true
		pending.response = JSON{}
		pending.ctx = nil
		run.mu.Unlock()
		pending.call.Complete(invocation.Outcome[WebSocketResult]{Present: true, Value: WebSocketResult{ack: &WebSocketAck{frameID: pending.frameID, generation: pending.generation}}, Primary: cause})
		pending.frame = larkws.Frame{}
		delete(run.pending, pending)
		run.stats.Abandoned++
	}
	for {
		select {
		case <-run.acknowledgements:
		default:
			run.publish()
			return
		}
	}
}
