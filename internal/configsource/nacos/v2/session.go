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

package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// session owns one Nacos registration epoch on one gRPC connection. The single
// receive loop owns control ACK sends after setup; notify is an internal bounded
// queue handoff, never a caller-supplied application callback.
type session struct {
	owner  *Client
	index  int
	ctx    context.Context
	cancel context.CancelCauseFunc
	conn   *grpc.ClientConn
	unary  wire.RequestClient
	stream wire.BiRequestStream_RequestBiStreamClient
	// received is nil until the receiver starts, then closes after its actual exit.
	received chan struct{}
	notify   func(key)
	once     sync.Once
	// A second transport dial would lose Nacos registration and must retire this epoch.
	dialed atomic.Bool
}

// newSession uses the already-admitted caller's lifetime and one setup budget.
// It validates ServerCheck, opens the stream, sends setup, then requires a native
// HealthCheck response; sending setup alone is not registration evidence.
func (client *Client) newSession(ctx context.Context, index int, notify func(key)) (*session, error) {
	work, cancel := context.WithCancelCause(ctx)
	current := &session{owner: client, index: index, ctx: work, cancel: cancel, notify: notify}
	transport := grpc.WithTransportCredentials(insecure.NewCredentials())
	if !client.settings.Plaintext {
		transport = grpc.WithTransportCredentials(&sessionCredentials{current})
	}
	conn, err := grpc.NewClient("passthrough:///"+client.settings.Servers[index].GRPCAddress,
		transport, grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxWireBytes), grpc.MaxCallSendMsgSize(64<<10)),
		grpc.WithInitialWindowSize(64<<10), grpc.WithInitialConnWindowSize(1<<20),
		grpc.WithMaxHeaderListSize(32<<10),
		grpc.WithContextDialer(func(native context.Context, address string) (net.Conn, error) {
			if !current.dialed.CompareAndSwap(false, true) {
				err := fail(ErrUnavailable, "connection-epoch")
				current.cancel(err)
				return nil, err
			}
			work, stop := context.WithCancelCause(current.ctx)
			defer stop(nil)
			propagation := context.AfterFunc(native, func() { stop(context.Cause(native)) })
			defer propagation()
			if native.Err() != nil {
				stop(context.Cause(native))
			}
			socket, err := client.dial(work, address)
			if err != nil {
				current.cancel(err)
			}
			return socket, err
		}))
	if err != nil {
		cancel(err)
		return nil, fail(ErrInput, "grpc-client", err)
	}
	current.conn, current.unary = conn, wire.NewRequestClient(conn)
	client.mu.Lock()
	closing := client.closing
	if !closing {
		client.sessions[current] = struct{}{}
	}
	client.mu.Unlock()
	if closing {
		current.close()
		return nil, fail(ErrClosed, "session")
	}
	ready := false
	defer func() {
		if !ready {
			current.close()
		}
	}()
	budget, stop := context.WithTimeout(work, client.settings.Timeout)
	defer stop()
	check := request.NewServerCheckRequest()
	check.RequestId = strconv.FormatUint(client.sequence.Add(1), 10)
	payload, err := current.unary.Request(budget, client.envelope(check, ""), grpc.WaitForReady(true))
	if err != nil {
		return nil, current.failure("server-check", err)
	}
	checked, err := decodeResponse(payload, "ServerCheckResponse")
	if err != nil {
		return nil, err
	}
	if identity := checked.(*response.ServerCheckResponse).ConnectionId; !identifier(identity, 256, false) {
		return nil, fail(ErrDecode, "connection-id")
	}
	current.stream, err = wire.NewBiRequestStreamClient(conn).RequestBiStream(work)
	if err != nil {
		return nil, current.failure("stream", err)
	}
	setup := request.NewConnectionSetupRequest()
	setup.RequestId = strconv.FormatUint(client.sequence.Add(1), 10)
	setup.ClientVersion = constant.CLIENT_VERSION
	setup.Tenant = client.settings.Namespace
	setup.Labels = map[string]string{"source": "sdk", "module": "config", "AppName": client.settings.AppName, "taskId": client.settings.Name}
	if err = current.send(budget, client.envelope(setup, "")); err != nil {
		return nil, err
	}
	current.received = make(chan struct{})
	go current.receive()
	health := request.NewHealthCheckRequest()
	health.RequestId = strconv.FormatUint(client.sequence.Add(1), 10)
	if _, err = current.call(budget, health, "HealthCheckResponse", false); err != nil {
		return nil, err
	}
	ready = true
	return current, nil
}
func (current *session) failure(operation string, err error) error {
	identity := ErrUnavailable
	if status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.PermissionDenied {
		identity = ErrDenied
	} else if status.Code(err) == codes.ResourceExhausted {
		identity = ErrLimit
	}
	return fail(identity, operation, err, current.ctx.Err(), context.Cause(current.ctx))
}

// call keeps native response decoding inside the caller's allowance and budget.
// Only code 301 (registration not ready) is retried here; it is not configuration
// absence, and retry exhaustion must retain the last native failure.
func (current *session) call(ctx context.Context, value request.IRequest, expected string, authenticated bool) (response.IResponse, error) {
	work, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := context.AfterFunc(current.ctx, func() { cancel(context.Cause(current.ctx)) })
	defer stop()
	if current.ctx.Err() != nil {
		cancel(context.Cause(current.ctx))
	}
	var lastError error
	for attempt := 0; attempt < 16; attempt++ {
		if work.Err() != nil {
			return nil, current.failure("request", errors.Join(work.Err(), context.Cause(work)))
		}
		accessToken := ""
		if authenticated {
			var err error
			accessToken, err = current.owner.token(work, current.index)
			if err != nil {
				return nil, err
			}
		}
		raw, err := current.unary.Request(work, current.owner.envelope(value, accessToken))
		if err != nil {
			return nil, current.failure("request", err)
		}
		result, err := decodeResponse(raw, expected)
		if err == nil {
			if work.Err() != nil {
				return nil, current.failure("response", errors.Join(work.Err(), context.Cause(work)))
			}
			return result, nil
		}
		if errors.Is(err, ErrDenied) {
			current.owner.forgetToken(current.index, accessToken)
		}
		lastError = err
		var native *RemoteError
		if !errors.As(err, &native) || native.ErrorCode() != 301 {
			return nil, err
		}
		timer := time.NewTimer(current.owner.settings.Retry)
		select {
		case <-timer.C:
		case <-work.Done():
			timer.Stop()
			return nil, current.failure("registration", errors.Join(err, work.Err(), context.Cause(work)))
		}
	}
	return nil, fail(ErrUnavailable, "registration", lastError)
}

// send is serialized by its owner: setup runs before receive starts, and receive
// owns later ACKs. A per-message timeout cancels the whole stream because gRPC
// Send has no independent operation context; no timed-out sender is detached.
func (current *session) send(ctx context.Context, payload *wire.Payload) error {
	stop := context.AfterFunc(ctx, func() { current.cancel(context.Cause(ctx)) })
	defer stop()
	if ctx.Err() != nil {
		return current.failure("stream-send", errors.Join(ctx.Err(), context.Cause(ctx)))
	}
	if err := current.stream.Send(payload); err != nil {
		return current.failure("stream-send", err)
	}
	return nil
}

// receive validates routing before handing off invalidations and ACKs the exact
// native request ID. Loss, malformed scope or a reset ends this registration epoch.
func (current *session) receive() {
	defer close(current.received)
	for {
		payload, err := current.stream.Recv()
		if err != nil {
			current.cancel(current.failure("stream-receive", err))
			return
		}
		raw, err := payloadBody(payload)
		if err != nil {
			current.cancel(err)
			return
		}
		var identity struct {
			RequestID *string `json:"requestId"`
		}
		if json.Unmarshal(raw, &identity) != nil || identity.RequestID == nil || !identifier(*identity.RequestID, 128, false) {
			current.cancel(fail(ErrDecode, "push-identity"))
			return
		}
		base := &response.Response{ResultCode: 200, Success: true, RequestId: *identity.RequestID}
		var reply response.IResponse
		reset := false
		switch payload.Metadata.Type {
		case "ClientDetectionRequest":
			reply = &response.ClientDetectionResponse{Response: base}
		case "ConnectResetRequest":
			// A reset is acknowledged, not authority to follow its suggested address.
			reply = &response.ConnectResetResponse{Response: base}
			reset = true
		case "ConfigChangeNotifyRequest":
			var fields struct {
				Group  *string `json:"group"`
				DataID *string `json:"dataId"`
				Tenant *string `json:"tenant"`
			}
			if json.Unmarshal(raw, &fields) != nil || fields.Group == nil || fields.DataID == nil {
				current.cancel(fail(ErrDecode, "push-scope"))
				return
			}
			// Default-namespace subscriptions can receive a null/omitted tenant from
			// the native server. The same form must never match a named namespace.
			tenant := ""
			if fields.Tenant != nil {
				tenant = *fields.Tenant
			}
			if tenant != current.owner.settings.Namespace {
				current.cancel(fail(ErrDecode, "push-scope"))
				return
			}
			selected := key{*fields.Group, *fields.DataID}
			if !slices.Contains(current.owner.settings.Keys, selected) {
				current.cancel(fail(ErrDecode, "push-key"))
				return
			}
			if current.notify != nil {
				current.notify(selected)
			}
			reply = &response.NotifySubscriberResponse{Response: base}
		default:
			current.cancel(fail(ErrUnsupported, "push-type"))
			return
		}
		budget, stop := context.WithTimeout(current.ctx, current.owner.settings.Timeout)
		err = current.send(budget, acknowledgement(reply))
		stop()
		if err != nil {
			current.cancel(err)
			return
		}
		if reset {
			current.cancel(fail(ErrUnavailable, "server-reset"))
			return
		}
	}
}

// close cancels native I/O before joining the receiver and unregistering the
// epoch. Outstanding native acquisitions remain accounted for by Client.native.
func (current *session) close() {
	current.once.Do(func() {
		current.cancel(ErrClosed)
		_ = current.conn.Close()
		if current.received != nil {
			<-current.received
		}
		current.owner.mu.Lock()
		delete(current.owner.sessions, current)
		current.owner.mu.Unlock()
	})
}
