/*
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

package chromedp

import (
	"context"
	"reflect"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
)

// Request retains one observed request generation on one target session.
// A redirect that reuses its native ID revokes the earlier generation.
type Request struct {
	private
	state *requestState
}

type requestState struct {
	control                    *controlState
	id                         network.RequestID
	frame                      cdp.FrameID
	loader                     cdp.LoaderID
	kind                       network.ResourceType
	response, finished, failed bool
	retired                    bool
}

type requestLedger struct {
	requests map[network.RequestID]*requestState
	events   map[uint64]*requestState
	count    int
	bytes    int64
}

// observeRequest runs in native event order under the control mutex. Optional
// observations cannot authorize an ID that was already active when they began.
func (state *controlState) observeRequest(method string, data jsonv2.Value, sequence uint64) error {
	if state.network == nil {
		return nil
	}
	switch method {
	case network.RequestWillBeSent.Method, network.ResponseReceived.Method, network.LoadingFinished.Method, network.LoadingFailed.Method:
	default:
		return nil
	}
	ledger, settings := state.network, state.session.owner.settings
	if ledger.count >= settings.MaxEvents || int64(len(data))+256 > settings.MaxResultBytes-ledger.bytes {
		return failure(ErrLimit, "request-origins")
	}
	ledger.count++
	ledger.bytes += int64(len(data)) + 256
	var event struct {
		RequestID network.RequestID    `json:"requestId"`
		FrameID   cdp.FrameID          `json:"frameId"`
		LoaderID  cdp.LoaderID         `json:"loaderId"`
		Type      network.ResourceType `json:"type"`
		Request   *struct{}            `json:"request"`
		Response  *struct{}            `json:"response"`
	}
	if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
		return failure(ErrNative, "request-event", err)
	}
	if sequence == 0 || event.RequestID == "" {
		return failure(ErrNative, "request-identity")
	}
	current := ledger.requests[event.RequestID]
	switch method {
	case network.RequestWillBeSent.Method:
		if event.Request == nil {
			return failure(ErrNative, "request-data")
		}
		if current != nil {
			current.retired = true
		}
		current = &requestState{control: state, id: event.RequestID, frame: event.FrameID, loader: event.LoaderID, kind: event.Type}
		ledger.requests[event.RequestID] = current
	case network.ResponseReceived.Method:
		if event.Response == nil {
			return failure(ErrNative, "response-data")
		}
		if current != nil {
			if current.loader != event.LoaderID || current.frame != event.FrameID {
				return failure(ErrNative, "response-origin")
			}
			current.response = true
		}
	case network.LoadingFinished.Method:
		if current != nil {
			current.finished = true
		}
	case network.LoadingFailed.Method:
		if current != nil {
			current.failed = true
		}
	}
	ledger.events[sequence] = current
	return nil
}

// Request derives authority from the producer-observed original request, not a
// requestId copied out of DataCopy. Events for unobserved starts have no grant.
func (pending Pending) Request() (Request, error) {
	if pending.state == nil || pending.state.network == nil {
		return Request{}, failure(ErrInput, "request-origin")
	}
	value := Request{state: pending.state.network}
	if err := value.check(""); err != nil {
		return Request{}, err
	}
	return value, nil
}

func (request Request) check(method string) error {
	if request.state == nil || request.state.control == nil {
		return failure(ErrInput, "request")
	}
	state, control := request.state, request.state.control
	control.mu.Lock()
	defer control.mu.Unlock()
	return state.checkLocked(method)
}

func (state *requestState) checkLocked(method string) error {
	control := state.control
	if control.network == nil || control.closing || control.closed || control.life.Err() != nil || state.retired || control.network.requests[state.id] != state {
		return failure(ErrState, "request-origin")
	}
	if method == network.GetResponseBody.Method || method == network.SearchInResponseBody.Method {
		if !state.response || !state.finished || state.failed {
			return failure(ErrState, "request-body-incomplete")
		}
	}
	if method == network.ReplayXHR.Method && state.kind != network.ResourceTypeXHR {
		return failure(ErrInput, "request-not-xhr")
	}
	return nil
}

func (request Request) enter(ctx context.Context, method string) (context.Context, func(), error) {
	if ctx == nil || request.state == nil || request.state.control == nil {
		return nil, nil, failure(ErrInput, "request")
	}
	state, control := request.state, request.state.control
	return control.enterCommand(ctx, func() error { return state.checkLocked(method) })
}

// RunRequestCommand delegates finite native response-body, post-data, body search
// and explicit XHR replay operations. Copying data never grants new identity.
func RunRequestCommand[P, R any](ctx context.Context, request Request, command cdp.Command[P, R], params P) (result R, err error) {
	work, finish, err := request.enter(ctx, command.Method)
	if err != nil {
		return result, err
	}
	defer finish()
	session := request.state.control.session
	return RunAction(work, session, Action[R]{local: func(work context.Context, session *Session) (result R, err error) {
		switch command.Method {
		case network.GetResponseBody.Method, network.GetRequestPostData.Method, network.SearchInResponseBody.Method, network.ReplayXHR.Method:
		default:
			return result, failure(ErrUnsupported, "request-command")
		}
		schema := protocolCommands[command.Method]
		if !nativeCommandType(reflect.TypeFor[P](), schema.params) || !nativeCommandType(reflect.TypeFor[R](), schema.result) {
			return result, failure(ErrInput, "request-command-types")
		}
		if err := request.check(command.Method); err != nil {
			return result, err
		}
		if err := session.reserveCommand(); err != nil {
			return result, err
		}
		buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return result, err
		}
		var copied P
		if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
			return result, err
		}
		var identity *network.RequestID
		switch value := any(&copied).(type) {
		case *network.GetResponseBodyParams:
			identity = &value.RequestID
		case *network.GetRequestPostDataParams:
			identity = &value.RequestID
		case *network.SearchInResponseBodyParams:
			identity = &value.RequestID
		case *network.ReplayXHRParams:
			identity = &value.RequestID
		}
		if identity == nil || *identity != "" && *identity != request.state.id {
			return result, failure(ErrInput, "request-identity")
		}
		*identity = request.state.id
		return cdp.Call(session.commandWork(work, false, func() error { return request.check(command.Method) }), sdk.FromContext(session.native).Target, command, copied)
	}})
}
