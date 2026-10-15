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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

// Callback is an exact already-bounded HTTP body and single header values.
// Receive does not read or write an HTTP connection.
type Callback struct {
	private
	Timestamp, Nonce, Signature string
	Body                        []byte
}

func (client *Client) Receive(ctx context.Context, input Callback) (Result, error) {
	return client.receive(ctx, input, false)
}
func (client *Client) ReceiveLegacy(ctx context.Context, input Callback) (Result, error) {
	return client.receive(ctx, input, true)
}
func (client *Client) receive(ctx context.Context, input Callback, legacy bool) (Result, error) {
	return client.dispatch(ctx, "receive", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		callback := native.Callback{Timestamp: input.Timestamp, Nonce: input.Nonce, Signature: input.Signature, Body: input.Body}
		if legacy {
			return connection.ReceiveLegacy(work, id, callback)
		}
		return connection.Receive(work, id, callback)
	})
}

// HTTPResponse preserves native raw/card/custom-status response choices. The
// callback constructs bounded bytes explicitly; modern responses require JSON,
// while legacy raw responses preserve arbitrary bytes. Nil body means no payload.
type HTTPResponse struct {
	private
	Status int
	Body   []byte
}

// HTTPWrite reports local response-write facts only. An entered write can be
// unknown to the platform, even when all bytes returned successfully.
type HTTPWrite struct {
	Status           int
	Entered, Written bool
	Bytes            int
}

func (value Result) HTTPWrite() HTTPWrite { return value.http }

// HandleHTTP authenticates one request and invokes the explicitly supplied
// cooperative handler, then writes its explicit response. It starts no server,
// selects no business ACK policy and never calls the handler for a challenge.
// Reader/Writer/handler remain borrowed through actual return, even after timeout.
// Authentication failures do not write a success response. The host owns error
// responses, socket deadlines and shutdown; an Inbox Ack is not an HTTP response.
func (client *Client) HandleHTTP(ctx context.Context, request *http.Request, writer http.ResponseWriter, legacy bool, handler func(context.Context, Event) (HTTPResponse, error)) (Result, error) {
	if request == nil || request.Body == nil || writer == nil || handler == nil {
		return Result{}, fail(ErrInput, "http-callback")
	}
	if ctx == nil {
		return Result{}, fail(ErrInput, "http-callback")
	}
	ctx, stop := joinContexts(ctx, request.Context())
	defer stop()
	return client.dispatchWith(ctx, "http-callback", func(work context.Context, connection *native.Client, id fault.Correlation) (receipt *invocation.Receipt[native.Result], err error) {
		defer func() { err = errors.Join(err, work.Err(), context.Cause(work)) }()
		if request.Method != "POST" {
			return nil, fail(ErrInput, "http-method")
		}
		read := func(name string) (string, error) {
			var found []string
			for key, values := range request.Header {
				if strings.EqualFold(key, name) {
					found = append(found, values...)
				}
			}
			if len(found) > 1 {
				return "", fail(ErrInput, "duplicate-header")
			}
			if len(found) == 0 {
				return "", nil
			}
			return found[0], nil
		}
		stamp, err := read("X-Lark-Request-Timestamp")
		if err != nil {
			return nil, err
		}
		nonce, err := read("X-Lark-Request-Nonce")
		if err != nil {
			return nil, err
		}
		signature, err := read("X-Lark-Signature")
		if err != nil {
			return nil, err
		}
		limit := connection.RequestLimit()
		if request.ContentLength > int64(limit) {
			return nil, fail(ErrLimit, "http-body")
		}
		var body []byte
		err = joinHTTPCallback(work, func() error {
			var readErr error
			body, readErr = io.ReadAll(io.LimitReader(callbackReader{ctx: work, reader: request.Body}, int64(limit)+1))
			return readErr
		})
		if err != nil {
			return nil, fail(ErrInput, "http-body", err)
		}
		if len(body) > limit {
			return nil, fail(ErrLimit, "http-body")
		}
		if work.Err() != nil {
			return nil, fail(ErrOperation, "http-body", work.Err(), context.Cause(work))
		}
		input := native.Callback{Timestamp: stamp, Nonce: nonce, Signature: signature, Body: body}
		if legacy {
			return connection.ReceiveLegacy(work, id, input)
		}
		return connection.Receive(work, id, input)
	}, func(work context.Context, result Result) (out Result, err error) {
		out = result
		err = joinHTTPCallback(work, func() error {
			event, ok := result.Event()
			if !ok {
				return fail(ErrState, "http-event")
			}
			var response HTTPResponse
			if event.Type() == "url_verification" {
				response.Status = http.StatusOK
				response.Body, _ = json.Marshal(map[string]string{"challenge": event.Challenge()})
			} else {
				var handlerErr error
				response, handlerErr = handler(work, event)
				if handlerErr != nil {
					return handlerErr
				}
			}
			if work.Err() != nil {
				return errors.Join(work.Err(), context.Cause(work))
			}
			if response.Status == 0 {
				response.Status = http.StatusOK
			}
			if response.Status < 200 || response.Status > 599 || len(response.Body) > 30<<10 || !legacy && len(response.Body) > 0 && !json.Valid(response.Body) {
				return fail(ErrInput, "http-response")
			}
			out.http = HTTPWrite{Status: response.Status, Entered: true}
			writer.Header().Set("Content-Type", "application/json; charset=utf-8")
			writer.WriteHeader(response.Status)
			written, writeErr := writer.Write(response.Body)
			out.http.Bytes = written
			out.http.Written = writeErr == nil && written == len(response.Body)
			if writeErr == nil && !out.http.Written {
				writeErr = io.ErrShortWrite
			}
			return writeErr
		})
		return out, err
	})
}

type callbackReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader callbackReader) Read(data []byte) (int, error) {
	if reader.ctx.Err() != nil {
		return 0, reader.ctx.Err()
	}
	return reader.reader.Read(data)
}

func joinHTTPCallback(ctx context.Context, run func() error) (err error) {
	done := make(chan struct{})
	go func() {
		returned := false
		defer func() {
			recovered := recover()
			if !returned {
				cause, _ := recovered.(error)
				err = fail(ErrOperation, "http-callback-exit", cause)
			}
			err = errors.Join(err, ctx.Err(), context.Cause(ctx))
			close(done)
		}()
		err = run()
		returned = true
	}()
	<-done
	return err
}
