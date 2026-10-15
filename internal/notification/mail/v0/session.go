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

package mail

import (
	"context"
	"errors"
	"slices"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// SessionInfo reports one actual connection, not source readiness or delivery.
type SessionInfo struct {
	private
	Network, Authentication, Greeting string
	TLS                               bool
	TLSVersion, CipherSuite           uint16
	Port, ConnectionAttempts          int
	Extensions                        []Extension
}
type Extension struct {
	private
	Name, Value string
	Supported   bool
}

// SessionRequest inspects or controls a single admitted session. A zero request
// connects/authenticates; subsequent calls may reuse an idle connection.
type SessionRequest struct {
	private
	Extensions         []string
	Verify             string
	Noop, Reset, Close bool
}

func (client *Client) execute(ctx context.Context, id fault.Correlation, name string, work func(context.Context) (Result, error, error)) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, name)
	}
	value := client.owner.settings
	call, err := invocation.Begin(ctx, client.access, invocation.Request{
		Name: name, Correlation: id, Shape: invocation.Finite, Bytes: value.reservation(), EvidenceBytes: value.evidenceReservation(),
		Admission: invocation.Budget{Limit: value.Timeout}, AttemptsKnown: true, MaxAttempts: 1}, client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	live, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	defer cancel()
	result, primary, cleanup := work(live)
	if acquired, ok := primary.(*inputCloseError); ok {
		primary, cleanup = acquired.primary, errors.Join(cleanup, acquired.cleanup)
	}
	if primary != nil {
		primary = phaseFailure(ErrSend, name, live, primary)
	}
	if cleanup != nil {
		cleanup = phaseFailure(ErrCleanup, name, live, cleanup)
	}
	call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: primary, Cleanup: cleanup})
	return call.Receipt(), nil
}

func (client *Client) Session(ctx context.Context, id fault.Correlation, request SessionRequest) (*invocation.Receipt[Result], error) {
	if len(request.Extensions) > 32 || !line(request.Verify, 512) {
		return nil, failure(ErrInput, "session")
	}
	for _, name := range request.Extensions {
		if name == "" || !token(name, 64) {
			return nil, failure(ErrInput, "extension")
		}
	}
	request.Extensions = slices.Clone(request.Extensions)
	return client.execute(ctx, id, "session", func(work context.Context) (Result, error, error) {
		conn, stop, err := client.owner.acquire(work)
		var result Result
		if err == nil {
			result.session = conn.info
			result.session.Greeting = conn.native.HelloResponse()
			for _, name := range request.Extensions {
				supported, value := conn.native.Extension(name)
				result.session.Extensions = append(result.session.Extensions, Extension{Name: name, Supported: supported, Value: value})
			}
			if request.Verify != "" {
				err = conn.native.Verify(request.Verify)
			}
			if err == nil && request.Noop {
				err = conn.native.Noop()
			}
			if err == nil && request.Reset {
				err = conn.native.Reset()
			}
		}
		var cleanup error
		if conn != nil && err == nil && request.Close {
			cleanup = conn.native.Quit()
		}
		stop()
		if conn != nil {
			result.responses = slices.Clone(conn.responses)
			result.connections = slices.Clone(conn.connectionErrors)
			if err == nil && cleanup == nil && !request.Close && work.Err() == nil {
				client.owner.put(conn)
			} else {
				cleanup = errors.Join(cleanup, conn.raw.Close())
			}
		}
		return result, err, cleanup
	})
}

func (result Result) SessionInfo() SessionInfo {
	value := result.session
	value.Extensions = slices.Clone(value.Extensions)
	return value
}
func (result Result) PreparedMessage() (Message, bool) {
	return result.message, result.message.content != nil
}
func (result Result) BytesWritten() int64 { return result.written }

// ResponseErrors retains original SMTP response failures, including errors a
// declared handler recovered from. Inspection may expose private peer text.
func (result Result) ResponseErrors() []error { return slices.Clone(result.responses) }

// ConnectionErrors retains failed initial transports even if explicit fallback succeeds.
func (result Result) ConnectionErrors() []error { return slices.Clone(result.connections) }

// requiresUTF8 concerns SMTP envelope bytes, not display names encoded by MIME.
func (message Message) requiresUTF8() bool {
	for _, value := range append([]string{message.sender}, message.recipients...) {
		for _, char := range value {
			if char > 127 {
				return true
			}
		}
	}
	return false
}
func (message Message) requires8Bit() bool {
	if message.content.Encoding == "8bit" {
		return true
	}
	for _, part := range message.content.Parts {
		if part.Encoding == "8bit" {
			return true
		}
	}
	for _, file := range message.content.Attachments {
		if file.Encoding == "8bit" {
			return true
		}
	}
	for _, file := range message.content.Inline {
		if file.Encoding == "8bit" {
			return true
		}
	}
	return false
}
func (message Message) checkSession(conn *connection, value settings) error {
	if message.requiresUTF8() {
		ok, _ := conn.native.Extension("SMTPUTF8")
		if !ok || value.DisableSMTPUTF8 {
			return failure(ErrUnsupported, "smtp-utf8")
		}
	}
	if message.requires8Bit() {
		ok, _ := conn.native.Extension("8BITMIME")
		if !ok {
			return failure(ErrUnsupported, "8bitmime")
		}
	}
	if value.DSNReturn != "" || len(value.DSNNotify) != 0 {
		ok, _ := conn.native.Extension("DSN")
		if !ok {
			return failure(ErrUnsupported, "dsn")
		}
	}
	return nil
}
