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
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/notification/mail/v0"
)

// BatchOptions enables continuation to later positions, never automatic resend.
type BatchOptions struct{ ContinueOnError bool }

func (client *Client) Send(ctx context.Context, message Message) (Result, error) {
	return client.SendBatch(ctx, []Message{message}, BatchOptions{})
}
func (client *Client) SendBatch(ctx context.Context, messages []Message, options BatchOptions) (Result, error) {
	if len(messages) > 64 {
		return Result{}, fail(ErrLimit, "batch")
	}
	frozen := make([]native.Message, len(messages))
	for index, message := range messages {
		frozen[index] = message.native
	}
	return client.dispatch(ctx, "send", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.SendBatchWithOptions(work, id, frozen, native.BatchOptions{ContinueOnError: options.ContinueOnError})
	})
}

// LocalDelivery authorizes only the explicit absolute executable. Arguments may
// not override the managed envelope. No shell, PATH search or real-mail permission
// is inferred from constructing this value.
type LocalDelivery struct {
	private
	Executable string
	Arguments  []string
}

func (client *Client) Sendmail(ctx context.Context, message Message, command LocalDelivery) (Result, error) {
	return client.dispatch(ctx, "sendmail", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Sendmail(work, id, message.native, native.LocalDelivery{Executable: command.Executable, Arguments: command.Arguments})
	})
}

// SessionRequest uses one admitted connection for inspection/NOOP/RSET/VRFY.
// Close terminates that session, not the shared source or other calls.
type SessionRequest struct {
	private
	Extensions         []string
	Verify             string
	Noop, Reset, Close bool
}

func (client *Client) Session(ctx context.Context, input SessionRequest) (Result, error) {
	return client.dispatch(ctx, "session", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Session(work, id, native.SessionRequest{Extensions: input.Extensions, Verify: input.Verify, Noop: input.Noop, Reset: input.Reset, Close: input.Close})
	})
}
