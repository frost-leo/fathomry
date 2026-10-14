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

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
)

// LoadResource retains native network status, headers and an independently owned
// IO stream. Nil frame preserves the native worker-target choice; it never fills
// a page's frame implicitly. Native transport success is not HTTP success.
// A non-nil stream needs Close even when the call also returns an error.
func (session *Session) LoadResource(ctx context.Context, frame *Frame, params network.LoadNetworkResourceParams) (result network.LoadNetworkResourceResult, stream *Stream, err error) {
	result, err = RunAction(ctx, session, Action[network.LoadNetworkResourceResult]{local: func(work context.Context, session *Session) (network.LoadNetworkResourceResult, error) {
		var zero network.LoadNetworkResourceResult
		check := func() error {
			if frame != nil {
				return frame.valid(session)
			}
			return nil
		}
		if err := check(); err != nil {
			return zero, err
		}
		var id cdp.FrameID
		if frame != nil {
			id = frame.id
		}
		if params.FrameID != "" && params.FrameID != id {
			return zero, failure(ErrInput, "resource-frame")
		}
		if params.Options == nil {
			return zero, failure(ErrInput, "resource-options")
		}
		var err error
		stream, err = session.reserveStream()
		if err != nil {
			return zero, err
		}
		abandon := func(err error) (network.LoadNetworkResourceResult, error) {
			stream.state.cancel()
			stream.state.finish()
			stream = nil
			return zero, err
		}
		if err := session.reserveCommand(); err != nil {
			return abandon(err)
		}
		buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return abandon(err)
		}
		var copied network.LoadNetworkResourceParams
		if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
			return abandon(err)
		}
		copied.FrameID = id
		if err := check(); err != nil {
			return abandon(err)
		}
		value, err := cdp.Call(session.commandWork(work, false, check), sdk.FromContext(session.native).Target, network.LoadNetworkResource, copied)
		if value.Resource != nil {
			stream.state.id = value.Resource.Stream
			if err == nil && value.Resource.Success && stream.state.id == "" {
				err = failure(ErrNative, "resource-stream-identity")
			}
		} else if err == nil {
			err = failure(ErrNative, "resource-result")
		}
		if err == nil {
			if stream.state.id == "" {
				stream.state.cancel()
				stream.state.finish()
				stream = nil
			}
			err = check()
		}
		if stream != nil {
			stream.state.primary = err
		}
		return value, err
	}})
	return result, stream, err
}
