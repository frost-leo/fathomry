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

	"github.com/chromedp/cdproto/fetch"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// ResponseBody reads this response-stage pause once. maxBytes bounds native
// encoded output before decoding, not network traffic or browser memory. The
// existing control ledger reserves twice that limit plus metadata before work.
// It retains a separate copy of the decoded native result even when ignored.
// No new Inbox or ordinary command admission is required. Reply remains a
// separate action; neither HTTP success nor request continuation is implied.
func (pending Pending) ResponseBody(ctx context.Context, maxBytes int64) (fetch.GetResponseBodyResult, error) {
	value, err := pending.native.ResponseBody(ctx, maxBytes)
	return value, translate(err, "response-body")
}

// ResponseBodyStream takes this response as a sequential managed IO stream.
// It needs one independent stream receipt before dispatch. Read through EOF,
// then FulfillRequest or FailRequest; continuing the original response is invalid.
// Close the stream even when a non-nil handle accompanies an error. Control.Close
// also closes ignored native streams and aborts their still-paused requests.
// Native IDs in event data cannot substitute for the original Pending grant.
func (pending Pending) ResponseBodyStream(ctx context.Context) (*Stream, *adapters.Receipt[Result], error) {
	if pending.control == nil || pending.control.state == nil {
		return nil, nil, fail(ErrInput, "response-stream")
	}
	return pending.control.state.page.openStream(ctx, "response_stream", func(work context.Context) (*native.Stream, error) {
		return pending.native.ResponseBodyStream(work)
	})
}
