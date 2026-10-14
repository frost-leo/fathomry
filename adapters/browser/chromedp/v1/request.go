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
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Request retains an observed network request on its original target and source.
// ControlOptions.Network observes request starts, responses and terminal events
// from the SDK-enabled Network domain. It does not intercept or replay requests.
// The control ledger bounds cumulative event count and bytes. Closing it revokes
// its grants without disabling the SDK's Network domain or canceling HTTP work.
type Request struct {
	private
	native  native.Request
	control *Control
}

// Request accepts only a Network event whose start this Control observed. Raw
// event IDs and events from an earlier subscription cannot create a grant.
// Redirects that reuse the native ID revoke grants for the preceding response.
func (pending Pending) Request() (Request, error) {
	if pending.control == nil {
		return Request{}, fail(ErrInput, "request-origin")
	}
	value, err := pending.native.Request()
	return Request{native: value, control: pending.control}, translate(err, "request-origin")
}

// RunRequestCommand accepts native Network.getResponseBody, getRequestPostData,
// searchInResponseBody and replayXHR with exact types. It fills RequestID only in
// a copied parameter value, rejecting conflicting identities. Body reads/search
// require an observed successful loadingFinished; HTTP error statuses still have
// bodies. Browser eviction may make a previously observed body unavailable.
// Post data keeps native multipart-file omissions. ReplayXHR explicitly sends
// another HTTP request; evidence redelivery never repeats it.
func RunRequestCommand[P, R any](ctx context.Context, request Request, command cdp.Command[P, R], params P) (result R, receipt *adapters.Receipt[Result], err error) {
	if request.control == nil || request.control.state == nil {
		return result, nil, fail(ErrInput, "request-command")
	}
	return runPageOperation(ctx, request.control.state.page, "request_command", func(work context.Context) (R, error) {
		return native.RunRequestCommand(work, request.native, command, params)
	})
}
