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
	"errors"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	sdk "github.com/chromedp/chromedp"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	resource "github.com/frost-leo/fathomry/internal/resource"
)

// RunOwnerCommand provides finite browser/process observations for an owned
// local browser only. A connection Owner for a borrowed browser is not a browser
// owner. Native process data and command-line results are deliberately sensitive.
// Owner commands share one per-source command/output allowance. They do not
// acquire the independent allowances of retained contexts.
func RunOwnerCommand[P, R any](ctx context.Context, owner *Owner, command cdp.Command[P, R], params P) (value R, receipt *adapters.Receipt[Result], err error) {
	if ctx == nil || owner == nil || owner.state == nil || owner.client == nil {
		return value, nil, fail(ErrInput, "owner-command")
	}
	state := owner.state
	var primary error
	receipt, err = owner.client.endpoint.Child(ctx, state.call.Scope(), request("owner_command", "", 0, state.prepared.policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := state.use()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			return
		}
		defer finish()
		resolved := false
		defer func() {
			if !resolved {
				cause, _ := recover().(error)
				primary = fail(ErrOperation, "owner-command-did-not-return", cause)
				_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			}
		}()
		capability, _, err := resource.Bind(state.assembly, state.selected)
		if err != nil {
			primary = translate(err, "owner-command")
			_ = call.Resolve(adapters.Outcome[Result]{Primary: primary})
			resolved = true
			return
		}
		output, err := native.RunOwnerCommand(call.Context(), capability, command, params)
		primary = translate(err, "owner-command")
		buffer := payloadBuffer{limit: state.prepared.policy.Budget.EvidenceBytes - 65536}
		if err == nil {
			var encoder jsonv2.Encoder
			encoder.Reset(&buffer, sdk.DefaultMarshalOptions)
			if err = jsonv2.MarshalEncode(&encoder, output, sdk.DefaultMarshalOptions); err != nil {
				primary = translate(err, "owner-output")
			} else {
				value = output
			}
		}
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: buffer.data, attribution: attribution(info.Info()), observation: browser.Observation{Returned: err == nil, Effect: browser.Unknown}}, Primary: primary})
		resolved = true
	})
	return value, receipt, errors.Join(err, receiptError(receipt, primary))
}
