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

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// LoadResource delegates the native resource loader using an observed frame.
// Nil frame preserves the worker-target form, not an implicit page frame.
// Native network success, HTTP status and positive stream EOF are separate facts.
// The receipt retains detached native metadata. A returned stream needs Close
// even when accompanied by an error. Returned ID strings are data, not grants.
func (p *Page) LoadResource(ctx context.Context, frame *Frame, params network.LoadNetworkResourceParams) (result network.LoadNetworkResourceResult, stream *Stream, receipt *adapters.Receipt[Result], err error) {
	stream, receipt, err = p.openStreamResult(ctx, "load_resource", func(work context.Context) (*native.Stream, []byte, error) {
		var selected *native.Frame
		if frame != nil {
			if frame.native == nil {
				return nil, nil, fail(ErrInput, "resource-frame")
			}
			selected = frame.native
		}
		value, opened, err := p.native.LoadResource(work, selected, params)
		result = value
		buffer := payloadBuffer{limit: p.context.budget.EvidenceBytes - 65536}
		var encoder jsonv2.Encoder
		encoder.Reset(&buffer, sdk.DefaultMarshalOptions)
		copyErr := jsonv2.MarshalEncode(&encoder, value, sdk.DefaultMarshalOptions)
		if copyErr != nil {
			buffer.data = nil
		}
		return opened, buffer.data, errors.Join(err, copyErr)
	})
	return result, stream, receipt, err
}
