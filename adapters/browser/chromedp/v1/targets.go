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

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/target"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// TargetRef carries immutable discovery provenance from one retained context.
// It is not the SDK's Target, and no raw target ID can construct this capability.
type TargetRef struct {
	private
	native  *native.TargetRef
	context *Context
}

func (value *TargetRef) DataCopy() []byte {
	if value == nil || value.native == nil {
		return nil
	}
	return value.native.DataCopy()
}

// Targets returns only native targets in this verified BrowserContext, including
// available popup/frame/worker types selected by the native filter. Unrelated
// peer targets are never returned. This does not infer ownership from discovery.
func (retained *Context) Targets(ctx context.Context, params target.GetTargetsParams) (values []*TargetRef, receipt *adapters.Receipt[Result], err error) {
	_, receipt, err = runContextOperation(ctx, retained, "targets", func(ctx context.Context) ([]jsonv2.Value, error) {
		discovered, err := retained.native.Targets(ctx, params)
		if err != nil {
			return nil, err
		}
		values = make([]*TargetRef, len(discovered))
		data := make([]jsonv2.Value, len(discovered))
		for index, value := range discovered {
			values[index] = &TargetRef{native: value, context: retained}
			data[index] = jsonv2.Value(value.DataCopy())
		}
		return data, nil
	})
	if err != nil {
		return nil, receipt, err
	}
	return values, receipt, nil
}

// Attach borrows a discovered target and verifies its membership again. Repeated
// attachment shares its page state. Native workers support their native runtime
// operations, not page DOM/navigation methods. Context cleanup detaches an attached
// target; an owned BrowserContext still has its normal whole-context disposal.
func (retained *Context) Attach(ctx context.Context, value *TargetRef) (page *Page, receipt *adapters.Receipt[Result], err error) {
	_, receipt, err = runContextOperation(ctx, retained, "attach_target", func(ctx context.Context) (bool, error) {
		if value == nil || value.native == nil || value.context == nil || value.context.contextState != retained.contextState {
			return false, fail(ErrInput, "target-origin")
		}
		attached, err := retained.native.Attach(ctx, value.native)
		if attached != nil {
			page = retained.page(attached)
		}
		return err == nil, err
	})
	return page, receipt, err
}
