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
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

func runManagedValue[T any](ctx context.Context, page *Page, action Action[T]) (value T, receipt *adapters.Receipt[Result], err error) {
	var output T
	group, receipt, err := newObjectGroup(ctx, page, action.name, true, func(ctx context.Context, group *native.ObjectGroup) (*native.Object, error) {
		var custody *native.Object
		var err error
		output, custody, err = native.RunValueAction(ctx, page.native, group, action.native)
		return custody, err
	}, func() ([]byte, error) {
		buffer := payloadBuffer{limit: page.context.budget.EvidenceBytes - 65536}
		var encoder jsonv2.Encoder
		encoder.Reset(&buffer, sdk.DefaultMarshalOptions)
		err := jsonv2.MarshalEncode(&encoder, output, sdk.DefaultMarshalOptions)
		if err == nil {
			value = output
		}
		return buffer.data, err
	})
	if group != nil && group.state.native.Settled() {
		err = errors.Join(err, group.Close(context.Background()))
	}
	return value, receipt, err
}
