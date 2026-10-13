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

	"github.com/chromedp/cdproto/runtime"
	"github.com/frost-leo/fathomry/adapters/v1"
)

// Properties returns native descriptors without evaluating getters. Parameters
// are borrowed until return. ObjectID must be absent or equal this object's ID.
// Returned native pointers can be passed to the original Page.Object to obtain
// a managed reference; copied metadata and raw IDs do not grant authority.
// References share this object's group and its document/source lifetime.
// Oversized results stop the group and retain custody until cleanup completes.
func (object *Object) Properties(ctx context.Context, params runtime.GetPropertiesParams) (runtime.GetPropertiesResult, *adapters.Receipt[Result], error) {
	if object == nil || object.native == nil || object.group == nil {
		return runtime.GetPropertiesResult{}, nil, fail(ErrInput, "object-properties")
	}
	return runGroupOperation(ctx, object.group, "object_properties", func(ctx context.Context) (runtime.GetPropertiesResult, []byte, error) {
		properties, err := object.native.Properties(ctx, params)
		return properties.NativeValue(), properties.DataCopy(), err
	})
}
