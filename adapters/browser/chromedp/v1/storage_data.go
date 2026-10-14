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

	"github.com/chromedp/cdproto/indexeddb"
	"github.com/frost-leo/fathomry/adapters/v1"
)

// ReadIndexedDB returns native paged records in this group's remote-object
// custody. The StorageScope must belong to the same original Page. Its origin,
// partition or bucket is filled in copied native parameters. PageSize and
// SkipCount must be nonnegative; PageSize zero keeps native semantics. Three
// object slots per requested row are reserved before encoding and dispatch.
//
// Key, primary-key and value pointers can be passed to Page.Object for managed
// inspection. Copies/raw IDs are not grants. Close this group to release these
// references individually, without clearing the shared native IndexedDB group or
// deleting stored data. Unknown or oversized results keep custody until positive
// session/resource teardown. HasMore is not a database snapshot or transaction.
func (group *ObjectGroup) ReadIndexedDB(ctx context.Context, scope *StorageScope, params indexeddb.RequestDataParams) (indexeddb.RequestDataResult, *adapters.Receipt[Result], error) {
	if group == nil || group.state == nil || scope == nil || scope.native == nil || scope.page == nil || scope.page.native != group.state.page.native {
		return indexeddb.RequestDataResult{}, nil, fail(ErrInput, "indexeddb-origin")
	}
	return runGroupOperation(ctx, group, "indexeddb_read", func(work context.Context) (indexeddb.RequestDataResult, []byte, error) {
		value, err := group.state.native.ReadIndexedDB(work, scope.native, params)
		return value.NativeValue(), value.DataCopy(), err
	})
}
