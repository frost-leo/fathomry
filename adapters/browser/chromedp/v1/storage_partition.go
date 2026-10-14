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

	"github.com/chromedp/cdproto/storage"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// StoragePartition observes a native storage key for this Page or an explicit
// Frame grant. Nil passes the native target-only choice; browser versions may
// require an explicit frame for a page and support nil only for workers.
// Zero, foreign and retired Frames are
// refused. Acquired partition data can outlive navigation, but not its original
// page/context/source. Borrowed contexts still require SharedStorage.
// It does not infer a partition from a URL, raw key or another page's metadata.
func (page *Page) StoragePartition(ctx context.Context, frame *Frame) (scope *StorageScope, receipt *adapters.Receipt[Result], err error) {
	_, receipt, err = runPageOperation(ctx, page, "storage_partition", func(work context.Context) (storage.GetStorageKeyResult, error) {
		var selected *native.Frame
		if frame != nil {
			if frame.native == nil {
				return storage.GetStorageKeyResult{}, fail(ErrInput, "storage-frame")
			}
			selected = frame.native
		}
		value, err := page.native.StoragePartition(work, selected)
		if err != nil {
			return storage.GetStorageKeyResult{}, err
		}
		scope = &StorageScope{native: value, page: page}
		return storage.GetStorageKeyResult{StorageKey: storage.SerializedStorageKey(value.StorageKey())}, nil
	})
	if err != nil {
		scope = nil
	}
	return scope, receipt, err
}

// StorageKey deliberately exposes sensitive native partition metadata. It is
// not a transferable grant. Origin-only scopes return the empty string.
func (scope *StorageScope) StorageKey() string {
	if scope == nil || scope.native == nil {
		return ""
	}
	return scope.native.StorageKey()
}

// Bucket narrows a partition grant to an explicit native bucket name. Empty
// selects the default bucket. It does not create, own or automatically delete
// browser data. Bucket scopes cannot widen back to a partition or origin.
func (scope *StorageScope) Bucket(name string) (*StorageScope, error) {
	if scope == nil || scope.native == nil {
		return nil, fail(ErrInput, "storage-bucket")
	}
	value, err := scope.native.Bucket(name)
	if err != nil {
		return nil, translate(err, "storage-bucket")
	}
	return &StorageScope{native: value, page: scope.page}, nil
}
