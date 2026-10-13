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

	"github.com/chromedp/cdproto/cachestorage"
	"github.com/chromedp/cdproto/cdp"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// StorageScope selects explicit native origin data in a retained BrowserContext.
// It is a non-owning capability, revoked with its original page/context/source.
// Native origin-level data can outlive a document. This scope does not imply
// cookie/storage exclusivity in a borrowed or default context.
type StorageScope struct {
	private
	native *native.StorageScope
	page   *Page
}

// StorageCache is a native cache result's authority, not a caller-supplied ID.
// It does not own the cache's persistent data and has no automatic Delete/Close.
type StorageCache struct {
	private
	native *native.StorageCache
	scope  *StorageScope
}

// Storage selects an explicit security origin without native work. Borrowed
// contexts require SharedStorage, including for reads. Empty/opaque origins are
// refused; URLs are not normalized into origins. Session storage means this page,
// while local storage/cache/database data is shared with its BrowserContext.
func (page *Page) Storage(origin string) (*StorageScope, error) {
	if page == nil || page.context == nil || page.native == nil {
		return nil, fail(ErrInput, "storage")
	}
	value, err := page.native.Storage(origin)
	if err != nil {
		return nil, translate(err, "storage")
	}
	return &StorageScope{native: value, page: page}, nil
}

// RunStorageCommand fills an exact origin in copied native parameters after
// admission. Its finite set covers DOMStorage data, CacheStorage discovery,
// IndexedDB schema/metadata/deletion and Storage origin usage/clear.
// Partition scopes also support native partition clear and explicit bucket
// deletion. Conflicting origin/key/bucket selectors cannot widen the grant.
// IndexedDB remote-object results use ObjectGroup.ReadIndexedDB instead.
func RunStorageCommand[P, R any](ctx context.Context, scope *StorageScope, command cdp.Command[P, R], params P) (value R, receipt *adapters.Receipt[Result], err error) {
	if scope == nil || scope.native == nil {
		return value, nil, fail(ErrInput, "storage")
	}
	return runPageOperation(ctx, scope.page, "storage_command", func(ctx context.Context) (R, error) {
		return native.RunStorageCommand(ctx, scope.native, command, params)
	})
}

// Cache accepts only an exact native pointer from this scope's RequestCacheNames
// result. Copying data, changing its ID or using another scope cannot grant access.
func (scope *StorageScope) Cache(value *cachestorage.Cache) (*StorageCache, error) {
	if scope == nil || scope.native == nil {
		return nil, fail(ErrInput, "storage-cache")
	}
	cache, err := scope.native.Cache(value)
	if err != nil {
		return nil, translate(err, "storage-cache")
	}
	return &StorageCache{native: cache, scope: scope}, nil
}

// RunStorageCacheCommand delegates finite native entry reads/deletions or cache
// deletion using immutable provenance. Parameters remain borrowed until return.
// A positive DeleteCache invalidates this scope's observed aliases. Another
// browser controller can concurrently change or recreate the same named data;
// this is not an exclusive lock, snapshot or transactional cache revision.
func RunStorageCacheCommand[P, R any](ctx context.Context, cache *StorageCache, command cdp.Command[P, R], params P) (value R, receipt *adapters.Receipt[Result], err error) {
	if cache == nil || cache.native == nil || cache.scope == nil {
		return value, nil, fail(ErrInput, "storage-cache")
	}
	return runPageOperation(ctx, cache.scope.page, "storage_cache_command", func(ctx context.Context) (R, error) {
		return native.RunStorageCacheCommand(ctx, cache.native, command, params)
	})
}
