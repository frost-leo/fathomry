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
	"reflect"
	"strings"
	"sync"

	"github.com/chromedp/cdproto/cachestorage"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/domstorage"
	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/storage"
	sdk "github.com/chromedp/chromedp"
)

// StorageScope selects an explicit security origin within this page's verified
// BrowserContext. It is not a browser-global origin grant. Session storage refers
// to this page, while local storage and named caches are shared in its context.
type StorageScope struct {
	private
	session *Session
	origin  string
	key     string
	bucket  *storage.Bucket
	mu      sync.Mutex
	caches  map[*cachestorage.Cache]*storageCacheState
}

type StorageCache struct {
	private
	state *storageCacheState
}

type storageCacheState struct {
	scope   *StorageScope
	id      cachestorage.CacheID
	deleted bool
}

// Storage performs no native operation. Origin must be an explicit, nonempty
// native security origin. There is no URL normalization or default-origin fallback.
func (session *Session) Storage(origin string) (*StorageScope, error) {
	if session == nil || session.sessionState == nil || session.owner == nil {
		return nil, failure(ErrInput, "storage")
	}
	if _, err := session.storageContext(); err != nil {
		return nil, err
	}
	if origin == "" || origin == "null" || strings.ContainsAny(origin, "\x00\r\n") || int64(len(origin)) > session.owner.settings.MaxCommandBytes {
		return nil, failure(ErrInput, "storage-origin")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.work == nil || session.work.Err() != nil || session.native == nil {
		return nil, failure(ErrState, "storage")
	}
	return &StorageScope{session: session, origin: origin, caches: make(map[*cachestorage.Cache]*storageCacheState)}, nil
}

func storageParams[P, R any](session *Session, allowed map[string]bool, command cdp.Command[P, R], params P) (copied P, err error) {
	if !allowed[command.Method] {
		return copied, failure(ErrUnsupported, "storage-command")
	}
	schema := protocolCommands[command.Method]
	if !nativeCommandType(reflect.TypeFor[P](), schema.params) || !nativeCommandType(reflect.TypeFor[R](), schema.result) {
		return copied, failure(ErrInput, "storage-command-types")
	}
	if err := session.reserveCommand(); err != nil {
		return copied, err
	}
	buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
	if err := marshalBounded(&buffer, params); err != nil {
		return copied, err
	}
	err = jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions)
	return copied, err
}

// RunStorageCommand accepts finite origin-scoped native operations, not arbitrary
// protocol resource IDs, producer controls or remote-object-producing results.
func RunStorageCommand[P, R any](ctx context.Context, scope *StorageScope, command cdp.Command[P, R], params P) (result R, err error) {
	if scope == nil || scope.session == nil {
		return result, failure(ErrInput, "storage")
	}
	return RunAction(ctx, scope.session, Action[R]{local: func(work context.Context, session *Session) (result R, err error) {
		if _, err := session.storageContext(); err != nil {
			return result, err
		}
		copied, err := storageParams(session, originStorageCommands, command, params)
		if err != nil {
			return result, err
		}
		if err := scope.fillParams(any(&copied)); err != nil {
			return result, err
		}
		result, err = cdp.Call(session.commandWork(work, false), sdk.FromContext(session.native).Target, command, copied)
		if err == nil {
			if names, ok := any(result).(cachestorage.RequestCacheNamesResult); ok {
				for _, cache := range names.Caches {
					if !scope.cacheOrigin(cache) {
						var zero R
						return zero, failure(ErrNative, "storage-cache-origin")
					}
				}
				scope.mu.Lock()
				for _, cache := range names.Caches {
					scope.caches[cache] = &storageCacheState{scope: scope, id: cache.CacheID}
				}
				scope.mu.Unlock()
			}
		}
		return result, err
	}})
}

// Cache derives authority only from an exact result pointer issued by this
// scope. Cache metadata can be changed by the caller, but identity cannot.
func (scope *StorageScope) Cache(value *cachestorage.Cache) (*StorageCache, error) {
	if scope == nil || scope.session == nil || value == nil {
		return nil, failure(ErrInput, "storage-cache")
	}
	scope.mu.Lock()
	state := scope.caches[value]
	if state == nil || value.CacheID != state.id || state.deleted {
		scope.mu.Unlock()
		return nil, failure(ErrInput, "storage-cache-origin")
	}
	scope.mu.Unlock()
	if scope.session.work.Err() != nil {
		return nil, failure(ErrState, "storage-cache")
	}
	return &StorageCache{state: state}, nil
}

// RunStorageCacheCommand copies parameters and fills only this cache's frozen
// identity. Cache IDs identify native named data, not an exclusive revision.
func RunStorageCacheCommand[P, R any](ctx context.Context, cache *StorageCache, command cdp.Command[P, R], params P) (result R, err error) {
	if cache == nil || cache.state == nil {
		return result, failure(ErrInput, "storage-cache")
	}
	state, scope := cache.state, cache.state.scope
	return RunAction(ctx, scope.session, Action[R]{local: func(work context.Context, session *Session) (result R, err error) {
		if _, err := session.storageContext(); err != nil {
			return result, err
		}
		scope.mu.Lock()
		deleted := state.deleted
		scope.mu.Unlock()
		if deleted {
			return result, failure(ErrState, "storage-cache-deleted")
		}
		copied, err := storageParams(session, cacheStorageCommands, command, params)
		if err != nil {
			return result, err
		}
		var identity *cachestorage.CacheID
		switch value := any(&copied).(type) {
		case *cachestorage.DeleteCacheParams:
			identity = &value.CacheID
		case *cachestorage.DeleteEntryParams:
			identity = &value.CacheID
		case *cachestorage.RequestEntriesParams:
			identity = &value.CacheID
		case *cachestorage.RequestCachedResponseParams:
			identity = &value.CacheID
		}
		if identity == nil || *identity != "" && *identity != state.id {
			return result, failure(ErrInput, "storage-cache-origin")
		}
		*identity = state.id
		result, err = cdp.Call(session.commandWork(work, false), sdk.FromContext(session.native).Target, command, copied)
		if err == nil && command.Method == cachestorage.DeleteCache.Method {
			scope.mu.Lock()
			for _, entry := range scope.caches {
				if entry.id == state.id {
					entry.deleted = true
				}
			}
			scope.mu.Unlock()
		}
		return result, err
	}})
}

func fillStorageOrigin(params any, origin string) error {
	return (&StorageScope{origin: origin}).fillParams(params)
}

func (scope *StorageScope) fillParams(params any) error {
	fill := scope.fillSelection
	domID := func(id *domstorage.StorageID) error {
		if id == nil {
			return failure(ErrInput, "dom-storage-id")
		}
		key := string(id.StorageKey)
		if err := fill(&id.SecurityOrigin, &key, nil); err != nil {
			return err
		}
		id.StorageKey = domstorage.SerializedStorageKey(key)
		return nil
	}
	switch value := params.(type) {
	case *domstorage.ClearParams:
		return domID(value.StorageID)
	case *domstorage.GetDOMStorageItemsParams:
		return domID(value.StorageID)
	case *domstorage.RemoveDOMStorageItemParams:
		return domID(value.StorageID)
	case *domstorage.SetDOMStorageItemParams:
		return domID(value.StorageID)
	case *cachestorage.RequestCacheNamesParams:
		return fill(&value.SecurityOrigin, &value.StorageKey, &value.StorageBucket)
	case *indexeddb.ClearObjectStoreParams:
		return fill(&value.SecurityOrigin, &value.StorageKey, &value.StorageBucket)
	case *indexeddb.DeleteDatabaseParams:
		return fill(&value.SecurityOrigin, &value.StorageKey, &value.StorageBucket)
	case *indexeddb.DeleteObjectStoreEntriesParams:
		return fill(&value.SecurityOrigin, &value.StorageKey, &value.StorageBucket)
	case *indexeddb.GetMetadataParams:
		return fill(&value.SecurityOrigin, &value.StorageKey, &value.StorageBucket)
	case *indexeddb.RequestDatabaseParams:
		return fill(&value.SecurityOrigin, &value.StorageKey, &value.StorageBucket)
	case *indexeddb.RequestDatabaseNamesParams:
		return fill(&value.SecurityOrigin, &value.StorageKey, &value.StorageBucket)
	case *indexeddb.RequestDataParams:
		return fill(&value.SecurityOrigin, &value.StorageKey, &value.StorageBucket)
	case *storage.ClearDataForOriginParams:
		return fill(&value.Origin, nil, nil)
	case *storage.GetUsageAndQuotaParams:
		return fill(&value.Origin, nil, nil)
	case *storage.ClearDataForStorageKeyParams:
		return fill(nil, &value.StorageKey, nil)
	case *storage.DeleteStorageBucketParams:
		return fill(nil, nil, &value.Bucket)
	}
	return failure(ErrUnsupported, "storage-command")
}

var originStorageCommands = map[string]bool{
	"DOMStorage.clear":                   true,
	"DOMStorage.getDOMStorageItems":      true,
	"DOMStorage.removeDOMStorageItem":    true,
	"DOMStorage.setDOMStorageItem":       true,
	"CacheStorage.requestCacheNames":     true,
	"IndexedDB.clearObjectStore":         true,
	"IndexedDB.deleteDatabase":           true,
	"IndexedDB.deleteObjectStoreEntries": true,
	"IndexedDB.getMetadata":              true,
	"IndexedDB.requestDatabase":          true,
	"IndexedDB.requestDatabaseNames":     true,
	"Storage.clearDataForOrigin":         true,
	"Storage.getUsageAndQuota":           true,
	"Storage.clearDataForStorageKey":     true,
	"Storage.deleteStorageBucket":        true,
}
var cacheStorageCommands = map[string]bool{
	"CacheStorage.deleteCache":           true,
	"CacheStorage.deleteEntry":           true,
	"CacheStorage.requestCachedResponse": true,
	"CacheStorage.requestEntries":        true,
}
