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
	"strings"

	"github.com/chromedp/cdproto/cachestorage"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/storage"
	sdk "github.com/chromedp/chromedp"
)

// StoragePartition observes this target's native storage key, or the key of an
// explicit frame grant. Nil preserves the native target-only choice, which some
// browser versions support only for workers. It never selects a browser default.
// The observed partition can outlive its document but not its original Session.
func (session *Session) StoragePartition(ctx context.Context, frame *Frame) (scope *StorageScope, err error) {
	_, err = RunAction(ctx, session, Action[storage.GetStorageKeyResult]{local: func(work context.Context, session *Session) (storage.GetStorageKeyResult, error) {
		var zero storage.GetStorageKeyResult
		if _, err := session.storageContext(); err != nil {
			return zero, err
		}
		if frame != nil {
			if err := frame.valid(session); err != nil {
				return zero, err
			}
		}
		if err := session.startObjectOrigin(); err != nil {
			return zero, err
		}
		epoch := session.objectOrigin.epoch.Load()
		check := func() error {
			if frame != nil {
				return frame.valid(session)
			}
			if session.objectOrigin.epoch.Load() != epoch {
				return failure(ErrState, "storage-partition-document")
			}
			return nil
		}
		if err := check(); err != nil {
			return zero, err
		}
		var id cdp.FrameID
		if frame != nil {
			id = frame.id
		}
		result, err := cdp.Call(session.commandWork(work, true, check), sdk.FromContext(session.native).Target, storage.GetStorageKey, storage.GetStorageKeyParams{FrameID: id})
		if err != nil {
			return result, err
		}
		if err := check(); err != nil {
			return zero, err
		}
		if result.StorageKey == "" || int64(len(result.StorageKey)) > session.owner.settings.MaxCommandBytes || strings.ContainsAny(string(result.StorageKey), "\x00\r\n") {
			return zero, failure(ErrNative, "storage-partition-key")
		}
		scope = &StorageScope{session: session, key: string(result.StorageKey), caches: make(map[*cachestorage.Cache]*storageCacheState)}
		return result, nil
	}})
	if err != nil {
		return nil, err
	}
	return scope, nil
}

// StorageKey returns sensitive native metadata, not authority. An origin-only
// scope has no observed storage key and returns the empty string.
func (scope *StorageScope) StorageKey() string {
	if scope == nil {
		return ""
	}
	return scope.key
}

// Bucket narrows an observed partition to a caller-selected native bucket name.
// Empty selects the default bucket. It creates no native bucket and owns no
// persistent data. A bucket scope cannot select another bucket or its parent.
func (scope *StorageScope) Bucket(name string) (*StorageScope, error) {
	if scope == nil || scope.session == nil || scope.key == "" || scope.bucket != nil {
		return nil, failure(ErrInput, "storage-bucket-scope")
	}
	if strings.ContainsAny(name, "\x00\r\n") || int64(len(name)) > scope.session.owner.settings.MaxCommandBytes-int64(len(scope.key)) {
		return nil, failure(ErrInput, "storage-bucket-name")
	}
	if scope.session.work.Err() != nil {
		return nil, failure(ErrState, "storage-bucket")
	}
	return &StorageScope{session: scope.session, key: scope.key, bucket: &storage.Bucket{StorageKey: storage.SerializedStorageKey(scope.key), Name: name}, caches: make(map[*cachestorage.Cache]*storageCacheState)}, nil
}

func (scope *StorageScope) fillSelection(origin, key *string, bucket **storage.Bucket) error {
	if origin != nil && *origin != "" && *origin != scope.origin || key != nil && *key != "" && *key != scope.key {
		return failure(ErrInput, "storage-selection")
	}
	if bucket != nil && *bucket != nil && (scope.bucket == nil || **bucket != *scope.bucket) {
		return failure(ErrInput, "storage-bucket-selection")
	}
	switch {
	case scope.bucket != nil:
		if bucket == nil || origin != nil && *origin != "" || key != nil && *key != "" {
			return failure(ErrInput, "storage-bucket-required")
		}
		copied := *scope.bucket
		*bucket = &copied
	case scope.key != "":
		if key == nil || origin != nil && *origin != "" {
			return failure(ErrInput, "storage-partition-required")
		}
		*key = scope.key
	case scope.origin != "":
		if origin == nil || key != nil && *key != "" {
			return failure(ErrInput, "storage-origin-required")
		}
		*origin = scope.origin
	default:
		return failure(ErrInput, "storage-selection")
	}
	return nil
}

func (scope *StorageScope) cacheOrigin(cache *cachestorage.Cache) bool {
	if cache == nil || cache.CacheID == "" {
		return false
	}
	if scope.key == "" {
		return cache.SecurityOrigin == scope.origin
	}
	if cache.StorageKey != scope.key {
		return false
	}
	if scope.bucket != nil {
		return cache.StorageBucket != nil && *cache.StorageBucket == *scope.bucket
	}
	return cache.StorageBucket == nil || cache.StorageBucket.StorageKey == storage.SerializedStorageKey(scope.key) && cache.StorageBucket.Name == ""
}
