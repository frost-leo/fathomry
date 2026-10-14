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
	"errors"
	"testing"

	"github.com/chromedp/cdproto/cachestorage"
	"github.com/chromedp/cdproto/domstorage"
	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/storage"
)

func TestStoragePartitionSelectionCannotWidenOrMutateCallerData(t *testing.T) {
	session := unitSession(t)
	scope := &StorageScope{session: session, key: "https://frame.invalid/^0https://top.invalid"}
	bucket, err := scope.Bucket("explicit")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		scope  *StorageScope
		params any
	}{
		{scope, &cachestorage.RequestCacheNamesParams{SecurityOrigin: "https://frame.invalid"}},
		{scope, &indexeddb.RequestDatabaseNamesParams{StorageKey: "foreign"}},
		{scope, &indexeddb.RequestDatabaseNamesParams{StorageBucket: &storage.Bucket{StorageKey: storage.SerializedStorageKey(scope.key)}}},
		{scope, &storage.GetUsageAndQuotaParams{}},
		{scope, &storage.DeleteStorageBucketParams{}},
		{bucket, &storage.ClearDataForStorageKeyParams{}},
		{bucket, &domstorage.ClearParams{StorageID: &domstorage.StorageID{IsLocalStorage: true}}},
		{bucket, &indexeddb.RequestDatabaseNamesParams{StorageBucket: &storage.Bucket{StorageKey: storage.SerializedStorageKey(scope.key), Name: "other"}}},
		{bucket, &indexeddb.RequestDatabaseNamesParams{StorageKey: scope.key}},
	} {
		if err := test.scope.fillParams(test.params); !errors.Is(err, ErrInput) {
			t.Fatal("storage selection widened its grant", err)
		}
	}
	params := indexeddb.RequestDatabaseNamesParams{}
	copied, err := storageParams(session, originStorageCommands, indexeddb.RequestDatabaseNames, params)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.fillParams(&copied); err != nil || copied.StorageBucket == nil || copied.StorageBucket.Name != "explicit" || copied.StorageBucket.StorageKey != storage.SerializedStorageKey(scope.key) {
		t.Fatal("native bucket selection missing", err)
	}
	copied.StorageBucket.Name = "caller-mutated"
	if params.StorageBucket != nil || bucket.bucket.Name != "explicit" {
		t.Fatal("copied native parameters altered caller or grant identity")
	}
	dom := domstorage.GetDOMStorageItemsParams{StorageID: &domstorage.StorageID{IsLocalStorage: false}}
	if err := scope.fillParams(&dom); err != nil || string(dom.StorageID.StorageKey) != scope.key || dom.StorageID.SecurityOrigin != "" || dom.StorageID.IsLocalStorage {
		t.Fatal("native explicit session-storage or key changed", err)
	}
	if _, err := bucket.Bucket("other"); !errors.Is(err, ErrInput) {
		t.Fatal("bucket scope selected a sibling", err)
	}
}

func TestStoragePartitionCacheResultsRequireExactNativeSelection(t *testing.T) {
	scope := &StorageScope{session: unitSession(t), key: "native-key"}
	bucket, err := scope.Bucket("one")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []*cachestorage.Cache{
		nil,
		{CacheID: "one", StorageKey: "foreign"},
		{CacheID: "one", StorageKey: "native-key"},
		{CacheID: "one", StorageKey: "native-key", StorageBucket: &storage.Bucket{StorageKey: "foreign", Name: "one"}},
		{CacheID: "one", StorageKey: "native-key", StorageBucket: &storage.Bucket{StorageKey: "native-key", Name: "two"}},
	} {
		if bucket.cacheOrigin(value) {
			t.Fatal("foreign cache result acquired bucket authority")
		}
	}
	valid := &cachestorage.Cache{CacheID: "one", StorageKey: "native-key", StorageBucket: &storage.Bucket{StorageKey: "native-key", Name: "one"}}
	if !bucket.cacheOrigin(valid) || scope.cacheOrigin(valid) {
		t.Fatal("named and default bucket selections were collapsed")
	}
	valid.StorageBucket.Name = ""
	if !scope.cacheOrigin(valid) {
		t.Fatal("native default bucket was not recognized")
	}
}
