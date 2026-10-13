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
	"encoding/json"
	"errors"
	"testing"

	"github.com/chromedp/cdproto/cachestorage"
	"github.com/chromedp/cdproto/domstorage"
	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/storage"
)

func TestLiveStoragePartitionAndNamedBucketsPreserveContextIsolation(t *testing.T) {
	owner, first, second, origin, dependencies := storageFixture(t)
	storageDataFixture(t, first.Page(), "first-default", dependencies)
	storageDataFixture(t, second.Page(), "second-default", dependencies)
	firstFrames, _, err := first.Page().Frames(bounded(t))
	if err != nil || len(firstFrames) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	scope, receipt, err := first.Page().StoragePartition(bounded(t), firstFrames[0])
	if err != nil || scope == nil || scope.StorageKey() == "" {
		t.Fatal("native storage key missing", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	var observed storage.GetStorageKeyResult
	if json.Unmarshal(evidence.PayloadCopy(), &observed) != nil || string(observed.StorageKey) != scope.StorageKey() {
		t.Fatal("storage-key evidence missing")
	}
	drain(t, dependencies, 1)
	frames, _, err := second.Page().Frames(bounded(t))
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := first.Page().StoragePartition(bounded(t), frames[0]); !errors.Is(err, ErrInput) {
		t.Fatal("foreign frame authorized storage acquisition", err)
	}
	drain(t, dependencies, 1)
	peer, _, err := second.Page().StoragePartition(bounded(t), frames[0])
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	input := domstorage.GetDOMStorageItemsParams{StorageID: &domstorage.StorageID{IsLocalStorage: true}}
	items, _, err := RunStorageCommand(bounded(t), scope, domstorage.GetDOMStorageItems, input)
	if err != nil || len(items.Entries) != 1 || items.Entries[0][1] != "first-default" || input.StorageID.StorageKey != "" {
		t.Fatal("partition storage scope or caller copy differs", err)
	}
	drain(t, dependencies, 1)
	for _, name := range []string{"chosen", "preserved"} {
		expression := `(async()=>{let b=await navigator.storageBuckets.open('` + name + `');await (await b.caches.open('cache')).put('/entry',new Response('` + name + `'));return true})()`
		if value, _, err := RunAction(bounded(t), first.Page(), Evaluate[bool](expression, EvalAwaitPromise())); err != nil || !value {
			t.Fatal("native named bucket setup failed", err)
		}
		drain(t, dependencies, 1)
	}
	bucket, err := scope.Bucket("chosen")
	if err != nil {
		t.Fatal(err)
	}
	names, _, err := RunStorageCommand(bounded(t), bucket, cachestorage.RequestCacheNames, cachestorage.RequestCacheNamesParams{})
	if err != nil || len(names.Caches) != 1 || names.Caches[0].StorageBucket == nil || names.Caches[0].StorageBucket.Name != "chosen" {
		t.Fatal("bucket names lost partition metadata", err)
	}
	drain(t, dependencies, 1)
	cache, err := bucket.Cache(names.Caches[0])
	if err != nil {
		t.Fatal(err)
	}
	result, _, err := RunStorageCacheCommand(bounded(t), cache, cachestorage.RequestCachedResponse, cachestorage.RequestCachedResponseParams{RequestURL: origin + "/entry", RequestHeaders: []*cachestorage.Header{}})
	if err != nil || result.Response == nil || string(result.Response.Body) != "chosen" {
		t.Fatal("named bucket read another bucket", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunStorageCommand(bounded(t), bucket, storage.ClearDataForStorageKey, storage.ClearDataForStorageKeyParams{StorageTypes: "all"}); !errors.Is(err, ErrInput) {
		t.Fatal("bucket grant cleared its parent partition", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunStorageCommand(bounded(t), bucket, storage.DeleteStorageBucket, storage.DeleteStorageBucketParams{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	keys, _, err := RunAction(bounded(t), first.Page(), Evaluate[[]string]("navigator.storageBuckets.keys()", EvalAwaitPromise()))
	if err != nil || len(keys) != 1 || keys[0] != "preserved" {
		t.Fatal("bucket deletion changed unrelated data", err)
	}
	drain(t, dependencies, 1)
	databases, _, err := RunStorageCommand(bounded(t), scope, indexeddb.RequestDatabaseNames, indexeddb.RequestDatabaseNamesParams{})
	if err != nil || len(databases.DatabaseNames) != 1 || databases.DatabaseNames[0] != "fixture" {
		t.Fatal("partition database selection differs", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunStorageCommand(bounded(t), scope, storage.ClearDataForStorageKey, storage.ClearDataForStorageKeyParams{StorageTypes: "local_storage"}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	peerItems, _, err := RunStorageCommand(bounded(t), peer, domstorage.GetDOMStorageItems, input)
	if err != nil || len(peerItems.Entries) != 1 || peerItems.Entries[0][1] != "second-default" {
		t.Fatal("serialized key silently granted another BrowserContext", err)
	}
	drain(t, dependencies, 1)
	if _, err := second.Page().Navigate(bounded(t), origin+"/next"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := second.Page().StoragePartition(bounded(t), frames[0]); !errors.Is(err, ErrState) {
		t.Fatal("retired document acquired a new partition", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunStorageCommand(bounded(t), peer, domstorage.GetDOMStorageItems, input); err != nil {
		t.Fatal("already-acquired stored data incorrectly followed document lifetime", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("storage partition grants kept a native owner")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
