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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chromedp/cdproto/cachestorage"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/storage"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestNativeStorageKeyAndNamedBucketIdentity(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<title>native storage partition</title>"))
	}))
	defer server.Close()
	retained, err := fixture.client.Retain(deadline(t), fault.Correlation{Call: "native-storage-key"}, SessionOptionsV1{ManagedEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close(context.Background())
	session := retained.Page()
	if err := session.Navigate(deadline(t), server.URL); err != nil {
		t.Fatal(err)
	}
	native := sdk.FromContext(session.native).Target
	tree, err := cdp.Call(deadline(t), native, page.GetFrameTree, cdp.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := cdp.Call(deadline(t), native, storage.GetStorageKey, storage.GetStorageKeyParams{FrameID: tree.FrameTree.Frame.ID})
	if err != nil || key.StorageKey == "" {
		t.Fatal("selected native storage-key route unavailable", err)
	}
	if _, err := sdk.Run(nativeContext{Context: deadline(t), native: session.native}, sdk.Evaluate[bool](`(async()=>{let b=await navigator.storageBuckets.open('named');await (await b.caches.open('bucket-cache')).put('/entry',new Response('bucket-body'));return true})()`, sdk.EvalAwaitPromise)); err != nil {
		t.Fatal("native bucket setup unavailable", err)
	}
	result, err := cdp.Call(deadline(t), native, cachestorage.RequestCacheNames, cachestorage.RequestCacheNamesParams{StorageBucket: &storage.Bucket{StorageKey: key.StorageKey, Name: "named"}})
	if err != nil || len(result.Caches) != 1 || result.Caches[0].StorageKey != string(key.StorageKey) || result.Caches[0].StorageBucket == nil || result.Caches[0].StorageBucket.Name != "named" {
		t.Fatal("native bucket result did not retain partition identity", err)
	}
	t.Log("selected GetStorageKey and named-bucket CacheStorage return matching native partition identities")
}
