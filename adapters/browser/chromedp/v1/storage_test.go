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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/chromedp/cdproto/cachestorage"
	"github.com/chromedp/cdproto/domstorage"
	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/storage"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func storageFixture(t *testing.T) (*Owner, *Context, *Context, string, Dependencies) {
	t.Helper()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<h1>origin storage fixture</h1>"))
	}))
	t.Cleanup(server.Close)
	prepared, err := Prepare(Settings{Name: "origin-storage", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(2), MaxPages: new(2), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	contexts := make([]*Context, 2)
	for index := range contexts {
		retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
		if err != nil {
			t.Fatal(err)
		}
		contexts[index] = retained
		if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
			t.Fatal(err)
		}
		drain(t, dependencies, 1)
	}
	return owner, contexts[0], contexts[1], server.URL, dependencies
}

func storageDataFixture(t *testing.T, page *Page, label string, dependencies Dependencies) {
	t.Helper()
	expression := fmt.Sprintf(`(async () => {
		localStorage.setItem("fixture", %q);
		sessionStorage.setItem("fixture", %q);
		await (await caches.open("fixture")).put("/cached", new Response(%q));
		await new Promise((resolve,reject) => {
			const request = indexedDB.open("fixture", 1);
			request.onupgradeneeded = () => request.result.createObjectStore("values");
			request.onerror = () => reject(request.error);
			request.onsuccess = () => {
				const db = request.result, transaction = db.transaction("values","readwrite");
				transaction.objectStore("values").put({answer:42}, "key");
				transaction.oncomplete = () => {db.close(); resolve();};
				transaction.onerror = () => reject(transaction.error);
			};
		});
		return true;
	})()`, label, label, label)
	if result, _, err := RunAction(bounded(t), page, Evaluate[bool](expression, EvalAwaitPromise())); err != nil || !result {
		t.Fatal("synthetic storage setup failed", err)
	}
	drain(t, dependencies, 1)
}

func TestLiveOriginStorageCacheAndDatabaseIsolation(t *testing.T) {
	owner, first, second, origin, dependencies := storageFixture(t)
	storageDataFixture(t, first.Page(), "private-first-value", dependencies)
	storageDataFixture(t, second.Page(), "second-value", dependencies)
	scope, err := first.Page().Storage(origin)
	if err != nil {
		t.Fatal(err)
	}
	other, err := second.Page().Storage(origin)
	if err != nil {
		t.Fatal(err)
	}
	input := domstorage.GetDOMStorageItemsParams{StorageID: &domstorage.StorageID{IsLocalStorage: true}}
	items, receipt, err := RunStorageCommand(bounded(t), scope, domstorage.GetDOMStorageItems, input)
	if err != nil || len(items.Entries) != 1 || items.Entries[0][1] != "private-first-value" {
		t.Fatal("wrong native local storage", err)
	}
	if input.StorageID.SecurityOrigin != "" {
		t.Fatal("scope mutated caller identity")
	}
	items.Entries[0][1] = "mutated-copy"
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	if !bytes.Contains(evidence.PayloadCopy(), []byte("private-first-value")) || bytes.Contains([]byte(fmt.Sprintf("%+v", evidence)), []byte("private-first-value")) {
		t.Fatal("storage evidence aliases caller data or leaks ordinary diagnostics")
	}
	drain(t, dependencies, 1)
	names, _, err := RunStorageCommand(bounded(t), scope, cachestorage.RequestCacheNames, cachestorage.RequestCacheNamesParams{})
	if err != nil || len(names.Caches) != 1 {
		t.Fatal("native cache discovery failed", err)
	}
	drain(t, dependencies, 1)
	cache, err := scope.Cache(names.Caches[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Cache(names.Caches[0]); !errors.Is(err, ErrInput) {
		t.Fatal("foreign context adopted cache identity", err)
	}
	copied := *names.Caches[0]
	if _, err := scope.Cache(&copied); !errors.Is(err, ErrInput) {
		t.Fatal("copied cache data became authority", err)
	}
	nativeID := names.Caches[0].CacheID
	names.Caches[0].CacheID = "foreign"
	if _, err := scope.Cache(names.Caches[0]); !errors.Is(err, ErrInput) {
		t.Fatal("changed cache ID accepted", err)
	}
	params := cachestorage.RequestCachedResponseParams{RequestURL: origin + "/cached", RequestHeaders: []*cachestorage.Header{}}
	body, _, err := RunStorageCacheCommand(bounded(t), cache, cachestorage.RequestCachedResponse, params)
	if err != nil || body.Response == nil || string(body.Response.Body) != "private-first-value" || params.CacheID != "" {
		t.Fatal("cache grant lost immutable identity/native bytes or changed caller params", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunStorageCacheCommand(bounded(t), cache, cachestorage.RequestEntries, cachestorage.RequestEntriesParams{CacheID: "foreign"}); !errors.Is(err, ErrInput) {
		t.Fatal("cache accepted foreign parameters", err)
	}
	drain(t, dependencies, 1)
	names.Caches[0].CacheID = nativeID
	databases, _, err := RunStorageCommand(bounded(t), scope, indexeddb.RequestDatabaseNames, indexeddb.RequestDatabaseNamesParams{})
	if err != nil || len(databases.DatabaseNames) != 1 || databases.DatabaseNames[0] != "fixture" {
		t.Fatal("native databases missing", err)
	}
	drain(t, dependencies, 1)
	metadata, _, err := RunStorageCommand(bounded(t), scope, indexeddb.GetMetadata, indexeddb.GetMetadataParams{DatabaseName: "fixture", ObjectStoreName: "values"})
	if err != nil || metadata.EntriesCount != 1 {
		t.Fatal("native database metadata changed", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunStorageCommand(bounded(t), scope, storage.ClearDataForOrigin, storage.ClearDataForOriginParams{StorageTypes: "all"}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	firstItems, _, err := RunStorageCommand(bounded(t), scope, domstorage.GetDOMStorageItems, input)
	if err != nil || len(firstItems.Entries) != 0 {
		t.Fatal("scoped origin clear did not clear local data", err)
	}
	drain(t, dependencies, 1)
	peerItems, _, err := RunStorageCommand(bounded(t), other, domstorage.GetDOMStorageItems, input)
	if err != nil || len(peerItems.Entries) != 1 || peerItems.Entries[0][1] != "second-value" {
		t.Fatal("origin clear crossed BrowserContext boundary", err)
	}
	drain(t, dependencies, 1)
	peerCaches, _, err := RunStorageCommand(bounded(t), other, cachestorage.RequestCacheNames, cachestorage.RequestCacheNamesParams{})
	if err != nil || len(peerCaches.Caches) != 1 {
		t.Fatal("origin clear erased peer cache", err)
	}
	drain(t, dependencies, 1)
	peerCache, err := other.Cache(peerCaches.Caches[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunStorageCacheCommand(bounded(t), peerCache, cachestorage.DeleteCache, cachestorage.DeleteCacheParams{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunStorageCacheCommand(bounded(t), peerCache, cachestorage.RequestEntries, cachestorage.RequestEntriesParams{}); !errors.Is(err, ErrState) {
		t.Fatal("deleted alias revived", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("origin data operations retained a native owner")
	}
	if _, _, err := RunStorageCommand(bounded(t), scope, domstorage.GetDOMStorageItems, input); !errors.Is(err, adapters.ErrClosed) {
		t.Fatal("closed source retained storage authority", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveSessionStorageUsesItsPageAndExplicitZeroValues(t *testing.T) {
	owner, first, _, origin, dependencies := storageFixture(t)
	scope, err := first.Page().Storage(origin)
	if err != nil {
		t.Fatal(err)
	}
	input := domstorage.SetDOMStorageItemParams{StorageID: &domstorage.StorageID{IsLocalStorage: false}, Key: "", Value: ""}
	if _, _, err := RunStorageCommand(bounded(t), scope, domstorage.SetDOMStorageItem, input); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	page, _, err := first.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, err := page.Navigate(bounded(t), origin); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	other, err := page.Storage(origin)
	if err != nil {
		t.Fatal(err)
	}
	otherItems, _, err := RunStorageCommand(bounded(t), other, domstorage.GetDOMStorageItems, domstorage.GetDOMStorageItemsParams{StorageID: &domstorage.StorageID{}})
	if err != nil || len(otherItems.Entries) != 0 {
		t.Fatal("session storage leaked across pages", err)
	}
	drain(t, dependencies, 1)
	items, _, err := RunStorageCommand(bounded(t), scope, domstorage.GetDOMStorageItems, domstorage.GetDOMStorageItemsParams{StorageID: &domstorage.StorageID{}})
	if err != nil || len(items.Entries) != 1 || len(items.Entries[0]) != 2 || items.Entries[0][0] != "" || items.Entries[0][1] != "" {
		t.Fatal("native empty key/value or explicit session selection changed", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("session storage prevented shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedOriginStorageRequiresGrantAndPreservesPeerData(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<h1>shared storage fixture</h1>"))
	}))
	defer server.Close()
	if err := sdk.Do(peer, sdk.Navigate(server.URL)); err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Storage(server.URL); !errors.Is(err, ErrState) {
		t.Fatal("borrowed context silently acquired origin data", err)
	}
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	granted, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(sdk.FromContext(peer).Target.TargetID), SharedStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := granted.Page().Storage(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunStorageCommand(bounded(t), scope, domstorage.SetDOMStorageItem, domstorage.SetDOMStorageItemParams{StorageID: &domstorage.StorageID{IsLocalStorage: true}, Key: "shared", Value: "retained"}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("borrowed storage did not detach", err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[string]("localStorage.getItem('shared')")); err != nil || value != "retained" {
		t.Fatal("provider cleanup deleted peer data or killed its page", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
