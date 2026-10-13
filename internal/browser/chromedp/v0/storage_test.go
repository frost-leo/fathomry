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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chromedp/cdproto/cachestorage"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/domstorage"
	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/storage"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestBrowserLegacyProtocolCannotMutateUngrantableSharedStorage(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	if err := fixture.client.owner.start(deadline(t)); err != nil {
		t.Fatal(err)
	}
	targets, err := cdp.Call(deadline(t), browserExecutor(t, fixture), target.GetTargets, target.GetTargetsParams{})
	if err != nil {
		t.Fatal(err)
	}
	var initial target.ID
	for _, info := range targets.TargetInfos {
		if info.Type == "page" && info.URL == "about:blank" {
			initial = info.TargetID
			break
		}
	}
	if initial == "" {
		t.Fatal("owned fixture default page missing")
	}
	retained, err := fixture.client.Retain(deadline(t), fault.Correlation{Call: "legacy-storage-boundary"}, SessionOptionsV1{Borrow: true, DefaultContext: true, TargetID: initial})
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("fixture")) }))
	defer server.Close()
	session := retained.Page()
	if err := session.Navigate(deadline(t), server.URL); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Storage(server.URL); !errors.Is(err, ErrState) {
		t.Fatal("fixture unexpectedly had shared storage authority", err)
	}
	executor := sdk.FromContext(session.native).Target
	identity := &domstorage.StorageID{SecurityOrigin: server.URL, IsLocalStorage: true}
	for _, mode := range []string{"protocol", "command"} {
		t.Run(mode, func(t *testing.T) {
			if _, err := cdp.Call(deadline(t), executor, domstorage.SetDOMStorageItem, domstorage.SetDOMStorageItemParams{StorageID: identity, Key: "peer-fixture", Value: "retained"}); err != nil {
				t.Fatal(err)
			}
			params := domstorage.ClearParams{StorageID: identity}
			var err error
			if mode == "protocol" {
				err = session.Protocol(deadline(t), func(ctx context.Context, grant cdp.Session) error {
					_, err := cdp.Call(ctx, grant, domstorage.Clear, params)
					return err
				})
			} else {
				_, err = RunAction(deadline(t), session, Command(domstorage.Clear, params))
			}
			if !errors.Is(err, ErrUnsupported) {
				t.Error("generic protocol accepted an operation without its scoped grant", err)
			}
			values, err := cdp.Call(deadline(t), executor, domstorage.GetDOMStorageItems, domstorage.GetDOMStorageItemsParams{StorageID: identity})
			if err != nil || len(values.Entries) != 1 || values.Entries[0][1] != "retained" {
				t.Error("ungranted command changed the peer's fixture data", err)
			}
		})
	}
}

func TestStorageScopeRequiresContextAuthorityBeforeNativeWork(t *testing.T) {
	session := unitSession(t)
	session.browserID, session.borrowed = "borrowed", true
	if _, err := session.Storage("https://fixture.invalid"); !errors.Is(err, ErrState) || session.data.commands != 0 {
		t.Fatal("borrowed storage authority inferred", err)
	}
	session.sharedStorage = true
	scope, err := session.Storage("https://fixture.invalid")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{
		&indexeddb.RequestDatabaseNamesParams{SecurityOrigin: "https://foreign.invalid"},
		&indexeddb.GetMetadataParams{StorageKey: "foreign"},
		&cachestorage.RequestCacheNamesParams{StorageBucket: &storage.Bucket{StorageKey: "foreign"}},
		&domstorage.ClearParams{},
		&domstorage.SetDOMStorageItemParams{StorageID: &domstorage.StorageID{StorageKey: "foreign"}},
	} {
		if err := fillStorageOrigin(value, scope.origin); !errors.Is(err, ErrInput) {
			t.Fatal("foreign or absent scoped identity accepted", err)
		}
	}
	if _, err := RunStorageCommand(context.Background(), scope, storage.GetCookies, storage.GetCookiesParams{}); !errors.Is(err, ErrUnsupported) || session.data.commands != 0 {
		t.Fatal("origin scope acquired browser/default cookie authority", err)
	}
	fake := cdp.Command[indexeddb.RequestDatabaseNamesParams, cdp.Empty]{Method: indexeddb.RequestDatabaseNames.Method}
	if _, err := RunStorageCommand(context.Background(), scope, fake, indexeddb.RequestDatabaseNamesParams{}); !errors.Is(err, ErrInput) || session.data.commands != 0 {
		t.Fatal("mismatched native command type reached dispatch", err)
	}
	if _, err := RunStorageCommand(context.Background(), scope, indexeddb.RequestData, indexeddb.RequestDataParams{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("unmanaged object producer accepted", err)
	}
	session.cancel(context.Canceled)
	if _, err := session.Storage(scope.origin); !errors.Is(err, ErrState) {
		t.Fatal("closed storage grant renewed", err)
	}
}

func TestStorageNativeParametersAreCopiedBeforeIdentityFilling(t *testing.T) {
	session := unitSession(t)
	original := domstorage.SetDOMStorageItemParams{StorageID: &domstorage.StorageID{IsLocalStorage: true}, Key: "", Value: ""}
	copied, err := storageParams(session, originStorageCommands, domstorage.SetDOMStorageItem, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := fillStorageOrigin(&copied, "https://fixture.invalid"); err != nil {
		t.Fatal(err)
	}
	if original.StorageID.SecurityOrigin != "" || copied.StorageID == original.StorageID || copied.Key != "" || copied.Value != "" || !copied.StorageID.IsLocalStorage {
		t.Fatal("identity fill changed aliases or explicit zero values")
	}
}

func TestStorageCacheAuthorityRequiresOriginalPointer(t *testing.T) {
	session := unitSession(t)
	session.browserID = "owned"
	scope, err := session.Storage("https://fixture.invalid")
	if err != nil {
		t.Fatal(err)
	}
	value := &cachestorage.Cache{CacheID: "native", SecurityOrigin: scope.origin}
	state := &storageCacheState{scope: scope, id: value.CacheID}
	scope.caches[value] = state
	if _, err := scope.Cache(value); err != nil {
		t.Fatal(err)
	}
	copy := *value
	if _, err := scope.Cache(&copy); !errors.Is(err, ErrInput) {
		t.Fatal("copy became a cache grant", err)
	}
	value.CacheID = "foreign"
	if _, err := scope.Cache(value); !errors.Is(err, ErrInput) {
		t.Fatal("mutated cache identity accepted", err)
	}
	value.CacheID = "native"
	state.deleted = true
	if _, err := scope.Cache(value); !errors.Is(err, ErrInput) {
		t.Fatal("deleted cache grant revived", err)
	}
}
