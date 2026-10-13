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
	"math"
	"testing"

	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/runtime"
)

func TestLiveIndexedDBValuesRetainIndependentObjectCustody(t *testing.T) {
	owner, first, second, origin, dependencies := storageFixture(t)
	storageDataFixture(t, first.Page(), "stored-first", dependencies)
	storageDataFixture(t, second.Page(), "stored-second", dependencies)
	scope, err := first.Page().Storage(origin)
	if err != nil {
		t.Fatal(err)
	}
	group, _, err := first.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	params := indexeddb.RequestDataParams{DatabaseName: "fixture", ObjectStoreName: "values", PageSize: 1}
	value, receipt, err := group.ReadIndexedDB(bounded(t), scope, params)
	if err != nil || len(value.ObjectStoreDataEntries) != 1 || value.HasMore || params.SecurityOrigin != "" {
		t.Fatal("native paging or origin copy differs", err)
	}
	entry := value.ObjectStoreDataEntries[0]
	object, err := first.Page().Object(entry.Value)
	if err != nil {
		t.Fatal("native database value lost managed identity", err)
	}
	copy := *entry.Value
	if _, err := first.Page().Object(&copy); !errors.Is(err, ErrInput) {
		t.Fatal("copied native result became a grant", err)
	}
	if _, err := second.Page().Object(entry.Value); !errors.Is(err, ErrInput) {
		t.Fatal("another page adopted database result authority", err)
	}
	entry.Value.Description = "caller changed description"
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	var recorded indexeddb.RequestDataResult
	if json.Unmarshal(evidence.PayloadCopy(), &recorded) != nil || len(recorded.ObjectStoreDataEntries) != 1 || recorded.ObjectStoreDataEntries[0].Value.Description == entry.Value.Description {
		t.Fatal("caller mutation changed database result evidence")
	}
	drain(t, dependencies, 1)
	properties, _, err := object.Properties(bounded(t), runtime.GetPropertiesParams{OwnProperties: new(true)})
	if err != nil || len(properties.Result) != 1 || properties.Result[0].Name != "answer" || string(properties.Result[0].Value.Value) != "42" {
		t.Fatal("native database value cannot be inspected", err)
	}
	drain(t, dependencies, 1)
	other, _, err := first.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	otherData, _, err := other.ReadIndexedDB(bounded(t), scope, params)
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	otherObject, err := first.Page().Object(otherData.ObjectStoreDataEntries[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Close(bounded(t)); err != nil || !group.Closed() || !object.Closed() {
		t.Fatal("database object group did not release", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := otherObject.Properties(bounded(t), runtime.GetPropertiesParams{OwnProperties: new(true)}); err != nil {
		t.Fatal("closing one group released the shared native indexeddb group", err)
	}
	drain(t, dependencies, 1)
	metadata, _, err := RunStorageCommand(bounded(t), scope, indexeddb.GetMetadata, indexeddb.GetMetadataParams{DatabaseName: "fixture", ObjectStoreName: "values"})
	if err != nil || metadata.EntriesCount != 1 {
		t.Fatal("remote-reference cleanup deleted persistent records", err)
	}
	drain(t, dependencies, 1)
	if err := other.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("database remote objects prevented shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveIndexedDBPagingBudgetsAndScopedNativeSelection(t *testing.T) {
	owner, first, second, origin, dependencies := storageFixture(t)
	storageDataFixture(t, first.Page(), "stored", dependencies)
	scope, err := first.Page().Storage(origin)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := second.Page().Storage(origin)
	if err != nil {
		t.Fatal(err)
	}
	group, _, err := first.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	params := indexeddb.RequestDataParams{DatabaseName: "fixture", ObjectStoreName: "values", PageSize: 1}
	if _, receipt, err := group.ReadIndexedDB(bounded(t), foreign, params); !errors.Is(err, ErrInput) || receipt != nil {
		t.Fatal("foreign storage scope reached object admission", err)
	}
	for _, size := range []int64{-1, math.MaxInt64, 128} {
		params.PageSize = size
		if _, _, err := group.ReadIndexedDB(bounded(t), scope, params); !errors.Is(err, ErrLimit) {
			t.Fatal("native requested row count bypassed capacity", err)
		}
		drain(t, dependencies, 1)
	}
	params.PageSize = 0
	zero, _, err := group.ReadIndexedDB(bounded(t), scope, params)
	if err != nil || len(zero.ObjectStoreDataEntries) != 0 || !zero.HasMore {
		t.Fatal("explicit zero page semantics changed", err)
	}
	drain(t, dependencies, 1)
	params.PageSize, params.SkipCount = 1, 1
	skipped, _, err := group.ReadIndexedDB(bounded(t), scope, params)
	if err != nil || len(skipped.ObjectStoreDataEntries) != 0 || skipped.HasMore {
		t.Fatal("native skip/terminal metadata changed", err)
	}
	drain(t, dependencies, 1)
	params.SkipCount = 0
	params.SecurityOrigin = "https://foreign.invalid"
	if _, _, err := group.ReadIndexedDB(bounded(t), scope, params); !errors.Is(err, ErrInput) {
		t.Fatal("copied parameters changed storage origin", err)
	}
	drain(t, dependencies, 1)
	params.SecurityOrigin = ""
	frames, _, err := first.Page().Frames(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	partition, _, err := first.Page().StoragePartition(bounded(t), frames[0])
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	value, _, err := group.ReadIndexedDB(bounded(t), partition, params)
	if err != nil || len(value.ObjectStoreDataEntries) != 1 {
		t.Fatal("partition-backed value read failed or preflight leaked capacity", err)
	}
	drain(t, dependencies, 1)
	object, err := first.Page().Object(value.ObjectStoreDataEntries[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Page().Navigate(bounded(t), origin+"/next"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := object.Properties(bounded(t), runtime.GetPropertiesParams{}); !errors.Is(err, ErrState) {
		t.Fatal("database object survived document revocation", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("revoked database group could not clean up")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
