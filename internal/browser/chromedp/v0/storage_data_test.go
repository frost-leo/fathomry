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

	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/runtime"
)

func TestIndexedDBRemoteResultsAreCountedAndValidated(t *testing.T) {
	entry := &indexeddb.DataEntry{Key: &runtime.RemoteObject{}, PrimaryKey: &runtime.RemoteObject{}, Value: &runtime.RemoteObject{ObjectID: "observed"}}
	for _, test := range []struct {
		name    string
		entries []*indexeddb.DataEntry
		size    int
		want    error
	}{
		{"zero-native-page", nil, 0, nil},
		{"three-native-values", []*indexeddb.DataEntry{entry}, 1, nil},
		{"oversized-reply", []*indexeddb.DataEntry{entry, entry}, 1, ErrLimit},
		{"nil-entry", []*indexeddb.DataEntry{nil}, 1, ErrNative},
		{"missing-key", []*indexeddb.DataEntry{{PrimaryKey: entry.PrimaryKey, Value: entry.Value}}, 1, ErrNative},
		{"missing-value", []*indexeddb.DataEntry{{Key: entry.Key, PrimaryKey: entry.PrimaryKey}}, 1, ErrNative},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects, err := indexedDBObjects(indexeddb.RequestDataResult{ObjectStoreDataEntries: test.entries}, test.size)
			if !errors.Is(err, test.want) || err == nil && len(objects) != len(test.entries)*3 || len(objects) > test.size*3 {
				t.Fatal("malformed or over-budget native object identities retained", err)
			}
		})
	}
}
