/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package chromedp

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/runtime"
)

func TestNativeSnapshotPreservesRemoteValuesRecursivelyAndBoundsBytes(t *testing.T) {
	for _, raw := range []string{`""`, `[]`, `{}`, `null`, `false`, `0`, `"nonempty"`, ``} {
		t.Run(raw, func(t *testing.T) {
			remote := &runtime.RemoteObject{Type: runtime.RemoteObjectTypeObject}
			if raw != "" {
				remote.Value = jsonv2.Value(raw)
			}
			original := append([]byte(nil), remote.Value...)
			for name, fixture := range map[string]any{"pointer": remote, "value": *remote} {
				buffer := boundedBuffer{limit: 4096}
				if err := WriteNativeResult(&buffer, fixture); err != nil {
					t.Fatal(err)
				}
				var saved map[string]json.RawMessage
				if err := json.Unmarshal(buffer.data, &saved); err != nil {
					t.Fatal(err)
				}
				if string(saved["value"]) != raw {
					t.Fatal(name, "native value presence changed")
				}
			}
			properties := runtime.GetPropertiesResult{Result: []*runtime.PropertyDescriptor{{Name: "test", Value: remote}}}
			buffer := boundedBuffer{limit: 4096}
			if err := WriteNativeResult(&buffer, properties); err != nil {
				t.Fatal(err)
			}
			var saved runtime.GetPropertiesResult
			if err := json.Unmarshal(buffer.data, &saved); err != nil || string(saved.Result[0].Value.Value) != raw {
				t.Fatal("nested property value lost", err)
			}
			data := indexeddb.RequestDataResult{ObjectStoreDataEntries: []*indexeddb.DataEntry{{Key: remote, PrimaryKey: remote, Value: remote}}}
			buffer = boundedBuffer{limit: 4096}
			if err := WriteNativeResult(&buffer, data); err != nil {
				t.Fatal(err)
			}
			var database indexeddb.RequestDataResult
			if err := json.Unmarshal(buffer.data, &database); err != nil {
				t.Fatal(err)
			}
			entry := database.ObjectStoreDataEntries[0]
			if string(entry.Key.Value) != raw || string(entry.PrimaryKey.Value) != raw || string(entry.Value.Value) != raw {
				t.Fatal("nested IndexedDB value lost")
			}
			if !bytes.Equal(remote.Value, original) {
				t.Fatal("snapshot mutated native result")
			}
			buffer = boundedBuffer{limit: 1}
			if err := WriteNativeResult(&buffer, properties); err == nil || len(buffer.data) > 1 {
				t.Fatal("snapshot ignored byte bound")
			}
		})
	}
}

func TestNativeSnapshotDoesNotChangeParameterEncoding(t *testing.T) {
	remote := &runtime.RemoteObject{Type: runtime.RemoteObjectTypeObject, Value: jsonv2.Value(`[]`)}
	before, after := boundedBuffer{limit: 4096}, boundedBuffer{limit: 4096}
	if err := marshalBounded(&before, remote); err != nil {
		t.Fatal(err)
	}
	var saved boundedBuffer
	saved.limit = 4096
	if err := WriteNativeResult(&saved, remote); err != nil {
		t.Fatal(err)
	}
	if err := marshalBounded(&after, remote); err != nil || !bytes.Equal(before.data, after.data) {
		t.Fatal("snapshot changed ordinary parameter codecs", err)
	}
	if !bytes.Contains(saved.data, []byte(`"value":[]`)) || bytes.Contains(before.data, []byte(`"value"`)) {
		t.Fatal("output-only correction was not exercised")
	}
	if err := WriteNativeResult(nil, remote); !errors.Is(err, ErrInput) {
		t.Fatal("nil writer accepted", err)
	}
}
