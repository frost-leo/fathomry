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
	"encoding/json"
	"testing"

	"github.com/chromedp/cdproto/runtime"
)

func TestLiveRemoteValuePresenceInSnapshots(t *testing.T) {
	for _, fixture := range []struct {
		name, expression, value string
	}{
		{"empty-string", `""`, `""`},
		{"empty-array", `[]`, `[]`},
		{"empty-object", `({})`, `{}`},
		{"null", `null`, `null`},
		{"false-control", `false`, `false`},
		{"zero-control", `0`, `0`},
		{"nonempty-string-control", `"value"`, `"value"`},
		{"nested-empty-control", `({string:"",array:[],object:{},nil:null})`, `{"string":"","array":[],"object":{},"nil":null}`},
		{"undefined-control", `undefined`, ``},
		{"nan-control", `NaN`, ``},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			owner, retained, dependencies := objectFixture(t)
			value, receipt, err := RunAction(bounded(t), retained.Page(), Evaluate[*runtime.RemoteObject](fixture.expression, EvalAsValue()))
			if err != nil || value == nil {
				t.Fatal("native result missing", err)
			}
			if string(value.Value) != fixture.value {
				t.Fatalf("native control value=%s, expected=%s", value.Value, fixture.value)
			}
			object, err := retained.Page().Object(value)
			if err != nil {
				t.Fatal(err)
			}
			metadata := object.DataCopy()
			if err := object.Close(bounded(t)); err != nil {
				t.Fatal(err)
			}
			snapshot, err := receipt.WaitReleased(bounded(t))
			if err != nil || snapshot.Err() != nil {
				t.Fatal("receipt failed", err, snapshot.Err())
			}
			facts, ok := snapshot.ValueCopy()
			if !ok {
				t.Fatal("independent evidence missing")
			}
			for label, payload := range map[string][]byte{"object": metadata, "receipt": facts.PayloadCopy()} {
				var decoded runtime.RemoteObject
				if err := json.Unmarshal(payload, &decoded); err != nil {
					t.Fatal(err)
				}
				t.Logf("%s: native type=%q subtype=%q value=%q, retained=%s, decoded value=%q", label, value.Type, value.Subtype, value.Value, payload, decoded.Value)
				if string(decoded.Value) != string(value.Value) {
					t.Errorf("%s loses explicit native JSON value", label)
				}
				if decoded.Type != value.Type || decoded.Subtype != value.Subtype || decoded.UnserializableValue != value.UnserializableValue {
					t.Errorf("%s changes native type/subtype/special-value distinction", label)
				}
			}
			drain(t, dependencies, 1)
			if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
				t.Fatal("fixture cleanup incomplete", err)
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}

func TestLivePropertyValuePresenceInSnapshots(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<title>property presence</title>", 32)
	root, registration, err := EvaluateObject(bounded(t), retained.Page(), `({empty:"",nil:null,undefined:undefined,zero:0,no:false})`)
	if err != nil {
		t.Fatal(err)
	}
	properties, receipt, err := root.Properties(bounded(t), runtime.GetPropertiesParams{OwnProperties: new(true)})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Err() != nil {
		t.Fatal("receipt failed", err, snapshot.Err())
	}
	facts, ok := snapshot.ValueCopy()
	if !ok {
		t.Fatal("independent evidence missing")
	}
	var stored runtime.GetPropertiesResult
	if err := json.Unmarshal(facts.PayloadCopy(), &stored); err != nil {
		t.Fatal(err)
	}
	for index, property := range properties.Result {
		if property.Name == "__proto__" {
			continue
		}
		object, err := retained.Page().Object(property.Value)
		if err != nil {
			t.Fatal(err)
		}
		var metadata runtime.RemoteObject
		if err := json.Unmarshal(object.DataCopy(), &metadata); err != nil {
			t.Fatal(err)
		}
		retainedValue := stored.Result[index].Value
		t.Logf("property=%s, native value=%q type=%q subtype=%q, object=%s, evidence value=%q", property.Name, property.Value.Value, property.Value.Type, property.Value.Subtype, object.DataCopy(), retainedValue.Value)
		if string(metadata.Value) != string(property.Value.Value) || string(retainedValue.Value) != string(property.Value.Value) {
			t.Error("property metadata or independent evidence loses native value")
		}
	}
	drain(t, dependencies, 1)
	if err := root.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := registration.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("fixture cleanup incomplete", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
