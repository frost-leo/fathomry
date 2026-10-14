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
	"encoding/json"
	"errors"
	"testing"

	"github.com/chromedp/cdproto/runtime"
)

func TestLiveObjectPropertiesPreserveDescriptorsAndReferenceAuthority(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>property fixture</p>", 32)
	root, registration, err := EvaluateObject(bounded(t), retained.Page(), `Object.assign(new class {
		#secret = {privateValue: 3};
		get privateValue() { return this.#secret; }
	}(), {child:{answer:42}, [Symbol("fixture")]:{symbolValue:7}})`)
	if err != nil {
		t.Fatal(err)
	}
	params := runtime.GetPropertiesParams{OwnProperties: new(true), GeneratePreview: new(true)}
	properties, receipt, err := root.Properties(bounded(t), params)
	if err != nil {
		t.Fatal(err)
	}
	if params.ObjectID != "" {
		t.Fatal("property parameters mutated")
	}
	var child *Object
	var symbol, private bool
	for _, property := range properties.Result {
		if property.Symbol != nil {
			if _, err := retained.Page().Object(property.Symbol); err != nil {
				t.Fatal("symbol lost custody", err)
			}
			symbol = true
		}
		if property.Name == "child" {
			child, err = retained.Page().Object(property.Value)
			if err != nil {
				t.Fatal(err)
			}
			copied := *property.Value
			if _, err := retained.Page().Object(&copied); !errors.Is(err, ErrInput) {
				t.Fatal("copied descriptor became authority", err)
			}
			original := property.Value.ObjectID
			property.Value.ObjectID = "foreign"
			if _, err := retained.Page().Object(property.Value); !errors.Is(err, ErrInput) {
				t.Fatal("mutable descriptor changed native grant", err)
			}
			property.Value.ObjectID = original
		}
	}
	for _, property := range properties.PrivateProperties {
		if property.Name == "#secret" && property.Value != nil {
			if _, err := retained.Page().Object(property.Value); err != nil {
				t.Fatal("private property lost custody", err)
			}
			private = true
		}
	}
	if child == nil || !symbol || !private {
		t.Fatal("native property kinds were dropped")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, ok := snapshot.ValueCopy()
	var stored runtime.GetPropertiesResult
	if !ok || json.Unmarshal(evidence.PayloadCopy(), &stored) != nil || len(stored.Result) == 0 {
		t.Fatal("property result lost independent evidence")
	}
	drain(t, dependencies, 1)
	answer, _, err := child.Call(bounded(t), "function(){return this.answer}")
	if err != nil {
		t.Fatal(err)
	}
	var value runtime.RemoteObject
	if err := json.Unmarshal(answer.DataCopy(), &value); err != nil || string(value.Value) != "42" {
		t.Fatal("property reference called the wrong receiver", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := root.Properties(bounded(t), runtime.GetPropertiesParams{ObjectID: "foreign"}); !errors.Is(err, ErrInput) {
		t.Fatal("foreign property ID accepted", err)
	}
	drain(t, dependencies, 1)
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<p>replacement document</p>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := child.Properties(bounded(t), params); !errors.Is(err, ErrState) {
		t.Fatal("old document reference accepted", err)
	}
	drain(t, dependencies, 1)
	_ = root.Close(bounded(t))
	if !root.Closed() || !child.Closed() {
		t.Fatal("property descendants outlived their group")
	}
	if _, err := registration.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("property custody blocked completed shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveObjectPropertiesDoNotEvaluateGettersAndSupportPollGroups(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	group, _, err := retained.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	object, _, err := group.PollFunction(bounded(t), `() => Object.defineProperty(Object.create(null), "value", {
		get() { globalThis.unexpectedGetter = true; return {answer:42}; }
	})`)
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	properties, _, err := object.Properties(bounded(t), runtime.GetPropertiesParams{AccessorPropertiesOnly: new(true)})
	if err != nil || len(properties.Result) != 1 || properties.Result[0].Get == nil {
		t.Fatal("native accessor descriptors changed", err)
	}
	drain(t, dependencies, 1)
	getter, err := retained.Page().Object(properties.Result[0].Get)
	if err != nil {
		t.Fatal(err)
	}
	if absent, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("typeof unexpectedGetter === 'undefined'")); err != nil || !absent {
		t.Fatal("property inspection executed a getter", err)
	}
	drain(t, dependencies, 1)
	if err := group.Close(bounded(t)); err != nil || !getter.Closed() {
		t.Fatal("ungrouped Poll property references not released", err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestObjectPropertiesNilReceiver(t *testing.T) {
	var object *Object
	if _, _, err := object.Properties(context.Background(), runtime.GetPropertiesParams{}); !errors.Is(err, ErrInput) {
		t.Fatal("nil property receiver accepted", err)
	}
}
