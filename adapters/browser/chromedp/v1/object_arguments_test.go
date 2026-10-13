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
	"sync/atomic"
	"testing"

	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveObjectArgumentsPreserveReferencesAndNativeSpecialValues(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	first, _, err := retained.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := retained.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	receiver, _, err := first.Evaluate(bounded(t), "({base:40})")
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	reference, _, err := second.Evaluate(bounded(t), "({delta:2})")
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	answer, _, err := receiver.CallArguments(bounded(t),
		"function(ref,n,u,s,z,empty){return this.base+ref.delta===42 && n===null && u===undefined && Number.isNaN(s) && Object.is(z,-0) && empty===''}",
		ObjectArgument(reference), ValueArgument(nil), UndefinedArgument(),
		UnserializableArgument("NaN"), UnserializableArgument("-0"), ValueArgument(""))
	if err != nil {
		t.Fatal(err)
	}
	var value runtime.RemoteObject
	if err := json.Unmarshal(answer.DataCopy(), &value); err != nil || string(value.Value) != "true" {
		t.Fatal("native reference/null/undefined/special argument semantics changed", err)
	}
	drain(t, dependencies, 1)
	if err := answer.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	for _, argument := range []Argument{{}, ObjectArgument(nil)} {
		if _, receipt, err := receiver.CallArguments(bounded(t), "function(){globalThis.refusedArgument=1}", ValueArgument(objectArgument{calls: &calls}), argument); !errors.Is(err, ErrInput) || receipt == nil || calls.Load() != 0 {
			t.Fatal("invalid reference reached value codecs or lost evidence", err)
		}
		drain(t, dependencies, 1)
	}
	if err := second.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := receiver.CallArguments(bounded(t), "function(){globalThis.refusedArgument=1}", ObjectArgument(reference)); !errors.Is(err, ErrState) {
		t.Fatal("released reference was accepted", err)
	}
	drain(t, dependencies, 1)
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	foreign, _, err := EvaluateObject(bounded(t), other, "({delta:99})")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := receiver.CallArguments(bounded(t), "function(){globalThis.refusedArgument=1}", ObjectArgument(foreign)); !errors.Is(err, ErrInput) {
		t.Fatal("foreign page reference was accepted", err)
	}
	drain(t, dependencies, 1)
	if err := foreign.Close(bounded(t)); err != nil {
		t.Fatal("foreign fixture did not release its group slot", err)
	}
	drain(t, dependencies, 1)
	if absent, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("typeof refusedArgument === 'undefined'")); err != nil || !absent {
		t.Fatal("refused reference operation executed", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !first.Closed() || !foreign.Closed() {
		t.Fatal("argument resources did not release")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveObjectArgumentCodecsWaitForEvidenceAdmission(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	receiver, _, err := EvaluateObject(bounded(t), retained.Page(), "({})")
	if err != nil {
		t.Fatal(err)
	}
	fillControlInbox(t, retained, dependencies)
	var calls atomic.Int64
	if _, _, err := receiver.CallArguments(bounded(t), "function(v){return v}", ValueArgument(objectArgument{calls: &calls})); err == nil || calls.Load() != 0 {
		t.Fatal("argument codec ran before independent evidence admission", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("saturated evidence prevented object cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestObjectArgumentsNilHandle(t *testing.T) {
	var object *Object
	if _, _, err := object.CallArguments(context.Background(), "function(){}"); !errors.Is(err, ErrInput) {
		t.Fatal("nil receiver was accepted", err)
	}
}

func TestLiveObjectCallOptionsPreserveNativeSemanticsAndIdentity(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	receiver, _, err := EvaluateObject(bounded(t), retained.Page(), "({base:40})")
	if err != nil {
		t.Fatal(err)
	}
	params := runtime.CallFunctionOnParams{
		FunctionDeclaration: "async function(delta){return {answer:this.base+delta}}",
		AwaitPromise:        new(true), ReturnByValue: new(true), Silent: new(false), UserGesture: new(true),
	}
	answer, _, err := receiver.CallWithOptions(bounded(t), params, ValueArgument(2))
	if err != nil || answer == nil {
		t.Fatal("native awaited call failed", err)
	}
	var value runtime.RemoteObject
	if err := json.Unmarshal(answer.DataCopy(), &value); err != nil || value.ObjectID != "" || string(value.Value) != `{"answer":42}` {
		t.Fatal("native promise/value options changed", err)
	}
	if params.ObjectID != "" || params.ObjectGroup != "" || params.Arguments != nil || !*params.AwaitPromise {
		t.Fatal("call preparation changed caller data")
	}
	drain(t, dependencies, 1)
	_ = answer.Close(bounded(t))
	params.AwaitPromise, params.ReturnByValue = new(false), new(false)
	answer, _, err = receiver.CallWithOptions(bounded(t), params, ValueArgument(2))
	if err != nil || answer == nil || answer.native.NativeValue().Subtype != runtime.RemoteObjectSubtypePromise || answer.native.NativeValue().ObjectID == "" {
		t.Fatal("explicit false promise/reference options changed", err)
	}
	drain(t, dependencies, 1)
	_ = answer.Close(bounded(t))
	answer, _, err = receiver.CallWithOptions(bounded(t), runtime.CallFunctionOnParams{
		FunctionDeclaration: "function(){return {nested:{answer:42}}}", ReturnByValue: new(false),
		SerializationOptions: &runtime.SerializationOptions{Serialization: runtime.SerializationOptionsSerializationDeep, MaxDepth: new(int64(1))},
	})
	if err != nil || answer == nil || answer.native.NativeValue().DeepSerializedValue == nil {
		t.Fatal("native deep serialization options missing", err)
	}
	drain(t, dependencies, 1)
	_ = answer.Close(bounded(t))
	_, _, err = receiver.CallWithOptions(bounded(t), runtime.CallFunctionOnParams{
		FunctionDeclaration: "function(){this.sideEffect=1;return 1}", ThrowOnSideEffect: new(true),
	})
	var exception *sdk.ExceptionError
	if !errors.As(err, &exception) {
		t.Fatal("native side-effect refusal lost its inspectable exception", err)
	}
	drain(t, dependencies, 1)
	var calls atomic.Int64
	for _, params := range []runtime.CallFunctionOnParams{
		{ObjectID: "foreign"}, {ObjectGroup: "foreign"}, {ExecutionContextID: 1}, {UniqueContextID: "foreign"},
		{Arguments: []*runtime.CallArgument{{ObjectID: "foreign"}}},
	} {
		params.FunctionDeclaration = "function(){this.forbidden=1}"
		if _, _, err := receiver.CallWithOptions(bounded(t), params, ValueArgument(objectArgument{calls: &calls})); !errors.Is(err, ErrInput) || calls.Load() != 0 {
			t.Fatal("native identity fields reached a value codec", err)
		}
		drain(t, dependencies, 1)
	}
	answer, _, err = receiver.Call(bounded(t), "function(){return this.sideEffect===undefined && this.forbidden===undefined}")
	executionValue(t, answer, "true", err)
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("call option results or exceptions escaped group custody")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
