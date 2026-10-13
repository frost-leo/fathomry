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
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/runtime"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

type objectArgument struct{ calls *atomic.Int64 }

func (value objectArgument) MarshalJSON() ([]byte, error) {
	value.calls.Add(1)
	return []byte("1"), nil
}

func objectFixture(t *testing.T) (*Owner, *Context, Dependencies) {
	t.Helper()
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || os.Getenv("FATHOMRY_CHROME_EXECUTABLE") == "" {
		t.Skip("explicit browser fixture not selected")
	}
	prepared, err := Prepare(Settings{Name: "objects", ExecPath: os.Getenv("FATHOMRY_CHROME_EXECUTABLE"), TempDir: t.TempDir(), MaxSessions: new(1), MaxPages: new(2), MaxObjectGroups: new(2), MaxObjects: new(8), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return owner, retained, dependencies
}

func TestLiveObjectsPreserveNativeDataAndOpaqueCustody(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	page := retained.Page()
	value, receipt, err := RunAction(bounded(t), page, Evaluate[*runtime.RemoteObject]("({answer:41})"))
	if err != nil || value == nil || value.ObjectID == "" {
		t.Fatal("native reference missing", err)
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("remote reference refunded its independent receipt")
	}
	object, err := page.Object(value)
	if err != nil {
		t.Fatal(err)
	}
	copy := *value
	if _, err := page.Object(&copy); !errors.Is(err, ErrInput) {
		t.Fatal("copied metadata became authority", err)
	}
	identity := value.ObjectID
	value.ObjectID = "foreign"
	if _, err := page.Object(value); !errors.Is(err, ErrInput) {
		t.Fatal("substituted native identity accepted", err)
	}
	value.ObjectID = identity
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, err := other.Object(value); !errors.Is(err, ErrInput) {
		t.Fatal("foreign page accepted reference", err)
	}
	answer, operation, err := object.Call(bounded(t), "function(delta){return this.answer+delta}", 1)
	if err != nil {
		t.Fatal(err)
	}
	var descriptor runtime.RemoteObject
	if err := json.Unmarshal(answer.DataCopy(), &descriptor); err != nil || string(descriptor.Value) != "42" {
		t.Fatal("native object call semantics changed", err)
	}
	if _, err := operation.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if err := object.Close(bounded(t)); err != nil || !object.Closed() || !answer.Closed() {
		t.Fatal("implicit group descendants not released", err)
	}
	result, err := receipt.WaitReleased(bounded(t))
	if err != nil || result.Err() != nil {
		t.Fatal(err, result.Err())
	}
	facts, ok := result.ValueCopy()
	if !ok || !facts.Object().GroupReleased {
		t.Fatal("independent group cleanup evidence missing")
	}
	drain(t, dependencies, 1)
	if _, _, err := object.Call(bounded(t), "function(){return 1}"); err == nil {
		t.Fatal("closed reference revived")
	}
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveObjectGroupPollAndSpecialValues(t *testing.T) {
	_, retained, dependencies := objectFixture(t)
	group, registration, err := retained.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	object, _, err := group.PollFunction(bounded(t), "value=>({answer:value})", WithPollingArgs(42))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	answer, _, err := object.Call(bounded(t), "function(){return this.answer}")
	if err != nil {
		t.Fatal(err)
	}
	var descriptor runtime.RemoteObject
	if err := json.Unmarshal(answer.DataCopy(), &descriptor); err != nil || string(descriptor.Value) != "42" {
		t.Fatal("Poll result was re-evaluated or changed", err)
	}
	drain(t, dependencies, 1)
	for _, expression := range []string{"null", "undefined"} {
		value, _, err := group.Evaluate(bounded(t), expression)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(value.DataCopy(), &descriptor); err != nil {
			t.Fatal(err)
		}
		if expression == "undefined" && descriptor.Type != runtime.RemoteObjectTypeUndefined || expression == "null" && descriptor.Subtype != runtime.RemoteObjectSubtypeNull {
			t.Fatal("native null/undefined distinction lost")
		}
		drain(t, dependencies, 1)
	}
	if err := group.Close(bounded(t)); err != nil || !object.Closed() {
		t.Fatal(err)
	}
	if _, err := registration.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
}

func TestLiveObjectCanceledProducerKeepsCustodyUntilContextClose(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	object, receipt, err := EvaluateObject(short, retained.Page(), "new Promise(()=>{})", EvalAwaitPromise())
	cancel()
	if object == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unknown producer handle missing", err)
	}
	if err := object.Close(bounded(t)); err == nil || object.Closed() {
		t.Fatal("early release falsely settled producer", err)
	}
	if result, _ := receipt.Snapshot(); result.Info().Released {
		t.Fatal("uncertain producer released evidence custody")
	}
	_ = retained.Close(bounded(t))
	if !object.Closed() {
		t.Fatal("owned context disposal did not settle reference")
	}
	result, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(result.Primary(), context.DeadlineExceeded) || result.Cleanup() == nil {
		t.Fatal("cancellation or cleanup history lost", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("owned source did not finish")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveObjectEvidenceAdmissionPrecedesNativeSetup(t *testing.T) {
	_, retained, dependencies := objectFixture(t)
	group, _, err := retained.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	fillControlInbox(t, retained, dependencies)
	var encodes atomic.Int64
	if object, _, err := RunAction(bounded(t), retained.Page(), PollFunction[*runtime.RemoteObject]("value=>{globalThis.refusedObject=1;return {value}}", WithPollingArgs(objectArgument{calls: &encodes}))); object != nil || !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("full Inbox admitted an implicit producer", err)
	}
	if object, _, err := group.PollFunction(bounded(t), "value=>{globalThis.refusedObject=1;return {value}}", WithPollingArgs(objectArgument{calls: &encodes})); object != nil || !errors.Is(err, adapters.ErrEvidence) || encodes.Load() != 0 {
		t.Fatal("codec ran before child evidence admission", err)
	}
	if err := group.Close(bounded(t)); err != nil || !group.Closed() {
		t.Fatal("group cleanup needed fresh evidence", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding-2)
	if absent, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("typeof refusedObject === 'undefined'")); err != nil || !absent {
		t.Fatal("rejected object producer ran", err)
	}
	drain(t, dependencies, 1)
}

func TestLiveObjectConstructorAndPublicWorkRemainJoined(t *testing.T) {
	_, retained, dependencies := objectFixture(t)
	page := retained.Page()
	for _, construction := range []bool{true, false} {
		t.Run(map[bool]string{true: "constructor", false: "operation"}[construction], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var group *ObjectGroup
			var receipt *adapters.Receipt[Result]
			var err error
			if !construction {
				group, receipt, err = page.NewObjectGroup(bounded(t))
				if err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if construction {
					group, receipt, err = newObjectGroup(bounded(t), page, "object_fixture", true, func(ctx context.Context, nativeGroup *native.ObjectGroup) (*native.Object, error) {
						value, err := native.RunObjectAction(ctx, page.native, nativeGroup, native.Evaluate[*runtime.RemoteObject]("({value:1})"))
						close(entered)
						<-release
						return value, err
					})
				} else {
					_, _, err = group.run(bounded(t), "object_fixture", func(ctx context.Context) (*native.Object, error) {
						value, err := native.RunObjectAction(ctx, page.native, group.state.native, native.Evaluate[*runtime.RemoteObject]("({value:1})"))
						close(entered)
						<-release
						return value, err
					})
				}
			}()
			<-entered
			if construction {
				retained.mu.Lock()
				for state := range retained.objectGroups {
					group = &ObjectGroup{state: state}
					receipt = state.call.Receipt()
				}
				retained.mu.Unlock()
			}
			short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			closeErr := group.Close(short)
			cancel()
			if closeErr == nil || group.Closed() {
				t.Fatal("public metadata/receipt work was not joined", closeErr)
			}
			if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
				t.Fatal("group receipt released before metadata")
			}
			close(release)
			<-done
			_ = group.Close(bounded(t))
			if !group.Closed() {
				t.Fatal("completed public work remained held", err)
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding-2)
		})
	}
}
