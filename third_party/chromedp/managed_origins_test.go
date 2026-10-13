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
	"testing/synctest"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/runtime"
)

func TestManagedExecutionOriginIsCapturedBeforeQueuedDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		target := &Target{events: subscribers{limits: EventBuffer{MaxEvents: 8, MaxBytes: 4096}}}
		change := func(method string, params string) {
			t.Helper()
			if err := target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(method), Params: jsonv2.Value(params)}); err != nil {
				t.Fatal(err)
			}
		}
		var captured []string
		events, stop := target.SubscribeBounded(EventBuffer{MaxEvents: 4, MaxBytes: 1024, Observe: func(_ string, _ jsonv2.Value, _ uint64) error {
			captured = append(captured, target.executionOrigin(7))
			return nil
		}}, runtime.BindingCalled.Method)
		defer stop()
		change(runtime.ExecutionContextCreated.Method, `{"context":{"id":7,"uniqueId":"old-world"}}`)
		target.events.publish(runtime.BindingCalled.Method, jsonv2.Value(`{"name":"owned","executionContextId":7}`))
		synctest.Wait()
		change(runtime.ExecutionContextDestroyed.Method, `{"executionContextUniqueId":"old-world"}`)
		change(runtime.ExecutionContextCreated.Method, `{"context":{"id":7,"uniqueId":"new-world"}}`)
		target.events.publish(runtime.BindingCalled.Method, jsonv2.Value(`{"name":"owned","executionContextId":7}`))
		if len(captured) != 2 || captured[0] != "old-world" || captured[1] != "new-world" {
			t.Fatal("event-time source followed a reused numeric context ID")
		}
		<-events
		<-events
		if target.hasExecutionOrigin("old-world") || !target.hasExecutionOrigin("new-world") {
			t.Fatal("stale execution origin remained valid")
		}
		change(runtime.ExecutionContextsCleared.Method, `{}`)
		if len(target.executionOriginCopy()) != 0 {
			t.Fatal("cleared contexts retained authority")
		}
	})
}

func TestManagedBindingRoutingPrecedesQueueCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observed, overflow := 0, 0
		subs := subscribers{}
		_, stop := subs.subscribeBounded(EventBuffer{MaxEvents: 1, MaxBytes: 1024,
			filter:     func(_ string, raw jsonv2.Value) bool { return string(raw) == `{"name":"ours"}` },
			Observe:    func(string, jsonv2.Value, uint64) error { observed++; return nil },
			OnOverflow: func(error) { overflow++ },
		}, runtime.BindingCalled.Method)
		defer stop()
		subs.publish(runtime.BindingCalled.Method, jsonv2.Value(`{"name":"ours"}`))
		synctest.Wait()
		for range 32 {
			subs.publish(runtime.BindingCalled.Method, jsonv2.Value(`{"name":"other"}`))
		}
		if observed != 1 || overflow != 0 {
			t.Fatal("unrelated events consumed another binding's native capacity")
		}
		subs.publish(runtime.BindingCalled.Method, jsonv2.Value(`{"name":"ours"}`))
		if overflow != 1 || observed != 1 {
			t.Fatal("matched events bypassed native queue admission")
		}
	})
}

func TestManagedOriginCompletenessNeedsAConfirmedReset(t *testing.T) {
	target := &Target{events: subscribers{limits: EventBuffer{MaxBytes: 128}}}
	created := func(id string) error {
		return target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(runtime.ExecutionContextCreated.Method), Params: jsonv2.Value(`{"context":{"id":1,"uniqueId":"` + id + `"}}`)})
	}
	if err := created("first"); err != nil {
		t.Fatal(err)
	}
	if err := target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(runtime.ExecutionContextDestroyed.Method), Params: jsonv2.Value(`{}`)}); err == nil || target.executionOriginError() == nil {
		t.Fatal("missing removal identity retained a complete origin claim")
	}
	if err := created("replacement"); err != nil || target.executionOriginError() == nil {
		t.Fatal("one later valid event hid unknown world ownership", err)
	}
	for _, malformed := range []string{`[]`, `null`, `true`, `0`, `"{}"`, " \t", " \tnull"} {
		if err := target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(runtime.ExecutionContextsCleared.Method), Params: jsonv2.Value(malformed)}); err == nil || target.executionOriginError() == nil {
			t.Fatal("malformed reset certified complete origin tracking")
		}
	}
	if err := target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(runtime.ExecutionContextsCleared.Method), Params: jsonv2.Value(`{}`)}); err != nil || target.executionOriginError() != nil {
		t.Fatal("confirmed reset did not retire uncertain origins", err)
	}
	if err := created("final"); err != nil || errors.Is(target.executionOriginError(), ErrEventOverflow) {
		t.Fatal("new complete origin set was refused", err)
	}
}

func TestManagedExecutionOriginSnapshotIsBoundedCopiedAndCurrent(t *testing.T) {
	target := &Target{events: subscribers{limits: EventBuffer{MaxBytes: 4096}}}
	change := func(method, params string) {
		t.Helper()
		if err := target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(method), Params: jsonv2.Value(params)}); err != nil {
			t.Fatal(err)
		}
	}
	change(runtime.ExecutionContextCreated.Method, `{"context":{"id":9,"uniqueId":"first"}}`)
	change(runtime.ExecutionContextCreated.Method, `{"context":{"id":3,"uniqueId":"second"}}`)
	for _, limit := range []int{-1, 0, 1} {
		if values, err := target.ExecutionOrigins(limit); !errors.Is(err, ErrEventOverflow) || values != nil {
			t.Fatal("count refusal returned a snapshot", err)
		}
	}
	values, err := target.ExecutionOrigins(2)
	if err != nil || len(values) != 2 || values[0].ID != 3 || values[1].ID != 9 {
		t.Fatal("snapshot order or identity changed", err)
	}
	values[0].UniqueID = "caller-change"
	current, err := target.ExecutionOrigins(2)
	if err != nil || current[0].UniqueID != "second" {
		t.Fatal("snapshot remained aliased", err)
	}
	change(runtime.ExecutionContextDestroyed.Method, `{"executionContextUniqueId":"first"}`)
	change(runtime.ExecutionContextCreated.Method, `{"context":{"id":9,"uniqueId":"replacement"}}`)
	current, err = target.ExecutionOrigins(2)
	if err != nil || current[1].UniqueID != "replacement" || values[1].UniqueID != "first" {
		t.Fatal("numeric reuse changed an old snapshot", err)
	}
	if err := target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(runtime.ExecutionContextDestroyed.Method), Params: jsonv2.Value(`{}`)}); err == nil {
		t.Fatal("malformed destruction accepted")
	}
	if values, err := target.ExecutionOrigins(100); err == nil || values != nil {
		t.Fatal("uncertain origin table was published")
	}
	change(runtime.ExecutionContextsCleared.Method, `{}`)
	empty, err := target.ExecutionOrigins(0)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatal("empty snapshot lost its meaning", err)
	}
	var absent *Target
	if _, err := absent.ExecutionOrigins(1); !errors.Is(err, ErrInvalidContext) {
		t.Fatal(err)
	}
	target.stopped = make(chan struct{})
	close(target.stopped)
	if _, err := target.ExecutionOrigins(1); !errors.Is(err, ErrInvalidContext) {
		t.Fatal("stopped target returned identities", err)
	}
}

func TestManagedIsolatedWorldDoesNotReplaceDefaultFrameExecution(t *testing.T) {
	frameID := cdp.FrameID("frame")
	target := &Target{execContexts: make(map[cdp.FrameID]runtime.ExecutionContextID), execUniqueIDs: make(map[cdp.FrameID]string)}
	target.runtimeEvent(&runtime.EventExecutionContextCreated{Context: &runtime.ExecutionContextDescription{ID: 7, UniqueID: "default", AuxData: jsonv2.Value(`{"frameId":"frame","isDefault":true}`)}})
	target.runtimeEvent(&runtime.EventExecutionContextCreated{Context: &runtime.ExecutionContextDescription{ID: 9, UniqueID: "isolated", AuxData: jsonv2.Value(`{"frameId":"frame","isDefault":false}`)}})
	target.frameMu.RLock()
	id, unique := target.execContexts[frameID], target.execUniqueIDs[frameID]
	target.frameMu.RUnlock()
	if id != 7 || unique != "default" {
		t.Fatal("isolated world replaced the native default-frame execution")
	}
	target.runtimeEvent(&runtime.EventExecutionContextDestroyed{ExecutionContextUniqueID: "isolated"})
	target.frameMu.RLock()
	id = target.execContexts[frameID]
	target.frameMu.RUnlock()
	if id != 7 {
		t.Fatal("isolated-world destruction revoked the default frame")
	}
}
