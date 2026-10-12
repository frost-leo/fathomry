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
