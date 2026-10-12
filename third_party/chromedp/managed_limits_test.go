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
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chromedp/cdproto"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/runtime"
)

func TestManagedCallAdmissionBeforeCodec(t *testing.T) {
	var called atomic.Bool
	input := &managedCodec{called: &called}
	sentinel := errors.New("admission")
	ctx := WithCallLimits(context.Background(), CallLimits{Before: func(string) error { return sentinel }})
	if _, err := encodeCall(ctx, "Runtime.evaluate", input); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if called.Load() {
		t.Fatal("codec ran before admission")
	}
}

func TestManagedHelperAdmissionBeforeArguments(t *testing.T) {
	var called atomic.Bool
	sentinel := errors.New("admission")
	ctx := WithCallLimits(context.Background(), CallLimits{Before: func(string) error { return sentinel }})
	_, _, err := callFunctionOn[int](ctx, nil, "function(x) { return x; }", nil, &managedCodec{called: &called})
	if !errors.Is(err, sentinel) || called.Load() {
		t.Fatal("helper encoded arguments before admission", err)
	}
	args := &errAppender{limit: 8}
	args.append("12345")
	if len(args.args) != 1 || args.bytes != args.limit {
		t.Fatal("positive argument did not fill its reservation", args.err)
	}
	args.append(&managedCodec{called: &called})
	if !errors.Is(args.err, ErrCommandLimit) || called.Load() || args.bytes > args.limit {
		t.Fatal("argument encoding exceeded cumulative admission", args.err)
	}
	args = &errAppender{limit: 8}
	args.append(strings.Repeat("x", 1024))
	if !errors.Is(args.err, ErrCommandLimit) || len(args.args) != 0 {
		t.Fatal("large argument retained", args.err)
	}
}

func TestManagedPollChargesOneCommand(t *testing.T) {
	ctx, cancel := testAllocate(t, "form.html")
	defer cancel()
	ctx, done := context.WithTimeout(ctx, 5*time.Second)
	defer done()
	if err := Do(ctx); err != nil {
		t.Fatal(err)
	}
	var calls int
	work := WithCallLimits(ctx, CallLimits{MaxInputBytes: 64 << 10, Before: func(method string) error {
		calls++
		if method != runtime.CommandCallFunctionOn || calls > 1 {
			return ErrCommandLimit
		}
		return nil
	}})
	value, err := Run(work, PollFunction[int]("value => value", WithPollingArgs(7)))
	if err != nil || value != 7 || calls != 1 {
		t.Fatal("helper admission was lost or charged twice", value, calls, err)
	}
}

func TestManagedBindingRoutesBeforeHandlerAdmission(t *testing.T) {
	state := bindingState{limits: BindingLimits{MaxActive: 1, MaxInputBytes: 128}}
	if !state.acquire(1) {
		t.Fatal("positive admission")
	}
	defer state.release()
	matched, err := state.matches(jsonv2.Value(`{"name":"other","payload":"opaque"}`), "selected")
	if err != nil || matched {
		t.Fatal("unrelated event took handler admission", err)
	}
	if _, err := state.matches(jsonv2.Value(strings.Repeat("x", 129)), "selected"); !errors.Is(err, ErrBindingLimit) {
		t.Fatal("routing exceeded its input bound", err)
	}
	if _, err := state.matches(jsonv2.Value(`{"name":`), "selected"); err == nil {
		t.Fatal("malformed routing accepted")
	}
}

type managedCodec struct{ called *atomic.Bool }

func (v *managedCodec) MarshalJSON() ([]byte, error) {
	v.called.Store(true)
	return []byte(`{"value":7}`), nil
}

func TestManagedCallLogicalBytes(t *testing.T) {
	ctx := WithCallLimits(context.Background(), CallLimits{MaxInputBytes: 8, MaxOutputBytes: 8})
	if _, err := encodeCall(ctx, "test", map[string]string{"too": "large"}); !errors.Is(err, ErrCommandLimit) {
		t.Fatal(err)
	}
	var result int
	if err := decodeCall(ctx, "test", &cdproto.Message{Result: jsonv2.Value("123456789")}, &result); !errors.Is(err, ErrCommandLimit) {
		t.Fatal(err)
	}
	if result != 0 {
		t.Fatal("result decoded before its limit")
	}
}

func TestManagedCallArgumentsPreserveEmptyValues(t *testing.T) {
	for _, raw := range []string{`""`, `[]`, `{}`, `null`, `false`, `0`} {
		t.Run(raw, func(t *testing.T) {
			params := runtime.CallFunctionOnParams{FunctionDeclaration: "function(value) { return value; }", Arguments: []*runtime.CallArgument{{Value: jsonv2.Value(raw)}, {}, nil}}
			encoded, err := encodeCall(context.Background(), runtime.CommandCallFunctionOn, params)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Arguments []map[string]jsonv2.Value `json:"arguments"`
			}
			if err := jsonv2.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			if len(wire.Arguments) != 3 || string(wire.Arguments[0]["value"]) != raw {
				t.Fatal("empty argument became JavaScript undefined", string(encoded))
			}
			if _, exists := wire.Arguments[1]["value"]; exists || wire.Arguments[2] != nil {
				t.Fatal("absent argument or nil parameter changed")
			}
			if string(params.Arguments[0].Value) != raw {
				t.Fatal("caller argument changed")
			}
		})
	}
}

func TestManagedCallArgumentReferencesAndPointerParameters(t *testing.T) {
	params := &runtime.CallFunctionOnParams{FunctionDeclaration: "function(x) { return x; }", Arguments: []*runtime.CallArgument{{ObjectID: "owned-object"}, {UnserializableValue: "NaN"}, {Value: jsonv2.Value(`[]`)}}}
	ctx := WithCallLimits(context.Background(), CallLimits{MaxInputBytes: 1024})
	encoded, err := encodeCall(ctx, runtime.CommandCallFunctionOn, params)
	if err != nil {
		t.Fatal(err)
	}
	var copied runtime.CallFunctionOnParams
	if err := jsonv2.Unmarshal(encoded, &copied); err != nil {
		t.Fatal(err)
	}
	if len(copied.Arguments) != 3 || copied.Arguments[0].ObjectID != "owned-object" || copied.Arguments[1].UnserializableValue != "NaN" || string(copied.Arguments[2].Value) != "[]" {
		t.Fatal("argument identity fields changed")
	}
	ctx = WithCallLimits(context.Background(), CallLimits{MaxInputBytes: 8})
	if _, err := encodeCall(ctx, runtime.CommandCallFunctionOn, params); !errors.Is(err, ErrCommandLimit) {
		t.Fatal("argument shadow bypassed input bound", err)
	}
}

func TestManagedBindingActualExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		target := &Target{bindings: bindingState{limits: BindingLimits{MaxActive: 1, MaxInputBytes: 4}}}
		if target.bindings.acquire(5) {
			t.Fatal("large input admitted")
		}
		if !target.bindings.acquire(4) || target.bindings.acquire(1) {
			t.Fatal("handler limit")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := target.JoinBindings(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal("active handler released", err)
		}
		if target.bindings.acquire(1) {
			t.Fatal("sealed admission")
		}
		target.bindings.release()
		if err := target.JoinBindings(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestManagedSubscriptionBoundedDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var failures int
		subs := subscribers{limits: EventBuffer{MaxEvents: 2, MaxBytes: 8, OnOverflow: func(error) { failures++ }}}
		values, stop := subs.subscribe("gate")
		defer stop()
		subs.publish("gate", jsonv2.Value("12"))
		synctest.Wait()
		subs.publish("gate", jsonv2.Value("34"))
		if got := <-values; string(got) != "12" {
			t.Fatal(string(got))
		}
		synctest.Wait()
		// One value is still held by the pump. Bytes and count include it.
		for sub := range subs.subs["gate"] {
			sub.mu.Lock()
			if sub.count != 1 || sub.bytes != 2 {
				t.Errorf("lost in-flight accounting: %d/%d", sub.count, sub.bytes)
			}
			sub.mu.Unlock()
		}
		subs.publish("gate", jsonv2.Value("1234567"))
		synctest.Wait()
		if failures != 1 {
			t.Fatal("overflow not observed", failures)
		}
		for range values {
		}
		subs.close()
	})
}

func TestManagedSubscriptionAfterCloseDoesNotStartProducer(t *testing.T) {
	var subs subscribers
	subs.close()
	for _, many := range []bool{false, true} {
		var events <-chan jsonv2.Value
		var stop func()
		if many {
			events, stop = subs.subscribeMany("a", "b")
		} else {
			events, stop = subs.subscribe("a")
		}
		if _, ok := <-events; ok {
			t.Fatal("closed subscription produced a value")
		}
		stop()
		stop()
	}
}
