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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func bindingFixture(t *testing.T, address string, calls int) (*Owner, *Context, Dependencies) {
	t.Helper()
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	prepared, err := Prepare(Settings{Name: "bindings", ExecPath: path, TempDir: t.TempDir(), MaxSessions: new(1), MaxPages: new(1), MaxBindings: new(2), MaxBindingCalls: new(calls), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}, NativeOptions{})
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
	if _, err := retained.Page().Navigate(bounded(t), address); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	return owner, retained, dependencies
}

func TestLiveBindingNativeSemanticsIndependentEvidenceAndRevocation(t *testing.T) {
	owner, retained, dependencies := bindingFixture(t, "data:text/html,<h1>binding</h1>", 2)
	var calls atomic.Int64
	binding, registration, err := BindFunc(retained.Context(), retained.Page(), "sum", func(ctx context.Context, values []int) (int, error) {
		calls.Add(1)
		if sdk.FromContext(ctx) != nil {
			return 0, errors.New("native target escaped through handler context")
		}
		sum := 0
		for _, value := range values {
			sum += value
		}
		return sum, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`window.alias=window.sum;sum(2,3,4)`, EvalAwaitPromise()))
	if err != nil || value != 9 || calls.Load() != 1 {
		t.Fatal("native binding arguments or promise result changed", err)
	}
	found := false
	for range 2 {
		delivery, err := dependencies.Evidence.NextReleased(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			t.Fatal(err)
		}
		snapshot, _ := receipt.Snapshot()
		result, _ := snapshot.ValueCopy()
		if snapshot.Info().Operation == "browser.chromedp.binding_call" {
			found = true
			if string(result.PayloadCopy()) != "9\n" || !result.Binding().Returned || !result.Binding().ReplyAcknowledged || snapshot.Primary() != nil {
				t.Fatalf("callback facts: payload=%q returned=%t reply=%t primary=%v", result.PayloadCopy(), result.Binding().Returned, result.Binding().ReplyAcknowledged, snapshot.Primary())
			}
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatal("ignored callback return lost independent evidence")
	}
	if err := binding.Close(bounded(t)); err != nil || !binding.Closed() {
		t.Fatal(err)
	}
	if _, err := registration.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	message, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`alias(1).catch(e=>e.message)`, EvalAwaitPromise()))
	if err != nil || message != "binding closed" || calls.Load() != 1 {
		t.Fatal("captured alias revived callback", err)
	}
	drain(t, dependencies, 1)
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 2)
}

var bindingDecodeCalls atomic.Int64

type bindingDecoded int

func (value *bindingDecoded) UnmarshalJSON([]byte) error {
	bindingDecodeCalls.Add(1)
	*value = 7
	return nil
}

func TestLiveBindingEvidenceSaturationPrecedesDecoderAndHandler(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/trigger" {
			<-release
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<script>fetch('/trigger').then(()=>window.gated(7)).catch(()=>{})</script>`))
	}))
	defer func() { once.Do(func() { close(release) }); server.Close() }()
	_, retained, dependencies := bindingFixture(t, server.URL, 1)
	bindingDecodeCalls.Store(0)
	var handlers atomic.Int64
	binding, receipt, err := BindFunc(retained.Context(), retained.Page(), "gated", func(context.Context, bindingDecoded) (int, error) { handlers.Add(1); return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	fillControlInbox(t, retained, dependencies)
	once.Do(func() { close(release) })
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !binding.Closed() {
		t.Fatal("refused producer was not cleaned", err)
	}
	if !errors.Is(snapshot.Primary(), adapters.ErrEvidence) || bindingDecodeCalls.Load() != 0 || handlers.Load() != 0 {
		t.Fatal("decoder or handler ran before evidence admission", snapshot.Primary())
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding-2)
}

func TestLiveBindingCloseRetainsUncooperativeCallback(t *testing.T) {
	owner, retained, dependencies := bindingFixture(t, "data:text/html,<h1>held handler</h1>", 1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	binding, receipt, err := BindFunc(retained.Context(), retained.Page(), "held", func(ctx context.Context, _ int) (int, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return 4, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`held(1).catch(()=>{});1`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-bounded(t).Done():
		t.Fatal("handler did not start")
	}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = owner.Close(short)
	cancel()
	if err == nil || owner.ShutdownComplete() || binding.Closed() {
		t.Fatal("active handler was declared released", err)
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("registration released before handler")
	}
	once.Do(func() { close(release) })
	for range 3 {
		_ = owner.Close(bounded(t))
		if owner.ShutdownComplete() {
			break
		}
	}
	if !owner.ShutdownComplete() || !binding.Closed() {
		t.Fatal("completed callback remained owned")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

type bindingExitResult struct{}

func (bindingExitResult) MarshalJSON() ([]byte, error) { runtime.Goexit(); return nil, nil }

type bindingPanicResult struct{}

func (bindingPanicResult) MarshalJSON() ([]byte, error) { panic("private-encoder-canary") }

func TestLiveBindingEncoderGoexitAndPanicPreserveOwnershipAndPrivacy(t *testing.T) {
	_, retained, dependencies := bindingFixture(t, "data:text/html,<h1>codec fixture</h1>", 2)
	exiting, _, err := BindFunc(retained.Context(), retained.Page(), "exiting", func(context.Context, int) (bindingExitResult, error) { return bindingExitResult{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	panicking, _, err := BindFunc(retained.Context(), retained.Page(), "panicking", func(context.Context, int) (bindingPanicResult, error) { return bindingPanicResult{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"exiting", "panicking"} {
		message, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](name+"(1).catch(e=>e.message)", EvalAwaitPromise()))
		if err != nil || message != "binding failed" {
			t.Fatal("codec failure leaked or promise stayed pending", err, message)
		}
		seen := false
		for range 2 {
			delivery, err := dependencies.Evidence.NextReleased(bounded(t))
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := delivery.Receipt()
			if err != nil {
				t.Fatal(err)
			}
			snapshot, _ := receipt.Snapshot()
			if snapshot.Info().Operation == "browser.chromedp.binding_call" {
				seen = true
				result, _ := snapshot.ValueCopy()
				if snapshot.Primary() == nil || !result.Binding().Returned || !result.Binding().ReplyAcknowledged {
					t.Fatal("handler return and codec failure conflated")
				}
				if strings.Contains(fmt.Sprintf("%+v", snapshot.Err()), "private-encoder-canary") || bytes.Contains(result.PayloadCopy(), []byte("private-encoder-canary")) {
					t.Fatal("private codec payload escaped")
				}
			}
			if err := delivery.Ack(); err != nil {
				t.Fatal(err)
			}
		}
		if !seen {
			t.Fatal("codec failure lost callback evidence")
		}
	}
	_ = exiting.Close(bounded(t))
	_ = panicking.Close(bounded(t))
	if !exiting.Closed() || !panicking.Closed() {
		t.Fatal("codec abnormal exit retained phantom work")
	}
	drain(t, dependencies, 2)
}

func TestLiveBindingProducerRefusalSurvivesCallbackCancellation(t *testing.T) {
	owner, retained, dependencies := bindingFixture(t, "data:text/html,<h1>producer saturation</h1>", 1)
	entered := make(chan struct{})
	binding, receipt, err := BindFunc(retained.Context(), retained.Page(), "saturated", func(ctx context.Context, _ int) (int, error) {
		close(entered)
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`saturated(1).catch(()=>{});1`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-bounded(t).Done():
		t.Fatal("first handler did not start")
	}
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`saturated(2).catch(()=>{});2`)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !binding.Closed() {
		t.Fatal("producer refusal did not finish the cooperative registration", err)
	}
	if !errors.Is(snapshot.Primary(), sdk.ErrBindingLimit) || !errors.Is(snapshot.Primary(), context.Canceled) {
		t.Fatal("producer refusal or callback cancellation disappeared", snapshot.Primary())
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("joined binding prevented source shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

type bindingExitArgument struct{}

func (*bindingExitArgument) UnmarshalJSON([]byte) error { runtime.Goexit(); return nil }

type bindingPanicArgument struct{}

func (*bindingPanicArgument) UnmarshalJSON([]byte) error { panic("private-decoder-canary") }

func TestLiveBindingDecoderAndHandlerAbnormalExit(t *testing.T) {
	_, retained, dependencies := bindingFixture(t, "data:text/html,<h1>abnormal callback</h1>", 1)
	tests := []struct {
		name string
		bind func() (*Binding, *adapters.Receipt[Result], error)
	}{
		{"decoder_exit", func() (*Binding, *adapters.Receipt[Result], error) {
			return BindFunc(retained.Context(), retained.Page(), "abnormal", func(context.Context, bindingExitArgument) (int, error) {
				t.Error("decoder exit reached handler")
				return 1, nil
			})
		}},
		{"decoder_panic", func() (*Binding, *adapters.Receipt[Result], error) {
			return BindFunc(retained.Context(), retained.Page(), "abnormal", func(context.Context, bindingPanicArgument) (int, error) {
				t.Error("decoder panic reached handler")
				return 1, nil
			})
		}},
		{"handler_exit", func() (*Binding, *adapters.Receipt[Result], error) {
			return BindFunc(retained.Context(), retained.Page(), "abnormal", func(context.Context, int) (int, error) { runtime.Goexit(); return 1, nil })
		}},
		{"handler_panic", func() (*Binding, *adapters.Receipt[Result], error) {
			return BindFunc(retained.Context(), retained.Page(), "abnormal", func(context.Context, int) (int, error) { panic("private-handler-canary") })
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding, _, err := test.bind()
			if err != nil {
				t.Fatal(err)
			}
			message, _, err := RunAction(bounded(t), retained.Page(), Evaluate[string](`abnormal(1).catch(e=>e.message)`, EvalAwaitPromise()))
			if err != nil || message != "binding failed" {
				t.Fatal("abnormal exit left the promise pending or exposed private text", err, message)
			}
			seen := false
			for range 2 {
				delivery, err := dependencies.Evidence.NextReleased(bounded(t))
				if err != nil {
					t.Fatal(err)
				}
				receipt, _ := delivery.Receipt()
				snapshot, _ := receipt.Snapshot()
				if snapshot.Info().Operation == "browser.chromedp.binding_call" {
					seen = true
					result, _ := snapshot.ValueCopy()
					if snapshot.Primary() == nil || result.Binding().Returned || !result.Binding().ReplyAcknowledged {
						t.Fatal("callback exit was reported as a returned value")
					}
					if strings.Contains(fmt.Sprintf("%+v", snapshot.Err()), "canary") || bytes.Contains(result.PayloadCopy(), []byte("canary")) {
						t.Fatal("private failure escaped")
					}
				}
				if err := delivery.Ack(); err != nil {
					t.Fatal(err)
				}
			}
			if !seen {
				t.Fatal("callback evidence disappeared")
			}
			_ = binding.Close(bounded(t))
			if !binding.Closed() {
				t.Fatal("abnormal exit retained phantom work")
			}
			drain(t, dependencies, 1)
		})
	}
	positive, _, err := BindFunc(retained.Context(), retained.Page(), "after", func(context.Context, int) (int, error) { return 42, nil })
	if err != nil {
		t.Fatal(err)
	}
	if value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`after(1)`, EvalAwaitPromise())); err != nil || value != 42 {
		t.Fatal("abnormal exits consumed the callback capacity permanently", err)
	}
	if err := positive.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 3)
}

func TestLiveBorrowedBindingCleanupRetryKeepsPeerAndPriorProperty(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if _, err := sdk.Run(peer, sdk.Evaluate[sdk.Void](`globalThis.bridge="peer-owned"`)); err != nil {
		t.Fatal(err)
	}
	binding, receipt, err := BindFunc(retained.Context(), retained.Page(), "bridge", func(context.Context, int) (int, error) { return 57, nil })
	if err != nil {
		t.Fatal(err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int](`globalThis.alias=bridge;bridge(1)`, sdk.EvalAwaitPromise)); err != nil || value != 57 {
		t.Fatal("borrowed target binding did not run", err)
	}
	if _, err := sdk.Run(peer, sdk.Evaluate[sdk.Void](`globalThis.refuseRestore=true;globalThis.savedDefine=Object.defineProperty;Object.defineProperty=function(object,name,descriptor){if(object===globalThis&&name==="bridge"&&refuseRestore)throw new Error("private-restore-canary");return savedDefine(object,name,descriptor)}`)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(bounded(t)); err == nil || owner.ShutdownComplete() || binding.Closed() {
		t.Fatal("unrestored borrowed property was declared released", err)
	}
	if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
		t.Fatal("failed cleanup released registration custody")
	}
	if _, err := sdk.Run(peer, sdk.Evaluate[sdk.Void](`refuseRestore=false;Object.defineProperty=savedDefine;delete globalThis.savedDefine`)); err != nil {
		t.Fatal("provider cleanup killed or disconnected the borrowed peer", err)
	}
	for range 3 {
		_ = owner.Close(bounded(t))
		if owner.ShutdownComplete() {
			break
		}
	}
	if !owner.ShutdownComplete() || !binding.Closed() {
		t.Fatal("repaired borrowed cleanup did not finish")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || snapshot.Cleanup() == nil {
		t.Fatal("cleanup history disappeared", err)
	}
	if strings.Contains(fmt.Sprintf("%+v", snapshot.Err()), "private-restore-canary") {
		t.Fatal("native cleanup exception leaked")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[string](`bridge`)); err != nil || value != "peer-owned" {
		t.Fatal("borrowed page property was not restored", err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[string](`alias(1).catch(e=>e.message)`, sdk.EvalAwaitPromise)); err != nil || value != "binding closed" {
		t.Fatal("captured alias survived binding cleanup", err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int](`Object.getOwnPropertyNames(globalThis).filter(n=>n.startsWith("__chromedp_")).length`)); err != nil || value != 0 {
		t.Fatal("borrowed page retained binding properties", err)
	}
	if err := sdk.Do(peer, sdk.Navigate("data:text/html,<h1>borrowed peer survives</h1>")); err != nil {
		t.Fatal("peer was not usable after provider shutdown", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
