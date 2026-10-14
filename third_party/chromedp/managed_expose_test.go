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
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
)

func managedBindingPage(t *testing.T, options ...BrowserOption) context.Context {
	t.Helper()
	ctx, cancel := NewContext(allocCtx, WithBrowserOption(options...))
	t.Cleanup(func() {
		if err := Cancel(ctx); err != nil {
			t.Error(err)
		}
		cancel()
	})
	if err := Do(ctx, Navigate("data:text/html,<h1>managed binding fixture</h1>")); err != nil {
		t.Fatal(err)
	}
	return ctx
}
func bindingDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestManagedBindingCloseRevokesAliasesAndRestoresPriorValue(t *testing.T) {
	ctx := managedBindingPage(t)
	if _, err := Run(ctx, Evaluate[Void](`window.restore="prior"`)); err != nil {
		t.Fatal(err)
	}
	binding, err := Run(ctx, BindFunc("restore", func(_ context.Context, value int) (int, error) { return value * 2, nil }, BindingOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	if value, err := Run(ctx, Evaluate[int](`window.saved=window.restore; saved(4)`, EvalAwaitPromise)); err != nil || value != 8 {
		t.Fatal(err)
	}
	if err := binding.Close(bindingDeadline(t)); err != nil || !binding.Closed() {
		t.Fatal(err)
	}
	if value, err := Run(ctx, Evaluate[string](`saved(4).then(()=>"unexpected",e=>e.message)`, EvalAwaitPromise)); err != nil || value != "binding closed" {
		t.Fatal("captured alias was not revoked", err)
	}
	if value, err := Run(ctx, Evaluate[string](`window.restore`)); err != nil || value != "prior" {
		t.Fatal("prior property was not restored", err)
	}
	if value, err := Run(ctx, Evaluate[int](`Object.getOwnPropertyNames(window).filter(n=>n.startsWith("__chromedp_")).length`)); err != nil || value != 0 {
		t.Fatal("private native function remained", value, err)
	}
	if err := Do(ctx, Navigate("data:text/html,<h1>after removal</h1>")); err != nil {
		t.Fatal(err)
	}
	if value, err := Run(ctx, Evaluate[string](`typeof window.restore`)); err != nil || value != "undefined" {
		t.Fatal("removed binding returned after navigation", err)
	}
}

func TestManagedBindingInheritsLimitsAndEscapesReplyBytes(t *testing.T) {
	ctx := managedBindingPage(t, WithBindingLimits(BindingLimits{MaxOutputBytes: 32768, PrivateErrors: true}))
	binding, err := Run(ctx, BindFunc("reply", func(_ context.Context, mode int) (string, error) {
		if mode == 1 {
			return strings.Repeat("x", 40000), nil
		}
		if mode == 2 {
			return "", errors.New("private-handler-canary")
		}
		return strings.Repeat("\\", 8192), nil
	}, BindingOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	if value, err := Run(ctx, Evaluate[string](`reply(0)`, EvalAwaitPromise)); err != nil || value != strings.Repeat("\\", 8192) {
		t.Fatal("valid output exhausted incorrectly calculated reply budget", err)
	}
	for _, expression := range []string{`reply(1).catch(e=>e.message)`, `reply(2).catch(e=>e.message)`} {
		if value, err := Run(ctx, Evaluate[string](expression, EvalAwaitPromise)); err != nil || value != "binding failed" {
			t.Fatal("zero options bypassed global binding policy", err)
		}
	}
	if !errors.Is(binding.Err(), ErrBindingLimit) {
		t.Fatal("native output refusal lost")
	}
	if err := binding.Close(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestManagedBindingLargeNeighborDoesNotCancelSmallBinding(t *testing.T) {
	ctx := managedBindingPage(t, WithEventBuffer(EventBuffer{MaxEvents: 16, MaxBytes: 32768}))
	small, err := Run(ctx, BindFunc("small", func(_ context.Context, value int) (int, error) { return value, nil }, BindingOptions{Limits: BindingLimits{MaxInputBytes: 256}, MaxEventBytes: 512}))
	if err != nil {
		t.Fatal(err)
	}
	large, err := Run(ctx, BindFunc("large", func(_ context.Context, value string) (int, error) { return len(value), nil }, BindingOptions{Limits: BindingLimits{MaxInputBytes: 8192}, MaxEventBytes: 16384}))
	if err != nil {
		t.Fatal(err)
	}
	if value, err := Run(ctx, Evaluate[int](`large("x".repeat(2048))`, EvalAwaitPromise)); err != nil || value != 2048 {
		t.Fatal(err)
	}
	if value, err := Run(ctx, Evaluate[int](`small(7)`, EvalAwaitPromise)); err != nil || value != 7 {
		t.Fatal("neighbor traffic revoked another binding", err)
	}
	if err := small.Close(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if value, err := Run(ctx, Evaluate[int](`large("ok")`, EvalAwaitPromise)); err != nil || value != 2 {
		t.Fatal("closing a neighbor revoked this binding", err)
	}
	if err := large.Close(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestManagedBindingCancellationJoinsAdmitBeforeDiscard(t *testing.T) {
	ctx := managedBindingPage(t, WithEventBuffer(EventBuffer{MaxEvents: 16, MaxBytes: 32768}))
	entered, resume, completed := make(chan struct{}), make(chan struct{}), make(chan BindingOutcome, 1)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(resume) }) })
	var handlers atomic.Int64
	binding, err := Run(ctx, BindFunc("race", func(context.Context, int) (int, error) { handlers.Add(1); return 1, nil }, BindingOptions{
		Admit: func(context.Context, int64) (context.Context, func(BindingOutcome), error) {
			close(entered)
			<-resume
			return context.Background(), func(outcome BindingOutcome) { completed <- outcome }, nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	trigger := make(chan error, 1)
	go func() { _, err := Run(ctx, Evaluate[Void](`race(1).catch(()=>{})`)); trigger <- err }()
	select {
	case <-entered:
	case <-bindingDeadline(t).Done():
		t.Fatal("admission did not start")
	}
	binding.Cancel()
	limit := time.Now().Add(time.Second)
	observed := false
	for time.Now().Before(limit) {
		buffer := make([]byte, 1<<20)
		size := goruntime.Stack(buffer, true)
		if strings.Contains(string(buffer[:size]), "(*subscribers).subscribeBounded.func") {
			observed = true
			break
		}
		goruntime.Gosched()
	}
	if !observed {
		t.Fatal("listener never reached the native producer join")
	}
	once.Do(func() { close(resume) })
	select {
	case <-completed:
	case <-bindingDeadline(t).Done():
		t.Fatal("cancellation discarded an admitted completion")
	}
	if err := binding.WaitStopped(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if handlers.Load() != 0 {
		t.Fatal("canceled pending work invoked the handler")
	}
	if err := <-trigger; err != nil {
		t.Fatal(err)
	}
	if err := binding.Close(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestManagedBindingCancellationReachesIndependentAdmittedContext(t *testing.T) {
	ctx := managedBindingPage(t)
	entered, completed := make(chan struct{}), make(chan BindingOutcome, 1)
	binding, err := Run(ctx, BindFunc("wait", func(ctx context.Context, _ int) (int, error) { close(entered); <-ctx.Done(); return 42, nil }, BindingOptions{
		Limits: BindingLimits{PrivateErrors: true},
		Admit: func(context.Context, int64) (context.Context, func(BindingOutcome), error) {
			return context.Background(), func(outcome BindingOutcome) { completed <- outcome }, nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := Run(ctx, Evaluate[string](`wait(1).catch(e=>e.message)`, EvalAwaitPromise))
		result <- err
	}()
	select {
	case <-entered:
	case <-bindingDeadline(t).Done():
		t.Fatal("handler did not start")
	}
	binding.Cancel()
	select {
	case outcome := <-completed:
		if !outcome.Returned || !errors.Is(outcome.Primary, context.Canceled) || !outcome.ReplyAcknowledged {
			t.Fatal("cancellation, return and reply facts conflated", outcome)
		}
	case <-bindingDeadline(t).Done():
		t.Fatal("independent admitted context did not receive cancellation")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := binding.Close(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestManagedBindingPrewireScriptRefusalIsNotUnknownCustody(t *testing.T) {
	ctx := managedBindingPage(t)
	rejected := errors.New("prewire-script-refusal")
	var queued atomic.Int64
	work := WithCallLimits(ctx, CallLimits{
		Before: func(method string) error {
			if method == page.AddScriptToEvaluateOnNewDocument.Method {
				return rejected
			}
			return nil
		},
	})
	work = withQueued(work, func(method string) {
		if method == page.AddScriptToEvaluateOnNewDocument.Method {
			queued.Add(1)
		}
	})
	binding, err := Run(work, BindFunc("prewire", func(context.Context, int) (int, error) { return 1, nil }, BindingOptions{}))
	if binding == nil || !errors.Is(err, rejected) || queued.Load() != 0 {
		t.Fatal("script was not refused before submission", err)
	}
	if err := binding.Close(bindingDeadline(t)); err != nil || !binding.Closed() {
		t.Fatal("prewire rejection became permanent unknown custody", err)
	}
	if value, err := Run(ctx, Evaluate[int](`Object.getOwnPropertyNames(window).filter(n=>n.startsWith("__chromedp_")).length`)); err != nil || value != 0 {
		t.Fatal("partial binding left a private function", value, err)
	}
}

func TestManagedBindingKeepsSetupErrorAfterEarlierHandlerFailure(t *testing.T) {
	ctx := managedBindingPage(t)
	handlerError := errors.New("handler-first")
	setupError := errors.New("setup-second")
	completed := make(chan struct{})
	if _, err := Run(ctx, Evaluate[Void](`window.watch=setInterval(()=>{if(typeof constructed==="function"){clearInterval(watch);constructed(1).catch(()=>{})}},0)`)); err != nil {
		t.Fatal(err)
	}
	work := WithCallLimits(ctx, CallLimits{Before: func(method string) error {
		if method == page.AddScriptToEvaluateOnNewDocument.Method {
			select {
			case <-completed:
			case <-bindingDeadline(t).Done():
				return errors.New("callback setup fixture did not run")
			}
			return setupError
		}
		return nil
	}})
	binding, err := Run(work, BindFunc("constructed", func(context.Context, int) (int, error) { return 0, handlerError }, BindingOptions{
		Admit: func(ctx context.Context, _ int64) (context.Context, func(BindingOutcome), error) {
			return ctx, func(BindingOutcome) { close(completed) }, nil
		},
	}))
	if binding == nil || !errors.Is(err, setupError) {
		t.Fatal("setup counterexample did not occur", err)
	}
	if !errors.Is(binding.Err(), handlerError) || !errors.Is(binding.Err(), setupError) {
		t.Fatal("earlier callback erased setup failure", binding.Err())
	}
	if err := binding.Close(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestManagedBindingCloseRemovesNativeFunctionFromLaterWorld(t *testing.T) {
	ctx := managedBindingPage(t)
	binding, err := Run(ctx, BindFunc("world", func(context.Context, int) (int, error) { return 1, nil }, BindingOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	target := FromContext(ctx).Target
	tree, err := cdp.Call(ctx, target, page.GetFrameTree, cdp.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	world, err := cdp.Call(ctx, target, page.CreateIsolatedWorld, page.CreateIsolatedWorldParams{FrameID: tree.FrameTree.Frame.ID, WorldName: "managed-test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.Close(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
	value, err := cdp.Call(ctx, target, runtime.Evaluate, runtime.EvaluateParams{
		ContextID:     world.ExecutionContextID,
		Expression:    `Object.getOwnPropertyNames(globalThis).filter(n=>n.startsWith("__chromedp_")).length`,
		ReturnByValue: ptr(true),
	})
	if err != nil || value.Result == nil || string(value.Result.Value) != "0" {
		t.Fatal("native binding remained in a later execution world", value.Result, err)
	}
}

func TestManagedBindingCleanupRetryPreservesReplacement(t *testing.T) {
	ctx := managedBindingPage(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(resume) }) })
	binding, err := Run(ctx, BindFunc("retry", func(context.Context, int) (int, error) {
		close(entered)
		<-resume
		return 1, nil
	}, BindingOptions{CleanupTimeout: 3 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, Evaluate[Void](`retry(1).catch(()=>{}); undefined`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-bindingDeadline(t).Done():
		t.Fatal("handler did not start")
	}
	short, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := binding.Close(short); !errors.Is(err, context.DeadlineExceeded) || binding.Closed() {
		t.Fatal("unfinished handler lost its owner", err)
	}
	if value, err := Run(ctx, Evaluate[int](`Object.getOwnPropertyNames(window).filter(n=>n.startsWith("__chromedp_")).length`)); err != nil || value != 0 {
		t.Fatal("fixture did not reach completed wrapper cleanup", value, err)
	}
	if _, err := Run(ctx, Evaluate[Void]("globalThis."+binding.nativeName+" = function () { return 73 }")); err != nil {
		t.Fatal(err)
	}
	release.Do(func() { close(resume) })
	if err := binding.Close(bindingDeadline(t)); !errors.Is(err, context.DeadlineExceeded) || !binding.Closed() {
		t.Fatal("retry did not retain cleanup history and finish", err)
	}
	if value, err := Run(ctx, Evaluate[int]("globalThis."+binding.nativeName+"()")); err != nil || value != 73 {
		t.Fatal("cleanup retry removed a replacement function", err)
	}
}

func TestManagedBindingUnknownCleanupReplyDoesNotRecaptureReplacement(t *testing.T) {
	ctx := managedBindingPage(t)
	binding, err := Run(ctx, BindFunc("uncertain", func(context.Context, int) (int, error) { return 1, nil }, BindingOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	name := binding.nativeName
	expression := `(function(){const key="` + name + `_revoke";const stop=globalThis[key];Object.defineProperty(globalThis,key,{configurable:true,value:function(){const result=stop();delete globalThis[key];const end=performance.now()+750;while(performance.now()<end){};return result}})})()`
	if _, err := Run(ctx, Evaluate[Void](expression)); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := binding.Close(short); !errors.Is(err, context.DeadlineExceeded) || binding.Closed() {
		t.Fatal("cleanup reply was not uncertain", err)
	}
	if value, err := Run(ctx, Evaluate[int](`Object.getOwnPropertyNames(window).filter(n=>n.startsWith("__chromedp_")).length`)); err != nil || value != 0 {
		t.Fatal("uncertain cleanup did not have its remote effect", value, err)
	}
	if _, err := Run(ctx, Evaluate[Void]("globalThis."+name+" = function () { return 91 }")); err != nil {
		t.Fatal(err)
	}
	if err := binding.Close(bindingDeadline(t)); err == nil || binding.Closed() {
		t.Fatal("unknown property ownership was declared released", err)
	}
	if value, err := Run(ctx, Evaluate[int]("globalThis."+name+"()")); err != nil || value != 91 {
		t.Fatal("uncertain cleanup recaptured a replacement function", err)
	}
}

func TestManagedBindingKeepsIsolatedWorldPublicNames(t *testing.T) {
	ctx := managedBindingPage(t)
	target := FromContext(ctx).Target
	tree, err := cdp.Call(ctx, target, page.GetFrameTree, cdp.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	world, err := cdp.Call(ctx, target, page.CreateIsolatedWorld, page.CreateIsolatedWorldParams{FrameID: tree.FrameTree.Frame.ID, WorldName: "existing-world"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cdp.Call(ctx, target, runtime.Evaluate, runtime.EvaluateParams{ContextID: world.ExecutionContextID, Expression: `globalThis.isolated="prior"`}); err != nil {
		t.Fatal(err)
	}
	binding, err := Run(ctx, BindFunc("isolated", func(context.Context, int) (int, error) { return 1, nil }, BindingOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	value, err := cdp.Call(ctx, target, runtime.Evaluate, runtime.EvaluateParams{ContextID: world.ExecutionContextID, Expression: `globalThis.isolated`, ReturnByValue: ptr(true)})
	if err != nil || value.Result == nil || string(value.Result.Value) != `"prior"` {
		t.Fatal("binding overwrote a name in an existing isolated world", value.Result, err)
	}
	if value, err := Run(ctx, Evaluate[int](`isolated(1)`, EvalAwaitPromise)); err != nil || value != 1 {
		t.Fatal("default world binding did not work", err)
	}
	if err := binding.Close(bindingDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestManagedBindingCleanupHistoryDoesNotGrowAcrossNewWorlds(t *testing.T) {
	ctx := managedBindingPage(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(resume) }) })
	binding, err := Run(ctx, BindFunc("worlds", func(context.Context, int) (int, error) {
		close(entered)
		<-resume
		return 1, nil
	}, BindingOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, Evaluate[Void](`worlds(1).catch(()=>{});undefined`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-bindingDeadline(t).Done():
		t.Fatal("handler did not start")
	}
	closeHeld := func() {
		t.Helper()
		short, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := binding.Close(short); !errors.Is(err, context.DeadlineExceeded) || binding.Closed() {
			t.Fatal("unfinished handler did not remain owned", err)
		}
	}
	closeHeld()
	if binding.nativeAttempted || binding.scriptAttempted {
		t.Fatal("fixture did not reach removed native registration")
	}
	retained := len(binding.captures)
	for range 4 {
		if err := Do(ctx, Navigate("data:text/html,<h1>new execution world</h1>")); err != nil {
			t.Fatal(err)
		}
		closeHeld()
		if len(binding.cleaned) != 0 || len(binding.captures) > retained {
			t.Fatal("cleanup retry retained unrelated execution worlds", len(binding.cleaned), len(binding.captures))
		}
	}
	release.Do(func() { close(resume) })
	if err := binding.Close(bindingDeadline(t)); !errors.Is(err, context.DeadlineExceeded) || !binding.Closed() {
		t.Fatal("retired execution worlds prevented eventual cleanup", err)
	}
}

func TestManagedBindingRejectsIncompleteExecutionOrigins(t *testing.T) {
	ctx, cancel := NewContext(allocCtx, WithBrowserOption(WithEventBuffer(EventBuffer{MaxEvents: 16, MaxBytes: 1})))
	defer cancel()
	defer Cancel(ctx)
	if err := Do(ctx); err != nil {
		t.Fatal(err)
	}
	binding, err := Run(ctx, BindFunc("originBound", func(context.Context, int) (int, error) { return 1, nil }, BindingOptions{}))
	if err == nil {
		cleanup := binding.Close(bindingDeadline(t))
		remaining, _ := Run(ctx, Evaluate[string](`typeof globalThis.originBound`))
		t.Fatalf("incomplete origin tracking installed a binding: closed=%t, cleanup_error=%t, wrapper_remains=%t", binding.Closed(), cleanup != nil, remaining == "function")
	}
	if !errors.Is(err, ErrEventOverflow) || binding != nil {
		t.Fatal("origin refusal acquired partial binding ownership", err)
	}
	if remaining, err := Run(ctx, Evaluate[string](`typeof globalThis.originBound`)); err != nil || remaining != "undefined" {
		t.Fatal("refused binding installed page state", err)
	}
}

func TestManagedBindingUnknownOriginsPreventFalseCleanup(t *testing.T) {
	ctx := managedBindingPage(t, WithEventBuffer(EventBuffer{MaxEvents: 64, MaxBytes: 4096}))
	binding, err := Run(ctx, BindFunc("unknownOrigins", func(context.Context, int) (int, error) { return 3, nil }, BindingOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	target := FromContext(ctx).Target
	params := `{"context":{"id":79,"uniqueId":"` + strings.Repeat("x", 8192) + `"}}`
	if err := target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(runtime.ExecutionContextCreated.Method), Params: jsonv2.Value(params)}); !errors.Is(err, ErrEventOverflow) {
		t.Fatal("fixture did not lose an execution origin", err)
	}
	if err := binding.Close(bindingDeadline(t)); !errors.Is(err, ErrEventOverflow) || binding.Closed() {
		t.Fatal("unknown world cleanup was declared complete", err)
	}
	if err := Do(ctx, Navigate("data:text/html,<h1>new document</h1>")); err != nil {
		t.Fatal(err)
	}
	if err := binding.Close(bindingDeadline(t)); !errors.Is(err, ErrEventOverflow) || !binding.Closed() {
		t.Fatal("confirmed world reset did not complete cleanup with its history", err)
	}
}

func TestManagedBindingChecksOriginsAfterActualHandlerJoin(t *testing.T) {
	ctx := managedBindingPage(t, WithEventBuffer(EventBuffer{MaxEvents: 64, MaxBytes: 4096}))
	entered, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(resume) }) })
	binding, err := Run(ctx, BindFunc("joiningOrigins", func(context.Context, int) (int, error) {
		close(entered)
		<-resume
		return 1, nil
	}, BindingOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, Evaluate[Void](`joiningOrigins(1).catch(()=>{});undefined`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-bindingDeadline(t).Done():
		t.Fatal("handler did not start")
	}
	done := make(chan error, 1)
	cleanup := bindingDeadline(t)
	go func() { done <- binding.Close(cleanup) }()
	joined := false
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		stack := make([]byte, 1<<20)
		count := goruntime.Stack(stack, true)
		if strings.Contains(string(stack[:count]), "(*Binding).WaitStopped") {
			joined = true
			break
		}
		goruntime.Gosched()
	}
	if !joined {
		t.Fatal("cleanup did not reach the actual handler join")
	}
	params := `{"context":{"id":79,"uniqueId":"` + strings.Repeat("x", 8192) + `"}}`
	if err := FromContext(ctx).Target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(runtime.ExecutionContextCreated.Method), Params: jsonv2.Value(params)}); !errors.Is(err, ErrEventOverflow) {
		t.Fatal("fixture did not invalidate origin completeness", err)
	}
	release.Do(func() { close(resume) })
	if err := <-done; !errors.Is(err, ErrEventOverflow) || binding.Closed() {
		t.Fatal("cleanup used an origin snapshot from before the handler join", err)
	}
}
